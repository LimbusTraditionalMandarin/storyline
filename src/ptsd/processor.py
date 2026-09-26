# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import html
import json
import logging
import os

from anyio import Path as AnyioPath, create_task_group

from .core import FileOperation, OperationType, ProjectFile
from .core.paratranz import APIClient
from .core.utils import (
    get_value_by_keys,
    is_blank,
    is_placeholder_only,
    load_json_file,
    match_project_file,
    save_json_file,
)

logger = logging.getLogger(__name__)


class ContextHandler:
    # Field names this project's schemas use, by convention, for a row's own
    # stable internal identifier (as opposed to player-facing text) - e.g.
    # `{"key": "AskToMoveFloor", "text": "..."}` or `{"id": "<code>", "name":
    # "...", "nickName": "..."}`. Its value never changes between old/new
    # source versions even when the row's array position does, which makes
    # it a far more reliable join key across a shift than array position or
    # exact-text matching alone. See `__fix_file_shift`.
    __IDENTIFIER_FIELDS = ("id", "key")

    def __init__(self, client: APIClient, root_dir: AnyioPath) -> None:
        self.client = client
        self.root_dir = root_dir

    async def __get_translations(self, file_id: int) -> list[dict] | None:
        return await self.client.request("GET", f"/files/{file_id}/translation")

    def __row_prefix(self, key: str) -> str:
        """Everything in a ParaTranz key path except the last segment, e.g.
        `dataList->6->text` -> `dataList->6`. Entries sharing a row prefix
        are sibling fields of the same source-JSON array element."""
        idx = key.rfind("->")
        return key[:idx] if idx != -1 else ""

    def __group_rows(self, translations: list[dict]) -> dict[str, dict[str, dict]]:
        """Group flat ParaTranz translation entries by row prefix.

        Returns {row_prefix: {field_name: entry}}.
        """
        rows: dict[str, dict[str, dict]] = {}
        for item in translations:
            key = item.get("key", "")
            if "->" not in key:
                continue
            rows.setdefault(self.__row_prefix(key), {})[key.rsplit("->", 1)[-1]] = item
        return rows

    async def __fix_file_shift(self, file_id: int, old_translation: list[dict]) -> None:
        if not old_translation:
            return

        # --- Tier 1: stable per-row identifier match ------------------------
        # For rows that carry their own identifier field (see
        # `__IDENTIFIER_FIELDS`), build identifier value -> {field: translation}
        # from the OLD data. This survives insertions/deletions/reordering
        # between versions, unlike array position, because the identifier's
        # value does not change even when the row moves.
        old_rows = self.__group_rows(old_translation)
        id_to_fields: dict[str, dict[str, str]] = {}
        seen_ids: set[str] = set()
        ambiguous_ids: set[str] = set()
        for fields in old_rows.values():
            id_field = next((f for f in self.__IDENTIFIER_FIELDS if f in fields), None)
            if id_field is None:
                continue
            id_value = str(fields[id_field].get("original", ""))
            if not id_value or is_placeholder_only(id_value):
                continue
            if id_value in seen_ids:
                # Same identifier value used by more than one row in the OLD
                # data - ambiguous, refuse to guess which one a NEW row with
                # this identifier should inherit from.
                ambiguous_ids.add(id_value)
                continue
            seen_ids.add(id_value)
            translated_fields = {
                name: str(f["translation"])
                for name, f in fields.items()
                if f.get("stage") not in (0, -1) and f.get("translation")
            }
            if translated_fields:
                id_to_fields[id_value] = translated_fields
        for id_value in ambiguous_ids:
            id_to_fields.pop(id_value, None)

        # --- Tier 2 (fallback): exact original-text match --------------------
        # Existing behaviour, unchanged: build original_text -> translation
        # from old data, for rows with no stable identifier field.
        original_to_translation: dict[str, str] = {}
        for item in old_translation:
            key = item.get("key", "")
            if key.endswith(("->id", "->model", "->key")):
                continue

            # Skip untranslated (0) AND hidden (-1) entries. `-1` entries are
            # ParaTranz's own auto-hide for blank/placeholder-only original text;
            # their leftover translation/context is frequently stale residue from
            # before a shift and must not be reused as translation memory.
            if item.get("stage") in (0, -1):
                continue

            translation = item.get("translation") or ""
            if not translation:
                continue

            original = str(item.get("original", ""))
            # Placeholder-only original text (e.g. a bare `{0}` or `<color>`) recurs
            # verbatim across many unrelated entries, so it is a high collision-risk
            # lookup key - never let it seed the translation-memory map.
            if original and not is_placeholder_only(original):
                original_to_translation[original] = str(translation)

        if not original_to_translation and not id_to_fields:
            return

        # Get latest translations for the file.
        if not (new_translations := await self.__get_translations(file_id)):
            return

        new_rows = self.__group_rows(new_translations)

        updates: list[dict] = []
        scrubs: list[dict] = []
        for item in new_translations:
            key = item.get("key", "")
            if key.endswith(("->id", "->model", "->key")):
                continue

            # Only fix entries that are currently untranslated (stage == 0).
            if item.get("stage") != 0:
                continue

            original = str(item.get("original", ""))
            if not original or is_placeholder_only(original):
                continue

            restored = None
            row = new_rows.get(self.__row_prefix(key), {})
            id_field = next((f for f in self.__IDENTIFIER_FIELDS if f in row), None)
            if id_field is not None:
                id_value = str(row[id_field].get("original", ""))
                if id_value and not is_placeholder_only(id_value):
                    field_name = key.rsplit("->", 1)[-1]
                    restored = id_to_fields.get(id_value, {}).get(field_name)

            if restored is None:
                restored = original_to_translation.get(original)

            if restored is not None:
                updates.append({
                    **item,
                    "translation": restored,
                    # Mark as translated so it is picked up by merger logic.
                    "stage": 1,
                })
            elif item.get("translation"):
                # --- Tier 3: scrub unrecoverable residue -------------------
                # Neither match tier above could verify this stage == 0
                # entry's current (non-empty) translation. ParaTranz carries
                # old translation content over to the new array position on
                # file replace regardless of whether it actually belongs
                # there; if nothing above confirms it does, it is most
                # likely stale leftover from a different, unrelated row that
                # used to sit at this array slot. Clear it instead of
                # leaving it looking like a real (if unreviewed) draft
                # translation. This never touches stage >= 1 (already
                # reviewed) entries.
                scrubs.append({**item, "translation": ""})

        all_updates = updates + scrubs
        if not all_updates:
            return

        logger.info(
            f"Fix file shift file_id={file_id} "
            f"restored={len(updates)} scrubbed={len(scrubs)}",
        )

        data = json.dumps(all_updates, ensure_ascii=False).encode("utf-8")
        await self.client.request(
            "POST",
            f"/files/{file_id}/translation",
            files={"file": (f"{file_id}.json", data)},
            data={"force": "true"},
        )

    async def __update_fixed_translation(self, file_id: int, filename: str) -> None:
        if not (translations := await self.client.request("GET", f"/files/{file_id}/translation")):
            return
        updates = []
        for item in translations:
            keys = item["key"].split("->")
            if (
                item["original"] != ""
                and item["translation"] != item["original"]
                # `id`/`model` are this project's convention for a row's own
                # untranslatable identifier field. `key` is the same
                # convention under a different name, used by several files
                # (e.g. RPGSystem UI strings shaped `{"key": "...", "text":
                # "..."}`, where "key" is an internal string constant, never
                # player-facing text). Treating it the same way both keeps it
                # out of the translation queue and - via `stage: 1` below -
                # makes it immune to the array-position-shift residue problem
                # `__fix_file_shift` deals with, same as `id`/`model` fields.
                and keys[-1] in ("id", "model", "key")
                and item["stage"] in (0, -1)
            ):
                new_item = {
                    **item,
                    "translation": item["original"],
                    "stage": 1,
                }
                updates.append(new_item)
        if updates:
            data = json.dumps(updates, ensure_ascii=False).encode("utf-8")
            await self.client.request(
                "POST",
                f"/files/{file_id}/translation",
                files={"file": (f"{filename}.json", data)},
                data={"force": "true"},
            )
            logger.info(
                f"Fixed translation for {filename} (ID: {file_id} Count: {len(updates)})",
            )

    async def __update_contexts(
        self,
        file_id: int,
        filename: str,
        langs: dict,
    ) -> None:
        """Update translations with context from other languages."""
        if not (translations := await self.client.request("GET", f"/files/{file_id}/translation")):
            return
        updates = []
        for item in translations:
            original = str(item.get("original", ""))
            # Blank/placeholder-only original text (e.g. `{0}`, `<color>`, or the
            # empty string) has nothing meaningful to give EN/JP context for, and is
            # exactly what ParaTranz's auto-hide (`stage == -1`) keys on. Skip
            # building context for it so stale EN/JP text left over from a previous
            # shift never gets re-attached here.
            placeholder_only = is_placeholder_only(original)

            context_parts = []
            if not placeholder_only:
                for lang in ("EN", "JP"):
                    lang_data = langs.get(lang)
                    if not lang_data:
                        continue

                    keys = item["key"].split("->")
                    value = get_value_by_keys(lang_data, keys)
                    if value is not None:
                        # Keep real line breaks as real line breaks. This used to
                        # `.replace("\n", "\\n")`, which wrote the literal two
                        # characters `\n` into the context field instead of an
                        # actual newline - on ParaTranz's UI that renders as a wall
                        # of text with visible backslash-n's in it instead of
                        # multiple lines, which is what prompted removing it. Every
                        # existing context entry written before this fix still has
                        # the literal `\n` baked in; that's a separate one-off
                        # cleanup (see `ContextNewlineFixer` / `ptsd fix-newlines`).
                        value_str = str(value)
                        if item["original"] in value_str:
                            break
                        # ParaTranz's context viewer renders this field as raw HTML, so a
                        # bare `<`/`>` in EN/JP text gets parsed as markup instead of shown
                        # literally - e.g. `<I know, but...>` collapses to `<i>` because the
                        # browser treats it as a malformed `<i ...>` tag. Escaping to HTML
                        # entities here keeps it displaying as literal text regardless of
                        # what it happens to look like. `quote=False` because quote marks
                        # don't trigger tag parsing and read better unescaped.
                        escaped_value = html.escape(value_str, quote=False)
                        context_parts.append(f"{lang}:\n{escaped_value}")

            new_item = {
                **item,
                # ParaTranz's `text.context` column is NOT NULL. Sending null makes the
                # whole batch upsert fail with ER_BAD_NULL_ERROR (HTTP 500), which
                # silently wiped out context for every entry in the file.
                "context": "\n\n".join(context_parts) if context_parts else "",
            }
            if is_blank(original):
                # Truly blank original: nothing to translate, so also scrub any
                # stale leftover translation rather than just the context.
                new_item["translation"] = ""
            updates.append(new_item)

        if updates:
            data = json.dumps(updates, ensure_ascii=False).encode("utf-8")
            await self.client.request(
                "POST",
                f"/files/{file_id}",
                files={"file": (f"{filename}.json", data)},
            )

    async def handle_upload(
        self,
        operation: FileOperation,
        project_files: list[ProjectFile],
    ) -> None:
        kr_file = self.root_dir / "kr" / operation.full_path
        filename = kr_file.name.replace("KR_", "")

        if not await kr_file.exists() and (operation.op_type != OperationType.DELETE):
            return

        en_file = self.root_dir / "en" / operation.full_path.replace("KR_", "EN_")
        jp_file = self.root_dir / "jp" / operation.full_path.replace("KR_", "JP_")

        async def load_lang_data(path: AnyioPath) -> dict | None:
            try:
                return await load_json_file(path) if await path.exists() else None
            except Exception as e:
                logger.error(f"Failed to load {path}: {e}")
                return None

        langs = {
            "EN": await load_lang_data(en_file),
            "JP": await load_lang_data(jp_file),
        }

        match operation.op_type:
            case OperationType.ADD:
                form = {
                    "path": (None, operation.folder),
                    "file": (filename, await kr_file.read_bytes(), "application/json"),
                }
                if res := await self.client.request("POST", "/files", files=form):
                    # Because Project Moon is st**d and puts empty data in the file.
                    if "status" in res:
                        logger.warning(f"Faield to add {operation.full_path}: {res}")
                        return
                    await self.__update_contexts(res["file"]["id"], filename, langs)
                    await self.__update_fixed_translation(res["file"]["id"], filename)
                    logger.info(f"Added {operation.full_path} (ID: {res['file']['id']})")

            case OperationType.MODIFY:
                if pf := match_project_file(project_files, operation.full_path):
                    old_translations = await self.__get_translations(pf.id)
                    form = {
                        "file": (filename, await kr_file.read_bytes(), "application/json"),
                    }
                    if res := await self.client.request("POST", f"/files/{pf.id}", files=form):
                        # Because Project Moon is st**d and puts empty data in the file.
                        if "status" in res:
                            logger.warning(f"Faield to add {operation.full_path}: {res}")
                            return
                        await self.__update_contexts(pf.id, filename, langs)
                        await self.__update_fixed_translation(pf.id, filename)
                        await self.__fix_file_shift(pf.id, old_translations)
                        logger.info(f"Updated {operation.full_path} (ID: {pf.id})")

            case OperationType.DELETE:
                if pf := match_project_file(project_files, operation.full_path):
                    await self.client.request("DELETE", f"/files/{pf.id}")
                    logger.info(f"Deleted {operation.full_path} (ID: {pf.id})")


class TranslationMerger:
    def __init__(self, client: APIClient, root_dir: AnyioPath, target_dir: str) -> None:
        self.client = client
        self.root_dir = root_dir
        self.target_dir = self.root_dir / target_dir

    def __apply_translations(self, data: dict, translations: list[dict]) -> None:
        for item in translations:
            # ParaTranz forces `stage == -1` (hidden) entries to export as the
            # original text on its own end; mirror that here so a stale/mismatched
            # leftover translation on a hidden entry never leaks into our output,
            # regardless of how or why it ended up hidden.
            if item["stage"] in (0, -1) or not item["translation"]:
                continue
            keys, target = item["key"].split("->"), data
            try:
                for key in keys[:-1]:
                    target = target[int(key) if key.isdigit() else key]

                final_key = keys[-1]
                if isinstance(target, list):
                    target[int(final_key)] = item["translation"].replace("\\n", "\n")
                else:
                    target[final_key] = item["translation"].replace("\\n", "\n")
            except (KeyError, IndexError, ValueError) as e:
                logger.error(f"Translation error at {item['key']}: {e!r}")

    def __update_from_en(self, en_data, raw_data):
        if isinstance(en_data, dict) and isinstance(raw_data, dict):
            for key in raw_data:
                if key in {"id", "model"}:
                    continue
                if key in en_data:
                    raw_data[key] = self.__update_from_en(en_data[key], raw_data[key])
            return raw_data

        elif isinstance(en_data, list) and isinstance(raw_data, list):
            updated_list = []
            for i, kr_item in enumerate(raw_data):
                if i < len(en_data):
                    en_item = en_data[i]
                    updated_item = self.__update_from_en(en_item, kr_item)
                    updated_list.append(updated_item)
                else:
                    updated_list.append(kr_item)
            return updated_list

        else:
            return en_data

    async def merge_translation(
        self,
        file: ProjectFile,
    ) -> None:
        para_path = AnyioPath(file.name)
        en_path = self.root_dir / "en" / para_path.parent / f"EN_{para_path.name}"
        raw_path = self.root_dir / "kr" / para_path.parent / f"KR_{para_path.name}"
        output_path = self.target_dir / file.name

        if not (translations := await self.client.request("GET", f"/files/{file.id}/translation")):
            return

        try:
            data = await load_json_file(raw_path)
            if os.path.exists(en_path):
                en_data = await load_json_file(en_path)
                data = self.__update_from_en(en_data, data)
            self.__apply_translations(data, translations)
            await save_json_file(data, output_path)
            logger.info(f"Merged translations for {file.name}")

        except Exception as e:
            logger.error(f"Failed to merge {file.name}: {e!r}")


class Replacer:
    def __init__(self, client: APIClient, reference_file: AnyioPath):
        self.client = client
        self.ref_dict = self.__process_dict(reference_file)

    def __process_dict(self, reference_file: AnyioPath):
        ref_dict = {}
        with open(reference_file, encoding="utf-8") as f:
            while l := f.readline().split():
                ref_dict[l[0]] = l[1]
        return str.maketrans(ref_dict)

    async def handle_replace(self, file: ProjectFile):
        if not (translations := await self.client.request("GET", f"/files/{file.id}/translation")):
            return

        updates = [
            {**item, "translation": translated, "stage": 1}
            for item in translations
            if item["stage"] != 5
            and (translated := (original := str(item["translation"])).translate(self.ref_dict))
            != original
        ]

        if updates:
            data = json.dumps(updates, ensure_ascii=False).encode("utf-8")
            await self.client.request(
                "POST",
                f"/files/{file.id}/translation",
                files={"file": (file.name, data)},
                data={"force": "true"},
            )

        logger.info(f"Replaced {file.name} (ID: {file.id})")


class ResidueCleaner:
    """One-off maintenance pass over the *existing* ParaTranz project.

    Scrubs stale context/translation left on entries that are hidden
    (`stage == -1`) because their original text is blank or placeholder-only,
    and flags anything that looks like it needs a human to look at it instead
    of being auto-fixed. Also covers hidden entries whose original is neither
    blank nor placeholder-only but that have no translation at all: with no
    translation, the entry isn't actually in use, so leftover context is
    residue regardless of why it got hidden (auto-hidden or hidden by hand) -
    entries that still have a translation are left alone and flagged instead,
    since clearing context out from under a real translation could be
    destructive. Dry-run by default (`apply=False`): nothing is written,
    only counted and reported.

    `context_only=True` restricts every write to the `context` field only - the
    `translation` field is never included in the pushed payload, even for blank
    originals. Use this when translation content has already been reconciled by
    hand and only the leftover context still needs clearing, so this pass cannot
    step on that work.
    """

    def __init__(self, client: APIClient, apply: bool, context_only: bool = False) -> None:
        self.client = client
        self.apply = apply
        self.context_only = context_only

    async def clean_file(self, file: ProjectFile) -> dict:
        report: dict = {
            "file": file.name,
            "id": file.id,
            "cleared_blank": 0,
            "cleared_context": 0,
            # Every entry actually (or, in dry-run, would-be) cleared, with its
            # ParaTranz string id so a specific entry can be pulled up directly
            # at paratranz.cn/projects/<project>/strings?id=<id> for review.
            "cleared_entries": [],
            # stage == -1 but the original doesn't look blank/placeholder-only -
            # possible false-positive auto-hide, i.e. a real line stuck invisible.
            "flagged_maybe_wrongly_hidden": [],
            # Not hidden, but the original IS blank/placeholder-only and still
            # carries context/translation - not touched automatically since it's
            # a normal, currently-visible entry a translator may be using.
            "flagged_visible_placeholder": [],
        }

        if not (translations := await self.client.request("GET", f"/files/{file.id}/translation")):
            return report

        # NOTE ON WRITE PATH: this used to batch every clear into one
        # `POST /files/{id}/translation` call. That endpoint was tested against
        # production (2026-09-14) and confirmed unsuitable for context-only
        # residue clears on blank-original entries: without `force=true` it
        # silently no-ops (`hashMatched`) since original/translation are
        # unchanged; *with* `force=true` a single-item batch still no-ops
        # (`unchanged`), and a multi-item batch is instead accepted as a
        # ParaTranz *import* (`type=import`) - which recomputes `stage` from
        # content (flipping manually-hidden -1 entries to 0) while silently
        # dropping the `context` field entirely, i.e. the one thing this pass
        # is supposed to write never actually lands. `PUT /strings/{id}` (one
        # entry at a time) was confirmed to apply a context-only write with no
        # effect on `stage`, so that's what's used below instead.
        to_clear: list[tuple[dict, str]] = []
        for item in translations:
            original = str(item.get("original", ""))
            stage = item.get("stage")
            has_leftover = bool(item.get("context")) or bool(item.get("translation"))

            if stage == -1:
                if is_blank(original):
                    # In context_only mode, translation is never part of the write,
                    # so only a leftover context is worth pushing an update for.
                    needs_clear = item.get("context") if self.context_only else has_leftover
                    if needs_clear:
                        to_clear.append((item, "blank"))
                elif is_placeholder_only(original):
                    if item.get("context"):
                        to_clear.append((item, "context"))
                elif item.get("context") and not item.get("translation"):
                    # Original isn't blank/placeholder-only, but this entry
                    # has no translation at all - whatever hid it (auto or by
                    # hand), an unused entry with leftover context is still
                    # just residue. Only context is cleared; translation is
                    # already empty so there's nothing there to protect.
                    to_clear.append((item, "context"))
                else:
                    report["flagged_maybe_wrongly_hidden"].append(
                        {"id": item.get("id"), "key": item.get("key"), "original": original},
                    )
            elif is_placeholder_only(original) and has_leftover:
                report["flagged_visible_placeholder"].append(
                    {"id": item.get("id"), "key": item.get("key"), "original": original},
                )

        async def _clear_one(item: dict, category: str) -> None:
            entry_id = item.get("id")
            body: dict = {"context": ""}
            if category == "blank" and not self.context_only:
                body["translation"] = ""

            if self.apply:
                if (await self.client.request("PUT", f"/strings/{entry_id}", json=body)) is None:
                    logger.warning(
                        f"Failed to clear entry id={entry_id} key={item.get('key')!r} "
                        f"in {file.name} (ID: {file.id})",
                    )
                    return

            # No `await` between here and the end of this coroutine, so these
            # mutations of the shared `report` dict can't interleave with any
            # other concurrently-running `_clear_one` task.
            if category == "blank":
                report["cleared_blank"] += 1
            else:
                report["cleared_context"] += 1
            report["cleared_entries"].append(
                {"id": entry_id, "key": item.get("key"), "category": category},
            )

        # Per-entry PUTs, run concurrently (still bounded by the shared
        # APIClient semaphore / max-concurrency, and by ParaTranz's own
        # ~120 requests/minute soft limit - the client backs off on 429s but
        # does not yet pace requests to stay under that budget proactively).
        async with create_task_group() as tg:
            for item, category in to_clear:
                tg.start_soon(_clear_one, item, category)

        if total := len(report["cleared_entries"]):
            verb = "Cleaned" if self.apply else "[dry-run] Would clean"
            logger.info(f"{verb} {file.name} (ID: {file.id}): {total} entries")

        return report


class ContextNewlineFixer:
    """One-off maintenance pass over the *existing* ParaTranz project.

    Historically `ContextHandler.__update_contexts` escaped real newlines in the
    EN/JP context text into the literal two characters `\\n` before writing to
    ParaTranz, so every context entry written before that was fixed still has
    `\\n` baked in as visible text instead of an actual line break - on
    ParaTranz's UI this renders as one dense wall of text instead of multiple
    lines. This pass finds every existing `context` value that still contains a
    literal `\\n` and replaces it with a real line break.

    Only ever touches the `context` field - `translation` is never read or
    written by this pass. Dry-run by default (`apply=False`): nothing is
    written, only counted and logged.
    """

    def __init__(self, client: APIClient, apply: bool) -> None:
        self.client = client
        self.apply = apply

    async def fix_file(self, file: ProjectFile) -> dict:
        report: dict = {"file": file.name, "id": file.id, "fixed": 0, "fixed_entries": []}

        if not (translations := await self.client.request("GET", f"/files/{file.id}/translation")):
            return report

        # See the write-path note in ResidueCleaner.clean_file: the batch
        # `POST /files/{id}/translation` endpoint was confirmed (2026-09-14)
        # to drop context-only changes and, once >=1 items are force-imported,
        # to recompute `stage` from content instead - which here would risk
        # resetting already-translated/proofread entries, not just hidden
        # ones. `PUT /strings/{id}` per entry is the confirmed-safe path.
        fixes: list[dict] = [
            item for item in translations if "\\n" in (item.get("context") or "")
        ]

        async def _fix_one(item: dict) -> None:
            entry_id = item.get("id")
            new_context = (item.get("context") or "").replace("\\n", "\n")

            if self.apply:
                if (
                    await self.client.request(
                        "PUT", f"/strings/{entry_id}", json={"context": new_context},
                    )
                ) is None:
                    logger.warning(
                        f"Failed to fix entry id={entry_id} key={item.get('key')!r} "
                        f"in {file.name} (ID: {file.id})",
                    )
                    return

            # No `await` between here and the end of this coroutine, so these
            # mutations of the shared `report` dict can't interleave with any
            # other concurrently-running `_fix_one` task.
            report["fixed"] += 1
            report["fixed_entries"].append({"id": entry_id, "key": item.get("key")})

        # Per-entry PUTs, run concurrently (still bounded by the shared
        # APIClient semaphore / max-concurrency, and by ParaTranz's own
        # ~120 requests/minute soft limit - the client backs off on 429s but
        # does not yet pace requests to stay under that budget proactively).
        async with create_task_group() as tg:
            for item in fixes:
                tg.start_soon(_fix_one, item)

        if total := report["fixed"]:
            verb = "Fixed" if self.apply else "[dry-run] Would fix"
            logger.info(f"{verb} {file.name} (ID: {file.id}): {total} entries")

        return report
