# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import json
import logging
import os

from anyio import Path as AnyioPath

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
    def __init__(self, client: APIClient, root_dir: AnyioPath) -> None:
        self.client = client
        self.root_dir = root_dir

    async def __get_translations(self, file_id: int) -> list[dict] | None:
        return await self.client.request("GET", f"/files/{file_id}/translation")

    async def __fix_file_shift(self, file_id: int, old_translation: list[dict]) -> None:
        if not old_translation:
            return

        # Build mapping from original text to translation based on old data.
        original_to_translation: dict[str, str] = {}
        for item in old_translation:
            key = item.get("key", "")
            if key.endswith(("->id", "->model")):
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

        if not original_to_translation:
            return

        # Get latest translations for the file.
        if not (new_translations := await self.__get_translations(file_id)):
            return

        updates: list[dict] = []
        for item in new_translations:
            key = item.get("key", "")
            if key.endswith(("->id", "->model")):
                continue

            # Only fix entries that are currently untranslated (stage == 0).
            if item.get("stage") != 0:
                continue

            original = str(item.get("original", ""))
            if not original or is_placeholder_only(original):
                continue

            if (restored := original_to_translation.get(original)) is None:
                continue

            new_item = {
                **item,
                "translation": restored,
                # Mark as translated so it is picked up by merger logic.
                "stage": 1,
            }
            updates.append(new_item)

        if not updates:
            return

        logger.info(f"Fix file shift file_id={file_id} update count={len(updates)}")

        data = json.dumps(updates, ensure_ascii=False).encode("utf-8")
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
                and keys[-1] in ("id", "model")
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
                        value_str = str(value).replace("\n", "\\n")
                        if item["original"] in value_str:
                            break
                        context_parts.append(f"{lang}:\n{value_str}")

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
    of being auto-fixed. Dry-run by default (`apply=False`): nothing is written,
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

        clears: list[dict] = []
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
                        update = {**item, "context": ""}
                        if not self.context_only:
                            update["translation"] = ""
                        clears.append(update)
                        report["cleared_blank"] += 1
                elif is_placeholder_only(original):
                    if item.get("context"):
                        clears.append({**item, "context": ""})
                        report["cleared_context"] += 1
                else:
                    report["flagged_maybe_wrongly_hidden"].append(
                        {"key": item.get("key"), "original": original},
                    )
            elif is_placeholder_only(original) and has_leftover:
                report["flagged_visible_placeholder"].append(
                    {"key": item.get("key"), "original": original},
                )

        if clears and self.apply:
            data = json.dumps(clears, ensure_ascii=False).encode("utf-8")
            await self.client.request(
                "POST",
                f"/files/{file.id}/translation",
                files={"file": (f"{file.id}.json", data)},
                data={"force": "true"},
            )
            logger.info(f"Cleaned {file.name} (ID: {file.id}): {len(clears)} entries")
        elif clears:
            logger.info(f"[dry-run] Would clean {file.name} (ID: {file.id}): {len(clears)} entries")

        return report
