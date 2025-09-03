# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import json
import logging
import os

from anyio import Path as AnyioPath

from .core import FileOperation, OperationType, ProjectFile
from .core.paratranz import APIClient
from .core.utils import get_value_by_keys, load_json_file, match_project_file, save_json_file

logger = logging.getLogger(__name__)


class ContextHandler:
    def __init__(self, client: APIClient, root_dir: AnyioPath) -> None:
        self.client = client
        self.root_dir = root_dir

    async def __update_fixed_translation(self, file_id: int, filename: str) -> None:
        if not (translations := await self.client.request("GET", f"/files/{file_id}/translation")):
            return
        updates = []
        for item in translations:
            keys = item["key"].split("->")
            if (
                item["original"] != ""
                and item["translation"] == ""
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
            context_parts = []
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
                "context": "\n\n".join(context_parts) if context_parts else None,
            }
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
            if item["stage"] == 0 or not item["translation"]:
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
