# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import json
import logging

from anyio import Path as AnyioPath
from opencc import OpenCC

from .core import FileOperation, OperationType, ProjectFile
from .core.paratranz import APIClient
from .core.utils import load_json_file, match_project_file, save_json_file

logger = logging.getLogger(__name__)


class TranslationHandler:
    def __init__(self, client: APIClient) -> None:
        self.client = client
        self.converter = OpenCC("./config/custom_s2tw.json")

    async def __update_translations(self, file_id: int, src_file: AnyioPath) -> None:
        if not (translations := await self.client.request("GET", f"/files/{file_id}/translation")):
            return
        updates = [
            {**item, "translation": self.converter.convert(item["original"]), "stage": 1}
            for item in translations
            if (item["stage"] == 0) or (item["stage"] == -1)
        ]

        if updates:
            data = json.dumps(updates, ensure_ascii=False).encode("utf-8")
            await self.client.request(
                "POST",
                f"/files/{file_id}/translation",
                files={"file": (src_file.name, data)},
            )

    async def handle_upload(
        self,
        operation: FileOperation,
        project_files: list[ProjectFile],
        hans_dir: AnyioPath,
    ) -> None:
        src_file = hans_dir / operation.full_path
        if not await src_file.exists() and (operation.op_type != OperationType.DELETE):
            return

        match operation.op_type:
            case OperationType.ADD:
                form = {
                    "path": (None, operation.folder),
                    "file": (src_file.name, await src_file.read_bytes(), "application/json"),
                }
                if res := await self.client.request("POST", "/files", files=form):
                    await self.__update_translations(id := res["file"]["id"], src_file)
                    logger.info(f"Added {operation.full_path} (ID: {id})")

            case OperationType.MODIFY:
                if pf := match_project_file(project_files, operation.full_path):
                    form = {
                        "file": (src_file.name, await src_file.read_bytes(), "application/json"),
                    }
                    if await self.client.request("POST", f"/files/{pf.id}", files=form):
                        await self.__update_translations(pf.id, src_file)
                        logger.info(f"Updated {operation.full_path} (ID: {pf.id})")

            case OperationType.DELETE:
                if pf := match_project_file(project_files, operation.full_path):
                    await self.client.request("DELETE", f"/files/{pf.id}")
                    logger.info(f"Deleted {operation.full_path} (ID: {pf.id})")


class TranslationMerger:
    def __init__(self, client: APIClient) -> None:
        self.client = client

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

    async def merge_translation(
        self,
        file: ProjectFile,
        hans_dir: AnyioPath,
        hant_dir: AnyioPath,
    ) -> None:
        raw_path = hans_dir / file.name
        output_path = hant_dir / file.name

        if not (translations := await self.client.request("GET", f"/files/{file.id}/translation")):
            return

        try:
            data = await load_json_file(raw_path)
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
