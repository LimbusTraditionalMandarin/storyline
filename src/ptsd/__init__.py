# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import argparse
import logging
from os import environ

from anyio import Path as AnyioPath
from anyio import create_task_group, run

from .core import ProjectFile
from .core.paratranz import APIClient
from .core.utils import parse_diff
from .translator import Replacer, TranslationHandler, TranslationMerger

PARATRANZ_PROJECT_ID: int = 13808


async def main_entry(
    mode: str,
    storyline_folder: str,
    max_concurrency: int,
    reference_file: str | None,
) -> None:
    """Corutine entry function for ParaTranz Sync Tool."""
    # Tokens by environment variable
    tokens = environ["PARATRANZ_TOKENS"].split(",")

    # Folder
    root = AnyioPath(storyline_folder)
    hans_dir = root / "Hans"

    # ParaTranz API Client
    client = APIClient(PARATRANZ_PROJECT_ID, tokens, max_concurrency)
    project_files = [
        ProjectFile(f["id"], f["name"]) for f in (await client.get_project_files()) or []
    ]

    if mode == "upload":
        diff_path = root / "file-diff.txt"
        handler = TranslationHandler(client)

        async with create_task_group() as tg:
            async for operation in parse_diff(diff_path):
                tg.start_soon(handler.handle_upload, operation, project_files, hans_dir)

    elif mode == "download":
        hant_dir = root / "Hant"
        merger = TranslationMerger(client)

        async with create_task_group() as tg:
            for file in project_files:
                tg.start_soon(merger.merge_translation, file, hans_dir, hant_dir)

    elif mode == "replace" and (reference_file is not None):
        replacer = Replacer(client, AnyioPath(reference_file))

        async with create_task_group() as tg:
            for file in project_files:
                tg.start_soon(replacer.handle_replace, file)

    await client.client.aclose()


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(description="ParaTranz Synchronization Daemon")
    parser.add_argument("mode", choices=["upload", "download", "replace"])
    parser.add_argument("-d", "--storyline-folder", required=True)
    parser.add_argument("-c", "--max-concurrency", type=int, default=8)
    parser.add_argument("-f", "--reference-file", type=str, default=None)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("paratranz_sync.log")],
    )

    run(main_entry, args.mode, args.storyline_folder, args.max_concurrency, args.reference_file)
