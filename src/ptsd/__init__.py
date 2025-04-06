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
from .processor import ContextHandler, Replacer, TranslationMerger

# Change PARATRANZ_PROJECT_ID to your ParaTranz project ID
PARATRANZ_PROJECT_ID: int = 14095

# Change TARGET_DIR to the name of your output folder (e.g., cn, RU, Th etc.).
# It must match the name in line 14 of the `.github/workflows/release.yml` file.
TARGET_DIR: str = "Hant"


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

    # ParaTranz API Client
    client = APIClient(PARATRANZ_PROJECT_ID, tokens, max_concurrency)
    project_files = [
        ProjectFile(f["id"], f["name"]) for f in (await client.get_project_files()) or []
    ]

    if mode == "upload":
        diff_path = root / "file-diff.txt"
        handler = ContextHandler(client, root)

        async with create_task_group() as tg:
            async for operation in parse_diff(diff_path):
                tg.start_soon(handler.handle_upload, operation, project_files)

    elif mode == "download":
        merger = TranslationMerger(client, root, TARGET_DIR)

        async with create_task_group() as tg:
            for file in project_files:
                tg.start_soon(merger.merge_translation, file)

    elif mode == "replace" and (reference_file is not None):
        replacer = Replacer(client, AnyioPath(reference_file))

        async with create_task_group() as tg:
            for file in project_files:
                tg.start_soon(replacer.handle_replace, file)

    await client.close()


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
