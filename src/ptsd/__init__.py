# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import argparse
import logging
from os import environ

from anyio import Path as AnyioPath
from anyio import create_task_group, run

from .core import ProjectFile
from .core.paratranz import APIClient
from .core.utils import parse_diff, save_json_file
from .processor import ContextHandler, Replacer, ResidueCleaner, TranslationMerger

logger = logging.getLogger(__name__)

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
    apply: bool = False,
    context_only: bool = False,
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

    elif mode == "cleanup":
        # One-off maintenance pass over the whole *existing* project. Defaults to
        # a dry-run: nothing is written to ParaTranz, only counted and written to
        # `cleanup_report.json` for review. Pass --apply to actually push the
        # (narrowly-scoped, force=true) clears.
        cleaner = ResidueCleaner(client, apply=apply, context_only=context_only)
        reports: list[dict] = []

        async def _run_cleanup(pf: ProjectFile) -> None:
            reports.append(await cleaner.clean_file(pf))

        async with create_task_group() as tg:
            for file in project_files:
                tg.start_soon(_run_cleanup, file)

        total_blank = sum(r["cleared_blank"] for r in reports)
        total_context = sum(r["cleared_context"] for r in reports)
        wrongly_hidden = sum(len(r["flagged_maybe_wrongly_hidden"]) for r in reports)
        visible_placeholder = sum(len(r["flagged_visible_placeholder"]) for r in reports)

        logger.info(
            f"Cleanup {'APPLIED' if apply else 'DRY-RUN (pass --apply to write)'} "
            f"{'[context-only]' if context_only else '[context+translation]'}: "
            f"{total_blank} blank entries cleared, {total_context} context-only clears, "
            f"{wrongly_hidden} flagged as possibly-wrongly-hidden (needs human review), "
            f"{visible_placeholder} flagged as visible-but-placeholder-original (informational)",
        )

        report_path = root / "cleanup_report.json"
        await save_json_file({"apply": apply, "reports": reports}, report_path)
        logger.info(f"Full report written to {report_path}")

    await client.close()


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(description="ParaTranz Synchronization Daemon")
    parser.add_argument("mode", choices=["upload", "download", "replace", "cleanup"])
    parser.add_argument("-d", "--storyline-folder", default=".")
    parser.add_argument("-c", "--max-concurrency", type=int, default=8)
    parser.add_argument("-f", "--reference-file", type=str, default=None)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="cleanup mode only: actually push the changes. Default is dry-run (report only).",
    )
    parser.add_argument(
        "--context-only",
        action="store_true",
        help=(
            "cleanup mode only: never write to the `translation` field, only "
            "`context`. Use when translation content has already been reconciled "
            "by hand and only leftover context still needs clearing."
        ),
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("paratranz_sync.log")],
    )

    run(
        main_entry,
        args.mode,
        args.storyline_folder,
        args.max_concurrency,
        args.reference_file,
        args.apply,
        args.context_only,
    )
