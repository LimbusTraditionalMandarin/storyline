# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import json
import logging
from collections.abc import AsyncGenerator

from anyio import Path as AnyioPath

from .models import FileOperation, OperationType, ProjectFile

logger = logging.getLogger(__name__)


async def parse_diff(
    diff_path: AnyioPath,
    ignore_paths: set[str] | None = None,
) -> AsyncGenerator[FileOperation]:
    """Parse a diff file and yield file operations."""
    ignore_paths = ignore_paths or set()

    try:
        async with await diff_path.open("r") as f:
            async for line in f:
                if not (line := line.strip()):
                    continue

                op_code, path = line.split(maxsplit=1)
                op_type = OperationType(op_code)
                full_path = path.strip()

                # Handle special path modification rules
                if op_type == OperationType.MODIFY and any(
                    full_path.startswith(p) for p in ignore_paths
                ):
                    logger.debug(f"Ignoring modified path: {full_path}")
                    continue

                yield FileOperation(op_type, full_path)
    except FileNotFoundError:
        logger.warning("No file-diff.txt found")


async def load_json_file(path: AnyioPath) -> dict:
    """Asynchronously load and parse a JSON file."""
    async with await path.open("r", encoding="utf-8-sig") as f:
        return json.loads(await f.read())


async def save_json_file(data: dict, path: AnyioPath) -> None:
    """Asynchronously save data to a JSON file."""
    await path.parent.mkdir(parents=True, exist_ok=True)
    async with await path.open("w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))


def match_project_file(files: list[ProjectFile], target: str) -> ProjectFile | None:
    """Find a project file matching the target name."""
    return next(
        (f for f in files if f.name.split("/")[-1] == target.split("/")[-1].split("_", 1)[-1]),
        None,
    )


def get_value_by_keys(data: dict | list, keys: list[str]) -> any:
    """Get value from nested structure using key path."""
    current = data
    for key in keys:
        if isinstance(current, list):
            try:
                index = int(key)
                current = current[index] if index < len(current) else None
            except ValueError:
                return None
        elif isinstance(current, dict):
            current = current.get(key)
        else:
            return None
        if current is None:
            break
    return current
