# This file is part of ptsd project which is released under GNU GPL v3.0.
# Copyright (c) 2025- Limbus Traditional Mandarin

import logging
from itertools import cycle
from typing import Literal

from anyio import Semaphore, sleep
from httpx import AsyncClient, HTTPStatusError, RequestError

logger = logging.getLogger(__name__)


class APIClient:
    def __init__(self, project_id: int, tokens: list[str], max_concurrency: int) -> None:
        self.BASE_URL = f"https://paratranz.cn/api/projects/{project_id}"
        self.token_rotator = cycle(tokens)
        self.semaphore = Semaphore(max_concurrency)

        self.client = AsyncClient(timeout=30)

    async def request(
        self,
        method: Literal["DELETE", "GET", "POST"],
        endpoint: str,
        **kwargs,
    ) -> dict | None:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = next(self.token_rotator)

        async with self.semaphore:
            for attempt in range(3):
                try:
                    response = await self.client.request(
                        method,
                        f"{self.BASE_URL}{endpoint}",
                        headers=headers,
                        **kwargs,
                    )
                    response.raise_for_status()
                    return response.json() if method != "DELETE" else None
                except HTTPStatusError as e:
                    if e.response.status_code == 429:
                        await sleep(int(e.response.headers.get("Retry-After", 5)))
                    else:
                        logger.error(f"API ERROR: {e}")
                        break
                except RequestError as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {e!s}")
                    await sleep(2**attempt)
            return None

    async def get_project_files(self) -> list[dict]:
        return await self.request("GET", "/files")

    async def close(self) -> None:
        """Close transport and proxies."""
        await self.client.aclose()
