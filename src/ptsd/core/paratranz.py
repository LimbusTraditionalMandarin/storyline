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
        method: Literal["DELETE", "GET", "POST", "PUT"],
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
                    status = e.response.status_code
                    # The response body carries ParaTranz's actual error detail;
                    # `e` alone only yields the URL and status code.
                    body = e.response.text[:2000]
                    if status == 429:
                        await sleep(int(e.response.headers.get("Retry-After", 5)))
                    elif status >= 500 and attempt < 2 and "ER_LOCK_DEADLOCK" in body:
                        # Retry only a confirmed-rolled-back MySQL deadlock on
                        # ParaTranz's own backend (their own error message says
                        # "try restarting transaction", i.e. nothing committed) -
                        # this is what silently dropped real writes (translation
                        # fix, context update, shift-fix restore) while the caller
                        # went on to log success regardless (see chat 2026-10-02,
                        # P10315). Deliberately narrower than "any 5xx": `request`
                        # is shared by every method/endpoint including the
                        # non-idempotent `POST /files` (new file creation, the ADD
                        # branch of handle_upload) - a blind retry there on some
                        # other 500 that happened to occur *after* the server
                        # actually committed (e.g. a dropped response) could create
                        # a duplicate file. Matching the specific, known-safe
                        # error code avoids that risk.
                        logger.warning(
                            f"Attempt {attempt + 1} got {status} ER_LOCK_DEADLOCK for "
                            f"{method} {endpoint}, retrying: {body}",
                        )
                        await sleep(2**attempt)
                    else:
                        logger.error(f"API ERROR: {e} | response: {body}")
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
