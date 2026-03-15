"""
HubSpot Async HTTP Client
- Token-bucket rate limiter  (190 req / 10s for private apps)
- Exponential backoff on 429 / 5xx
- Shared aiohttp session
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)

BASE_URL = "https://api.hubapi.com"

_RATE_LIMIT_CALLS = 180
_RATE_LIMIT_WINDOW = 10.0
_MAX_RETRIES = 5
_BATCH_SIZE = 100


class RateLimiter:
    """Simple token-bucket limiter: allows N calls per `window` seconds."""

    def __init__(self, calls: int = _RATE_LIMIT_CALLS, window: float = _RATE_LIMIT_WINDOW):
        self._calls = calls
        self._window = window
        self._tokens = calls
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            if elapsed >= self._window:
                self._tokens = self._calls
                self._last_refill = now

            if self._tokens <= 0:
                sleep_for = self._window - elapsed
                logger.debug(f"Rate limit reached, sleeping {sleep_for:.2f}s")
                await asyncio.sleep(sleep_for)
                self._tokens = self._calls
                self._last_refill = time.monotonic()

            self._tokens -= 1


class HubSpotClient:
    def __init__(self, access_token: str):
        self._token = access_token
        self._limiter = RateLimiter()
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        url = f"{BASE_URL}{path}"
        session = await self._get_session()

        for attempt in range(1, _MAX_RETRIES + 1):
            await self._limiter.acquire()
            try:
                async with session.request(method, url, params=params, json=json_body) as resp:
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 10))
                        logger.warning(f"429 rate-limited on {path}. Sleeping {retry_after}s (attempt {attempt})")
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status >= 500:
                        wait = 2 ** attempt
                        logger.warning(f"{resp.status} server error on {path}. Retry {attempt}/{_MAX_RETRIES} in {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()
                    return await resp.json()

            except aiohttp.ClientError as exc:
                if attempt == _MAX_RETRIES:
                    raise
                wait = 2 ** attempt
                logger.warning(f"Network error {exc}. Retry {attempt} in {wait}s")
                await asyncio.sleep(wait)

        raise RuntimeError(f"Failed after {_MAX_RETRIES} retries: {method} {path}")

    async def get_deals_page(
        self,
        after: Optional[str] = None,
        properties: Optional[List[str]] = None,
        limit: int = 100,
    ) -> Dict:
        """GET /crm/v3/objects/deals — single cursor page."""
        params: Dict[str, Any] = {"limit": limit, "archived": "false"}
        if after:
            params["after"] = after
        if properties:
            params["properties"] = ",".join(properties)
        return await self._request("GET", "/crm/v3/objects/deals", params=params)

    async def batch_get_associations(
        self,
        from_object: str,
        to_object: str,
        ids: List[str],
    ) -> Dict:
        """POST /crm/v3/associations/{from}/{to}/batch/read"""
        body = {"inputs": [{"id": str(i)} for i in ids]}
        return await self._request(
            "POST",
            f"/crm/v3/associations/{from_object}/{to_object}/batch/read",
            json_body=body,
        )

    async def batch_read_objects(
        self,
        object_type: str,
        ids: List[str],
        properties: List[str],
    ) -> Dict:
        """POST /crm/v3/objects/{type}/batch/read"""
        body = {
            "inputs": [{"id": str(i)} for i in ids],
            "properties": properties,
        }
        return await self._request(
            "POST",
            f"/crm/v3/objects/{object_type}/batch/read",
            json_body=body,
        )

    async def get_deal_attachments(self, deal_id: str) -> Dict:
        """GET /crm/v3/objects/deals/{id}/associations/notes"""
        return await self._request(
            "GET",
            f"/crm/v3/objects/deals/{deal_id}/associations/notes",
        )

    async def get_engagement_attachments(self, engagement_id: str) -> Dict:
        """Fetch full note/engagement to get file IDs."""
        return await self._request(
            "GET",
            f"/crm/v3/objects/notes/{engagement_id}",
            params={"properties": "hs_attachment_ids,hs_note_body"},
        )