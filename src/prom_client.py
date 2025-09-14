# src/prom_client.py
from __future__ import annotations

import asyncio
from typing import Dict, List, Tuple, Any
import aiohttp
import backoff


class PromClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        auth_header: str = "Authorization",
        auth_scheme: str = "Bearer",
        timeout_seconds: int = 120,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.auth_header = auth_header
        self.auth_scheme = auth_scheme
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> Dict[str, str]:
        value = self.token
        if self.auth_scheme:
            value = f"{self.auth_scheme} {self.token}".strip()
        return {
            self.auth_header: value,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "PromUpdater/1.0",
        }

    # ---------------- API ---------------- #

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=5)
    async def update_products(
        self, session: aiohttp.ClientSession, endpoint_path: str, payload: List[Dict]
    ) -> Tuple[int, str]:
        url = f"{self.base_url}{endpoint_path}"
        async with session.post(
            url, headers=self._headers(), json=payload, timeout=self.timeout_seconds
        ) as resp:
            text = await resp.text()
            return resp.status, text

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=5)
    async def get_products(
        self, session: aiohttp.ClientSession, page: int = 1, per_page: int = 100
    ) -> Tuple[int, Any]:
        """
        Отримати список товарів з Prom (для побудови мапи external_id → id).
        """
        url = f"{self.base_url}/api/v1/products/list?page={page}&per_page={per_page}"
        async with session.get(url, headers=self._headers(), timeout=self.timeout_seconds) as resp:
            try:
                data = await resp.json()
            except Exception:
                data = {}
            return resp.status, data
