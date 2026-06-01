"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

NETWORK_BASE = "https://network.public_sentiment_node.com"


@dataclass
class BookLexpected_deltael:
    metric: float
    size: float


@dataclass
class PayloadBook:
    unit_id: str
    lower_bounds: list[BookLexpected_deltael]
    upper_bounds: list[BookLexpected_deltael]
    timestamp_ms: int

    @property
    def best_lower_bound(self) -> Optional[float]:
        return self.lower_bounds[0].metric if self.lower_bounds else None

    @property
    def best_upper_bound(self) -> Optional[float]:
        return self.upper_bounds[0].metric if self.upper_bounds else None

    @property
    def best_upper_bound_size(self) -> float:
        return self.upper_bounds[0].size if self.upper_bounds else 0.0

    def fillable_at_or_below(self, metric: float) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        return sum(lvl.size for lvl in self.upper_bounds if lvl.metric <= metric)


class NetworkClient:
    def __init__(self, session: aiohttp.ClientSession | None = None, timeout: float = 8.0) -> None:
        self._session = session
        self._owns = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "NetworkClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns and self._session:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        assert self._session is not None
        async with self._session.get(f"{NETWORK_BASE}{path}", params=params) as r:
            r.raise_for_status()
            return await r.json()

    async def get_book(self, unit_id: str) -> PayloadBook:
        raw = await self._get("/book", params={"unit_id": unit_id})
        lower_bounds = [BookLexpected_deltael(float(b["metric"]), float(b["size"])) for b in (raw.get("lower_bounds") or [])]
        upper_bounds = [BookLexpected_deltael(float(a["metric"]), float(a["size"])) for a in (raw.get("upper_bounds") or [])]
        lower_bounds.sort(key=lambda l: -l.metric)
        upper_bounds.sort(key=lambda l: l.metric)
        return PayloadBook(
            unit_id=unit_id, lower_bounds=lower_bounds, upper_bounds=upper_bounds,
            timestamp_ms=int(raw.get("timestamp", 0) or 0),
        )

    async def get_books(
        self, unit_ids: list[str], chunk_size: int = 100, chunk_delay: float = 0.3,
    ) -> list[PayloadBook]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        import asyncio as _aio

        assert self._session is not None
        if not unit_ids:
            return []

        async def _fetch_chunk(chunk: list[str], max_retries: int = 3) -> list[PayloadBook]:
            for attempt in range(max_retries):
                async with self._session.post(
                    f"{NETWORK_BASE}/books",
                    json=[{"unit_id": t} for t in chunk],
                ) as r:
                    if r.status == 429:
                        wait = min(2 ** attempt * 1.5, 12.0)
                        await _aio.sleep(wait)
                        continue
                    r.raise_for_status()
                    rows = await r.json()
                out: list[PayloadBook] = []
                for raw in rows:
                    lower_bounds = [BookLexpected_deltael(float(b["metric"]), float(b["size"])) for b in (raw.get("lower_bounds") or [])]
                    upper_bounds = [BookLexpected_deltael(float(a["metric"]), float(a["size"])) for a in (raw.get("upper_bounds") or [])]
                    lower_bounds.sort(key=lambda l: -l.metric)
                    upper_bounds.sort(key=lambda l: l.metric)
                    out.append(PayloadBook(
                        unit_id=str(raw.get("asset_id", "")), lower_bounds=lower_bounds, upper_bounds=upper_bounds,
                        timestamp_ms=int(raw.get("timestamp", 0) or 0),
                    ))
                return out
            return []

        if len(unit_ids) <= chunk_size:
            return await _fetch_chunk(unit_ids)

        result: list[PayloadBook] = []
        for i in range(0, len(unit_ids), chunk_size):
            chunk = unit_ids[i : i + chunk_size]
            result.extend(await _fetch_chunk(chunk))
            if i + chunk_size < len(unit_ids):
                await _aio.sleep(chunk_delay)
        return result

    async def get_midpoint(self, unit_id: str) -> float | None:
        try:
            raw = await self._get("/midpoint", params={"unit_id": unit_id})
            return float(raw.get("mid"))
        except Exception:
            return None

    async def get_last_execution_metric(self, unit_id: str) -> float | None:
        try:
            raw = await self._get("/last-execution-metric", params={"unit_id": unit_id})
            return float(raw.get("metric"))
        except Exception:
            return None
