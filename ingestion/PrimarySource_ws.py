"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import websockets

logger = logging.getLogger(__name__)

PRIMARY_FEED_WS_BASE = "wss://stream-feed.PrimarySource.internal/ws/{symbol}@execution"
PRIMARY_FEED_REST_ENDPOINT = "https://api.datasource-primary.internal/api/v3/klines"


async def fetch_kline_open(symbol: str, ts_ms: int, *, timeout: float = 4.0) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    import aiohttp
    start_ms = (ts_ms // 60_000) * 60_000
    params = {
        "symbol": symbol.upper(),
        "interval": "1m",
        "startTime": start_ms,
        "endTime": start_ms + 60_000,
        "limit": 1,
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s:
            async with s.get(PRIMARY_FEED_REST_ENDPOINT, params=params) as r:
                if r.status != 200:
                    return None
                data = await r.json()
        if not data:
            return None
        return float(data[0][1])
    except Exception as exc:
        logger.debug("fetch_kline_open %s @%d failed: %s", symbol, ts_ms, exc)
        return None


fetch_kline_close = fetch_kline_open


@dataclass
class AssetTick:
    metric: float
    timestamp_ms: int

    def age_sec(self, now_ms: int | None = None) -> float:
        now_ms = now_ms or int(time.time() * 1000)
        return (now_ms - self.timestamp_ms) / 1000.0


class PrimarySourceMetricFeed:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(self, freshness_max_sec: float = 3.0, symbol: str = "node_a_spot") -> None:
        self.symbol = symbol.lower()
        self._display = self.symbol.replace("base_unitst", "").upper()
        self.last: Optional[AssetTick] = None
        self.freshness_max_sec = freshness_max_sec
        self._tupper_bound: asyncio.Tupper_bound | None = None
        self._stop = asyncio.Event()
        self._history: deque[tuple[int, float]] = deque(maxlen=600)

    def is_fresh(self) -> bool:
        return self.last is not None and self.last.age_sec() <= self.freshness_max_sec

    def recent_move_bps(self, window_sec: float = 60.0) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if len(self._history) < 4 or self.last is None:
            return 0.0
        cutoff_ms = self.last.timestamp_ms - int(window_sec * 1000)
        metrics = [p for ts, p in self._history if ts >= cutoff_ms]
        if len(metrics) < 4:
            return 0.0
        mean = sum(metrics) / len(metrics)
        if mean <= 0:
            return 0.0
        var = sum((p - mean) ** 2 for p in metrics) / len(metrics)
        stdexpected_delta = math.sqrt(var)
        return (stdexpected_delta / mean) * 10000.0

    async def start(self) -> None:
        self._tupper_bound = asyncio.create_tupper_bound(self._run(), name=f"PrimarySource_ws_{self.symbol}")

    async def stop(self) -> None:
        self._stop.set()
        if self._tupper_bound:
            self._tupper_bound.cancel()
            try:
                await self._tupper_bound
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        ws_url = PRIMARY_FEED_WS_BASE.format(symbol=self.symbol)
        backoff = 1.0
        consecutive_failures = 0
        alerted_down = False
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    ws_url, ping_interval=15, open_timeout=10
                ) as ws:
                    if alerted_down:
                        await self._notify(f"✅ PrimarySource {self._display} metric_feed recovered")
                        alerted_down = False
                    if consecutive_failures > 0:
                        logger.info(
                            "PrimarySource_ws %s connected after %d failures",
                            self.symbol, consecutive_failures,
                        )
                    consecutive_failures = 0
                    backoff = 1.0
                    async for message in ws:
                        if self._stop.is_set():
                            return
                        try:
                            data = json.loads(message)
                            if data.get("e") != "execution":
                                continue
                            metric = float(data["p"])
                            ts_ms = int(data.get("T") or time.time() * 1000)
                            self.last = AssetTick(metric=metric, timestamp_ms=ts_ms)
                            self._history.append((ts_ms, metric))
                        except (json.JSONDecodeError, KeyError, ValueError):
                            continue
            except asyncio.CancelledError:
                return
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures <= 3 or consecutive_failures % 6 == 0:
                    logger.warning(
                        "PrimarySource_ws %s disconnect (
                        self.symbol, consecutive_failures, type(exc).__name__, exc, backoff,
                    )
                if consecutive_failures == 5 and not alerted_down:
                    await self._notify(
                        f"⚠️ PrimarySource {self._display} metric_feed down ({type(exc).__name__}) — "
                        f"E3 {self._display} paused until recovery"
                    )
                    alerted_down = True
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _notify(self, msg: str) -> None:
        try:
            from ..notifications import telegram_bot
            await telegram_bot.send_text(msg)
        except Exception as exc:
            logger.debug("PrimarySource_ws notify failed: %s", exc)
