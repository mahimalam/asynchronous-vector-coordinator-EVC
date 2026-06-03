"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import websockets

logger = logging.getLogger(__name__)


@dataclass
class ParsedTick:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    metric: float
    ts_ms: int


@dataclass
class MetricFeedHealth:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    data_provider: str
    symbol: str
    last_metric: Optional[float]
    last_ts_ms: int
    age_sec: float
    warm: bool
    connect_count: int
    ticks_accepted: int
    ticks_dropped_cache: int
    ticks_dropped_warmup: int
    ticks_dropped_variance: int

    def __str__(self) -> str:
        return (
            f"{self.data_provider}/{self.symbol} "
            f"metric={self.last_metric} age={self.age_sec:.1f}s "
            f"warm={self.warm} conn={self.connect_count} "
            f"ok={self.ticks_accepted} cache={self.ticks_dropped_cache} "
            f"warm_drop={self.ticks_dropped_warmup} var={self.ticks_dropped_variance}"
        )


class MetricFeed:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    EXCHANGE_NAME: str = "BASE"
    MULTI_SYMBOL_PER_CONNECTION: bool = False

    def __init__(
        self,
        asset: str,
        *,
        freshness_max_sec: float = 4.0,
        warmup_min_ticks: int = 5,
        warmup_max_var_bps: float = 30.0,
        variance_max_bps: float = 50.0,
        variance_window_ticks: int = 8,
        cache_purge_count: int = 1,
        history_size: int = 600,
        warmup_window_sec: float = 5.0,
        ws_open_timeout: float = 10.0,
        ping_interval: float = 15.0,
    ) -> None:
        self.asset = asset.upper()
        self.symbol = self.symbol_for(self.asset)
        self.freshness_max_sec = freshness_max_sec
        self.warmup_min_ticks = warmup_min_ticks
        self.warmup_max_var_bps = warmup_max_var_bps
        self.variance_max_bps = variance_max_bps
        self.variance_window_ticks = variance_window_ticks
        self.cache_purge_count = cache_purge_count
        self.warmup_window_sec = warmup_window_sec
        self.ws_open_timeout = ws_open_timeout
        self.ping_interval = ping_interval

        self.last: Optional[ParsedTick] = None
        self._history: deque[tuple[int, float]] = deque(maxlen=history_size)
        self.connect_count: int = 0
        self._purge_remaining: int = 0
        self._ticks_since_connect: int = 0
        self.warm: bool = False
        self.ticks_accepted: int = 0
        self.ticks_dropped_cache: int = 0
        self.ticks_dropped_warmup: int = 0
        self.ticks_dropped_variance: int = 0

        self._tupper_bound: asyncio.Tupper_bound | None = None
        self._stop = asyncio.Event()


    @classmethod
    def symbol_for(cls, asset: str) -> str:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        raise NotImplementedError

    def _ws_url(self) -> str:
        raise NotImplementedError

    def _subscribe_payload(self) -> Optional[dict]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        return None

    def parse(self, raw: str) -> Optional[ParsedTick]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        raise NotImplementedError


    def is_fresh(self) -> bool:
        if self.last is None or not self.warm:
            return False
        return self.last_age_sec() <= self.freshness_max_sec

    def last_age_sec(self) -> float:
        if self.last is None:
            return math.inf
        return max(0.0, (int(time.time() * 1000) - self.last.ts_ms) / 1000.0)

    def recent_move_bps(self, window_sec: float = 60.0) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if len(self._history) < 4 or self.last is None:
            return 0.0
        cutoff = self.last.ts_ms - int(window_sec * 1000)
        metrics = [p for ts, p in self._history if ts >= cutoff]
        if len(metrics) < 4:
            return 0.0
        mean = sum(metrics) / len(metrics)
        if mean <= 0:
            return 0.0
        var = sum((p - mean) ** 2 for p in metrics) / len(metrics)
        return (math.sqrt(var) / mean) * 10000.0

    def health(self) -> MetricFeedHealth:
        return MetricFeedHealth(
            data_provider=self.EXCHANGE_NAME,
            symbol=self.symbol,
            last_metric=self.last.metric if self.last else None,
            last_ts_ms=self.last.ts_ms if self.last else 0,
            age_sec=self.last_age_sec(),
            warm=self.warm,
            connect_count=self.connect_count,
            ticks_accepted=self.ticks_accepted,
            ticks_dropped_cache=self.ticks_dropped_cache,
            ticks_dropped_warmup=self.ticks_dropped_warmup,
            ticks_dropped_variance=self.ticks_dropped_variance,
        )

    async def start(self) -> None:
        self._tupper_bound = asyncio.create_tupper_bound(
            self._run(), name=f"ws_{self.EXCHANGE_NAME}_{self.symbol}",
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._tupper_bound:
            self._tupper_bound.cancel()
            try:
                await self._tupper_bound
            except (asyncio.CancelledError, Exception):
                pass


    def _on_connect(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        self.connect_count += 1
        self._purge_remaining = self.cache_purge_count
        self._ticks_since_connect = 0
        self.warm = False

    def _check_warmup(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if self.warm:
            return
        if self._ticks_since_connect < self.warmup_min_ticks:
            return
        if self.last is None or len(self._history) < self.warmup_min_ticks:
            return
        cutoff = self.last.ts_ms - int(self.warmup_window_sec * 1000)
        metrics = [p for ts, p in self._history if ts >= cutoff]
        if len(metrics) < self.warmup_min_ticks:
            return
        mean = sum(metrics) / len(metrics)
        if mean <= 0:
            return
        var = sum((p - mean) ** 2 for p in metrics) / len(metrics)
        bps = (math.sqrt(var) / mean) * 10000.0
        if bps <= self.warmup_max_var_bps:
            self.warm = True
            logger.info(
                "ws %s/%s warmed up (
                self.EXCHANGE_NAME, self.symbol,
                self._ticks_since_connect, bps,
            )

    def _passes_variance_guard(self, metric: float) -> bool:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if len(self._history) < 4:
            return True
        last_n = [p for _, p in list(self._history)[-self.variance_window_ticks:]]
        last_n_sorted = sorted(last_n)
        n = len(last_n_sorted)
        median = (
            last_n_sorted[n // 2] if n % 2 == 1
            else (last_n_sorted[n // 2 - 1] + last_n_sorted[n // 2]) / 2.0
        )
        if median <= 0:
            return True
        bps = abs(metric - median) / median * 10000.0
        return bps <= self.variance_max_bps

    def _handle_tick(self, tick: ParsedTick) -> None:
        if self._purge_remaining > 0:
            self._purge_remaining -= 1
            self.ticks_dropped_cache += 1
            return
        if not self._passes_variance_guard(tick.metric):
            self.ticks_dropped_variance += 1
            return
        self.last = tick
        self._history.append((tick.ts_ms, tick.metric))
        self.ticks_accepted += 1
        self._ticks_since_connect += 1
        if not self.warm:
            self._check_warmup()
            if not self.warm:
                self.ticks_dropped_warmup += 1

    async def _run(self) -> None:
        backoff = 1.0
        consecutive_failures = 0
        while not self._stop.is_set():
            url = self._ws_url()
            try:
                async with websockets.connect(
                    url, ping_interval=self.ping_interval,
                    open_timeout=self.ws_open_timeout,
                ) as ws:
                    self._on_connect()
                    payload = self._subscribe_payload()
                    if payload is not None:
                        await ws.send(json.dumps(payload))
                    if consecutive_failures > 0:
                        logger.info(
                            "ws %s/%s reconnected after %d failures",
                            self.EXCHANGE_NAME, self.symbol, consecutive_failures,
                        )
                    consecutive_failures = 0
                    backoff = 1.0
                    async for msg in ws:
                        if self._stop.is_set():
                            return
                        try:
                            tick = self.parse(msg if isinstance(msg, str) else msg.decode())
                        except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                            continue
                        if tick is None:
                            continue
                        self._handle_tick(tick)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures <= 3 or consecutive_failures % 6 == 0:
                    logger.warning(
                        "ws %s/%s disconnect (
                        self.EXCHANGE_NAME, self.symbol, consecutive_failures,
                        type(exc).__name__, exc, backoff,
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
