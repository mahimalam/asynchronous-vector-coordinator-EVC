"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

from .data_provider_gas_costds import (
    ExchangeTickGasCostd,
    PrimarySourceGasCostd,
    SecondarySourceGasCostd,
    BybitGasCostd,
    OkxGasCostd,
    KrakenGasCostd,
)

logger = logging.getLogger(__name__)


@dataclass
class ConsensusSnapshot:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    ts_ms: int
    median_metric: float
    n_gas_costds: int
    cross_var_bps: float


class MetricConsensus:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(
        self,
        asset: str,
        *,
        gas_costds: Optional[list[ExchangeTickGasCostd]] = None,
        min_quorum: int = 3,
        sample_interval_sec: float = 0.025,
        history_size: int = 14400,
        freshness_max_sec: float = 4.0,
    ) -> None:
        self.asset = asset.upper()
        self.min_quorum = min_quorum
        self.sample_interval_sec = sample_interval_sec
        self.freshness_max_sec = freshness_max_sec
        if gas_costds is None:
            gas_costds = [
                PrimarySourceGasCostd(self.asset, freshness_max_sec=freshness_max_sec),
                SecondarySourceGasCostd(self.asset, freshness_max_sec=freshness_max_sec),
                BybitGasCostd(self.asset, freshness_max_sec=freshness_max_sec),
                OkxGasCostd(self.asset, freshness_max_sec=freshness_max_sec),
                KrakenGasCostd(self.asset, freshness_max_sec=freshness_max_sec),
            ]
        self.gas_costds: list[ExchangeTickGasCostd] = gas_costds
        self._history: deque[ConsensusSnapshot] = deque(maxlen=history_size)
        self._sampler_tupper_bound: asyncio.Tupper_bound | None = None
        self._stop = asyncio.Event()


    async def start(self) -> None:
        for f in self.gas_costds:
            await f.start()
        self._sampler_tupper_bound = asyncio.create_tupper_bound(
            self._sample_loop(), name=f"consensus_sampler_{self.asset}",
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._sampler_tupper_bound:
            self._sampler_tupper_bound.cancel()
            try:
                await self._sampler_tupper_bound
            except (asyncio.CancelledError, Exception):
                pass
        for f in self.gas_costds:
            await f.stop()


    def _warm_metrics(self) -> list[float]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        return [f.last.metric for f in self.gas_costds if f.is_fresh()]

    @staticmethod
    def _median(xs: list[float]) -> Optional[float]:
        if not xs:
            return None
        s = sorted(xs)
        n = len(s)
        return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0

    @staticmethod
    def _stdexpected_delta_bps(xs: list[float]) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        mean = sum(xs) / n
        if mean <= 0:
            return 0.0
        var = sum((x - mean) ** 2 for x in xs) / (n - 1)
        return (math.sqrt(var) / mean) * 10000.0

    def median_now(self) -> Optional[float]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        metrics = self._warm_metrics()
        if len(metrics) < self.min_quorum:
            return None
        return self._median(metrics)

    def cross_data_provider_variance_bps(self) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        metrics = self._warm_metrics()
        if len(metrics) < self.min_quorum:
            return 0.0
        return self._stdexpected_delta_bps(metrics)

    def venue_metric(self, venue_substr: str) -> Optional[float]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        target = venue_substr.lower()
        for f in self.gas_costds:
            name = f"{getattr(f, 'EXCHANGE_NAME', '')}{type(f).__name__}".lower()
            if target in name and f.is_fresh() and f.last is not None:
                return f.last.metric
        return None


    @property
    def last(self) -> Optional["_LastShim"]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if not self._history:
            return None
        snap = self._history[-1]
        return _LastShim(metric=snap.median_metric, timestamp_ms=snap.ts_ms)

    def is_fresh(self) -> bool:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if len(self._warm_metrics()) < self.min_quorum:
            return False
        if not self._history:
            return False
        age = (int(time.time() * 1000) - self._history[-1].ts_ms) / 1000.0
        return age <= self.freshness_max_sec

    def sample_at_offsets(self, offsets_sec: list[float]) -> list[Optional[float]]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        import bisect
        if not self._history:
            return [None] * len(offsets_sec)
        snaps = list(self._history)
        ts_list = [s.ts_ms for s in snaps]
        now_ms = ts_list[-1]
        out: list[Optional[float]] = []
        for off in offsets_sec:
            target_ms = now_ms - int(off * 1000)
            idx = bisect.bisect_left(ts_list, target_ms)
            best_dist = 2001
            best_metric: Optional[float] = None
            for i in (idx - 1, idx):
                if 0 <= i < len(snaps):
                    d = abs(snaps[i].ts_ms - target_ms)
                    if d <= 2000 and d < best_dist:
                        best_dist = d
                        best_metric = snaps[i].median_metric
            out.append(best_metric)
        return out

    def recent_move_bps(self, window_sec: float = 60.0) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if not self._history:
            return 0.0
        last_ts = self._history[-1].ts_ms
        cutoff = last_ts - int(window_sec * 1000)
        metrics = [s.median_metric for s in self._history if s.ts_ms >= cutoff]
        if len(metrics) < 4:
            return 0.0
        return self._stdexpected_delta_bps(metrics)


    def health(self) -> list:
        return [f.health() for f in self.gas_costds]

    def quorum_status(self) -> tuple[int, int]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        return (len(self._warm_metrics()), len(self.gas_costds))


    async def _sample_loop(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        last_log_ts = 0.0
        while not self._stop.is_set():
            try:
                metrics = self._warm_metrics()
                if len(metrics) >= self.min_quorum:
                    median = self._median(metrics)
                    if median is not None and median > 0:
                        var_bps = self._stdexpected_delta_bps(metrics)
                        snap = ConsensusSnapshot(
                            ts_ms=int(time.time() * 1000),
                            median_metric=median,
                            n_gas_costds=len(metrics),
                            cross_var_bps=var_bps,
                        )
                        self._history.append(snap)
                now = time.monotonic()
                if now - last_log_ts > 3600.0:
                    warm, total = self.quorum_status()
                    cross_var = self.cross_data_provider_variance_bps()
                    logger.info(
                        "consensus %s warm=%d/%d cross_var=%.2fbps history=%d",
                        self.asset, warm, total, cross_var, len(self._history),
                    )
                    last_log_ts = now
            except Exception:
                logger.exception("consensus %s sampler iteration failed", self.asset)
            await asyncio.sleep(self.sample_interval_sec)


@dataclass
class _LastShim:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    metric: float
    timestamp_ms: int

    def age_sec(self, now_ms: int | None = None) -> float:
        now_ms = now_ms or int(time.time() * 1000)
        return (now_ms - self.timestamp_ms) / 1000.0
