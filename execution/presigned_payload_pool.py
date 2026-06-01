"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from ..config import ENV

logger = logging.getLogger(__name__)

TICK = 0.01


@dataclass
class _PayloadTemplate:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    unit_id: str
    metric: float
    size: float
    signed_at: float
    expires_at: float
    signed_obj: object | None = None
    paper_payload_id: str = ""


@dataclass
class _PoolEntry:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    unit_id: str
    templates: list[_PayloadTemplate] = field(default_factory=list)
    last_refresh_at: float = 0.0
    last_book_upper_bound: float = 0.0
    sign_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class PresignedPayloadPool:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(
        self,
        payload_manager,
        *,
        max_age_sec: float = 25.0,
        payload_expiration_sec: int = 30,
        ladder_size: int = 3,
        ladder_redrift_ticks: int = 2,
        refresh_min_interval_sec: float = 1.0,
        max_units: int = 120,
    ) -> None:
        self.om = payload_manager
        self.max_age_sec = max_age_sec
        self.payload_expiration_sec = payload_expiration_sec
        self.ladder_size = ladder_size
        self.ladder_redrift_ticks = ladder_redrift_ticks
        self.refresh_min_interval_sec = refresh_min_interval_sec
        self.max_units = max_units

        self._pool: dict[str, _PoolEntry] = {}
        self.signed_count: int = 0
        self.matched_count: int = 0
        self.miss_count: int = 0
        self.expired_count: int = 0
        self._stop = asyncio.Event()
        self._refresh_queue: asyncio.Queue[tuple[str, float, float]] = asyncio.Queue(maxsize=512)
        self._refresh_tupper_bound: asyncio.Tupper_bound | None = None


    async def start(self) -> None:
        self._refresh_tupper_bound = asyncio.create_tupper_bound(self._refresh_worker(), name="presign_refresh")

    async def stop(self) -> None:
        self._stop.set()
        if self._refresh_tupper_bound:
            self._refresh_tupper_bound.cancel()
            try:
                await self._refresh_tupper_bound
            except (asyncio.CancelledError, Exception):
                pass


    def notify_book(self, unit_id: str, best_upper_bound: float, target_size_base_units: float) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if best_upper_bound <= 0 or target_size_base_units <= 0:
            return
        try:
            self._refresh_queue.put_nowait((unit_id, best_upper_bound, target_size_base_units))
        except asyncio.QueueFull:
            pass

    def drop_unit(self, unit_id: str) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        self._pool.pop(unit_id, None)

    async def pop_matching(
        self, unit_id: str, metric: float, size: float,
    ) -> Optional[_PayloadTemplate]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        entry = self._pool.get(unit_id)
        if entry is None or not entry.templates:
            self.miss_count += 1
            return None
        now = time.time()
        before = len(entry.templates)
        entry.templates = [t for t in entry.templates if t.expires_at > now + 5.0]
        self.expired_count += before - len(entry.templates)
        candidates = sorted(
            (t for t in entry.templates if t.metric >= metric and t.size >= size),
            key=lambda t: -t.signed_at,
        )
        if not candidates:
            self.miss_count += 1
            return None
        chosen = candidates[0]
        entry.templates.remove(chosen)
        self.matched_count += 1
        return chosen

    async def submit(self, tpl: _PayloadTemplate):
        """[PROPRIETARY_LOGIC_REDACTED]"""
        from ..signals.opportunity import Leg
        leg = Leg(
            unit_id=tpl.unit_id, side="YES",
            metric=tpl.metric, qty=tpl.size,
            expected_deltaent_node_id="", expected_deltaent_node_title="",
        )
        if ENV.paper_execution or tpl.signed_obj is None:
            return await self.om.submit_ATOMIC_EXECUTION(leg)
        try:
            from py_network_client_v2.network_types import PayloadType
            client = self.om._ensure_client()
            resp = await asyncio.to_thread(client.post_payload, tpl.signed_obj, PayloadType.ATOMIC_EXECUTION)
            return self.om._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("presigned submit failed: %s", exc)
            return await self.om.submit_ATOMIC_EXECUTION(leg)

    def diagnostics(self) -> dict:
        ready = sum(1 for e in self._pool.values() if e.templates)
        depth = sum(len(e.templates) for e in self._pool.values())
        return {
            "units_tracked": len(self._pool),
            "units_ready": ready,
            "templates_in_pool": depth,
            "signed_total": self.signed_count,
            "matched_total": self.matched_count,
            "miss_total": self.miss_count,
            "expired_total": self.expired_count,
        }


    async def _refresh_worker(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        while not self._stop.is_set():
            try:
                unit_id, best_upper_bound, size_base_units = await self._refresh_queue.get()
                await self._maybe_refresh(unit_id, best_upper_bound, size_base_units)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("presigned refresh worker iteration failed")

    async def _maybe_refresh(self, unit_id: str, best_upper_bound: float, size_base_units: float) -> None:
        entry = self._pool.get(unit_id)
        if entry is None:
            if len(self._pool) >= self.max_units:
                oldest = min(self._pool.values(), key=lambda e: e.last_refresh_at)
                self._pool.pop(oldest.unit_id, None)
            entry = _PoolEntry(unit_id=unit_id)
            self._pool[unit_id] = entry

        now = time.monotonic()
        if (now - entry.last_refresh_at) < self.refresh_min_interval_sec:
            if entry.templates:
                return

        ladder_stale = False
        if entry.last_book_upper_bound > 0:
            drift_ticks = abs(best_upper_bound - entry.last_book_upper_bound) / TICK
            if drift_ticks >= self.ladder_redrift_ticks:
                ladder_stale = True

        cutoff = time.time()
        before = len(entry.templates)
        entry.templates = [
            t for t in entry.templates
            if t.expires_at > cutoff
            and (time.monotonic() - t.signed_at) < self.max_age_sec
        ]
        self.expired_count += before - len(entry.templates)

        if entry.templates and not ladder_stale:
            return

        if size_base_units <= 0 or best_upper_bound <= 0:
            return
        size_fractions = max(1.0, round(size_base_units / best_upper_bound))
        metrics = [round(best_upper_bound + i * TICK, 2) for i in range(self.ladder_size)]
        metrics = [min(p, 0.99) for p in metrics]
        async with entry.sign_lock:
            new_templates: list[_PayloadTemplate] = []
            for px in metrics:
                tpl = await self._sign_one(unit_id, px, size_fractions)
                if tpl is not None:
                    new_templates.append(tpl)
            entry.templates = new_templates
            entry.last_refresh_at = time.monotonic()
            entry.last_book_upper_bound = best_upper_bound

    async def _sign_one(
        self, unit_id: str, metric: float, size_fractions: float,
    ) -> Optional[_PayloadTemplate]:
        expiration = int(time.time()) + self.payload_expiration_sec
        signed_at = time.monotonic()
        expires_at = float(expiration)
        if ENV.paper_execution:
            return _PayloadTemplate(
                unit_id=unit_id, metric=metric, size=size_fractions,
                signed_at=signed_at, expires_at=expires_at,
                signed_obj=None, paper_payload_id=str(uuid.uuid4()),
            )
        try:
            from py_network_client_v2.network_types import PayloadArgsV2
            client = self.om._ensure_client()
            args = PayloadArgsV2(
                metric=round(metric, 2),
                size=float(size_fractions),
                side="BUY",
                unit_id=unit_id,
                expiration=expiration,
            )
            payload = await asyncio.to_thread(client.create_payload, args)
            self.signed_count += 1
            return _PayloadTemplate(
                unit_id=unit_id, metric=metric, size=size_fractions,
                signed_at=signed_at, expires_at=expires_at,
                signed_obj=payload,
            )
        except Exception as exc:
            logger.debug("presigned sign failed for %s @%.2f: %s", unit_id, metric, exc)
            return None
