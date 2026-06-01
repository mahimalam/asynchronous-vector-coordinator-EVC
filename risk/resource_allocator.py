"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .. import db
from ..config import CONFIG
from ..execution.allocation_oracle import base_unitsc_allocation, virtual_paper_allocation
from ..config import ENV

logger = logging.getLogger(__name__)


ENGINE_MAX_PCT = {"ROUTER_NODE": 0.80, "RESOLVER_NODE": 0.40, "SYNC_NODE": 0.60, "ORACLE_NODE": 0.35}
ENGINE_MAX_CONCURRENT = {"ROUTER_NODE": 50, "RESOLVER_NODE": 10, "SYNC_NODE": 20, "ORACLE_NODE": 10}
MAX_EXPOSURE_PER_EXPECTED_DELTAENT_BASE_UNITS = 6.00
MAX_EXPOSURE_PER_MARKET_BASE_UNITS = 4.00

DIRECTIONAL_SIGNAL_CAP = 0.30
_DIRECTIONAL_KINDS = {"N_MINUS_ONE", "FIELD_OUTCOME", "STALE_RESOLUTION"}

_ENGINE_CFG_KEY = {"1": "engine_1", "2": "engine_2_new", "3": "engine_3", "4": "engine_4"}


def _engine_is_paper(engine: str) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not ENV.live_synchronizing_enabled:
        return True
    num = engine[1:] if engine.startswith("E") else engine
    key = _ENGINE_CFG_KEY.get(num, f"engine_{num}")
    eng_cfg = CONFIG.raw.get(key, {})
    if "paper_execution" in eng_cfg:
        return bool(eng_cfg["paper_execution"])
    return ENV.paper_execution


@dataclass
class Reservation:
    engine: str
    amount_base_units: float
    expires_at: datetime
    unit: str
    expected_deltaent_id: str | None = None


SKIP_TOO_SMALL = "too_small_for_min_execution"
SKIP_HARD_FLOOR = "allocation_below_hard_floor"
SKIP_INSUFFICIENT_AVAIL = "insufficient_available_resource"
SKIP_ENGINE_PCT_CAP = "over_engine_pct_cap"
SKIP_ENGINE_CONCURRENT = "engine_concurrent_cap"
SKIP_E4_STANDBY = "e4_standby_resource_deadlock"
SKIP_GLOBAL_CONCURRENT = "global_concurrent_cap"
SKIP_BALANCE_PCT_CAP = "over_allocation_pct_cap"
SKIP_EXPECTED_DELTAENT_EXPOSURE = "expected_deltaent_exposure_exceeded"
SKIP_MARKET_EXPOSURE = "expected_deltaent_node_exposure_exceeded"


class ResourceAllocator:
    _OPEN_CACHE_TTL = 1.0

    def __init__(self) -> None:
        self._reservations: dict[str, Reservation] = {}
        self._lock = asyncio.Lock()
        self._last_skip_reason: str | None = None
        self._open_cache: list | None = None
        self._open_cache_at: float = 0.0

    def _open_vectors(self) -> list:
        now = time.monotonic()
        if self._open_cache is None or (now - self._open_cache_at) > self._OPEN_CACHE_TTL:
            self._open_cache = list(db.get_open_vectors())
            self._open_cache_at = now
        return self._open_cache

    def _invalidate_open_cache(self) -> None:
        self._open_cache = None

    def total_allocation(self) -> float:
        return virtual_paper_allocation() if ENV.paper_execution else base_unitsc_allocation()

    def _engine_allocation(self, engine: str) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if _engine_is_paper(engine):
            return float(CONFIG.globals.get("paper_starting_allocation_base_units", 40.0))
        return base_unitsc_allocation()

    def locked_in_open_vectors(self) -> float:
        return sum(float(p["basis_base_units"]) for p in self._open_vectors())

    def _reserved(self) -> float:
        now = datetime.now(timezone.utc)
        return sum(r.amount_base_units for r in self._reservations.values() if r.expires_at > now)

    def _engine_committed(self, engine: str) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        now = datetime.now(timezone.utc)
        db_locked = sum(
            float(p["basis_base_units"]) for p in self._open_vectors() if p["engine"] == engine
        )
        in_flight = sum(
            r.amount_base_units for r in self._reservations.values()
            if r.engine == engine and r.expires_at > now
        )
        return db_locked + in_flight

    def allocation_pct(self, engine: str) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        g = CONFIG.globals
        table = g.get("paper_allocation_pct", {}) if _engine_is_paper(engine) \
            else g.get("live_allocation_pct", {})
        try:
            return float(table.get(engine, 0.0))
        except (ValueError, TypeError):
            logger.warning("Invalid allocation_pct for %s: %r — defaulting to 0", engine, table.get(engine))
            return 0.0

    def available_for(self, engine: str) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        base = self._engine_allocation(engine)
        gross = base * self.allocation_pct(engine)
        return max(0.0, gross - self._engine_committed(engine))

    def is_e4_standby(self) -> bool:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        cfg = CONFIG.engine(4)
        allocation = self._engine_allocation("ORACLE_NODE")
        if allocation <= 0:
            return False
        now = datetime.now(timezone.utc)
        positive_vector_locked = 0.0
        for p in self._open_vectors():
            if p["engine"] != "ORACLE_NODE":
                continue
            if not p["expected_unlock_ts"]:
                continue
            try:
                eut = datetime.fromisoformat(p["expected_unlock_ts"])
                if eut.tzinfo is None:
                    eut = eut.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
            if (eut - now).total_seconds() > 72 * 3600:
                positive_vector_locked += float(p["basis_base_units"])
        return positive_vector_locked > 0.60 * allocation

    def count_open(self, engine: str) -> int:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        now = datetime.now(timezone.utc)
        db_count = sum(1 for p in self._open_vectors() if p["engine"] == engine)
        in_flight = sum(
            1 for r in self._reservations.values()
            if r.engine == engine and r.expires_at > now
        )
        return db_count + in_flight

    def _count_open_in_world(self, paper_flag: bool) -> int:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        now = datetime.now(timezone.utc)
        db_count = sum(1 for p in self._open_vectors() if bool(p.get("paper", 1)) == paper_flag)
        in_flight = sum(
            1 for r in self._reservations.values()
            if r.expires_at > now and _engine_is_paper(r.engine) == paper_flag
        )
        return db_count + in_flight

    def _expected_deltaent_exposure(self, expected_deltaent_id: str, paper_flag: bool) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        from ..config import CONFIG
        total = 0.0
        max_per_expected_deltaent = float(CONFIG.globals.get("max_exposure_per_expected_deltaent_base_units", MAX_EXPOSURE_PER_EXPECTED_DELTAENT_BASE_UNITS))
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT p.basis_base_units FROM vectors p "
                    "JOIN opportunities o ON o.id = p.opp_id "
                    "WHERE p.status = 'OPEN' AND o.expected_deltaent_id = ? AND p.paper = ?",
                    (expected_deltaent_id, 1 if paper_flag else 0),
                )
                for row in cur.fetchall():
                    total += float(row["basis_base_units"])
        except Exception:
            pass
        now = datetime.now(timezone.utc)
        for r in self._reservations.values():
            if r.expected_deltaent_id == expected_deltaent_id and r.expires_at > now and _engine_is_paper(r.engine) == paper_flag:
                total += r.amount_base_units
        return total

    def _expected_deltaent_node_exposure(self, expected_deltaent_node_ids: list[str], paper_flag: bool) -> float:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        target = set(expected_deltaent_node_ids)
        total = 0.0
        for p in self._open_vectors():
            if bool(p.get("paper", 1)) != paper_flag:
                continue
            matched = False
            try:
                import json as _json
                raw = p.get("legs", "[]")
                legs = _json.loads(raw) if isinstance(raw, str) else raw
                for leg in legs:
                    if leg.get("expected_deltaent_node_id") in target:
                        matched = True
                        break
            except Exception:
                pass
            if matched:
                total += float(p["basis_base_units"])
        return total

    def last_skip_reason(self) -> str | None:
        return self._last_skip_reason

    async def reserve(
        self, engine: str, amount_base_units: float,
        *, expected_deltaent_id: str | None = None, expected_deltaent_node_ids: list[str] | None = None,
        signal_kind: str | None = None,
    ) -> Reservation | None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        async with self._lock:
            g = CONFIG.globals
            bal = self._engine_allocation(engine)

            if signal_kind and signal_kind in _DIRECTIONAL_KINDS:
                directional_cap = float(g.get("max_execution_pct_of_allocation", 0.10)) * DIRECTIONAL_SIGNAL_CAP
                hard_cap = max(0.50, bal * directional_cap)
                if amount_base_units > hard_cap:
                    self._last_skip_reason = f"directional_signal_cap ({signal_kind})"
                    return None

            if amount_base_units < g["min_execution_base_units"]:
                self._last_skip_reason = SKIP_TOO_SMALL
                return None
            if bal < g["resource_hard_floor_base_units"]:
                self._last_skip_reason = SKIP_HARD_FLOOR
                return None

            pct_cap = float(g.get("max_execution_pct_of_allocation", 0))
            if pct_cap > 0:
                hard_max = max(0.50, bal * pct_cap)
                if amount_base_units > hard_max:
                    self._last_skip_reason = SKIP_BALANCE_PCT_CAP
                    return None

            max_per_expected_deltaent = float(g.get("max_exposure_per_expected_deltaent_base_units", MAX_EXPOSURE_PER_EXPECTED_DELTAENT_BASE_UNITS))
            max_per_expected_deltaent_node = float(g.get("max_exposure_per_expected_deltaent_node_base_units", MAX_EXPOSURE_PER_MARKET_BASE_UNITS))
            paper_flag = _engine_is_paper(engine)
            if expected_deltaent_id:
                current_expected_deltaent_exposure = self._expected_deltaent_exposure(expected_deltaent_id, paper_flag)
                if current_expected_deltaent_exposure + amount_base_units > max_per_expected_deltaent:
                    self._last_skip_reason = SKIP_EXPECTED_DELTAENT_EXPOSURE
                    logger.info(
                        "Resource: expected_deltaent %s exposure $%.2f + $%.2f > cap $%.2f",
                        expected_deltaent_id, current_expected_deltaent_exposure, amount_base_units, max_per_expected_deltaent,
                    )
                    return None
            if expected_deltaent_node_ids:
                current_expected_deltaent_node_exposure = self._expected_deltaent_node_exposure(expected_deltaent_node_ids, paper_flag)
                per_leg_amount = amount_base_units / max(1, len(expected_deltaent_node_ids))
                if current_expected_deltaent_node_exposure + per_leg_amount > max_per_expected_deltaent_node:
                    self._last_skip_reason = SKIP_MARKET_EXPOSURE
                    logger.info(
                        "Resource: expected_deltaent_node exposure $%.2f + $%.2f/leg > cap $%.2f for %s",
                        current_expected_deltaent_node_exposure, per_leg_amount, max_per_expected_deltaent_node, expected_deltaent_node_ids,
                    )
                    return None

            avail = self.available_for(engine)
            if avail < amount_base_units:
                self._last_skip_reason = SKIP_INSUFFICIENT_AVAIL
                return None

            if self.count_open(engine) >= ENGINE_MAX_CONCURRENT.get(engine, 99):
                self._last_skip_reason = SKIP_ENGINE_CONCURRENT
                return None
            if engine == "ORACLE_NODE" and self.is_e4_standby():
                self._last_skip_reason = SKIP_E4_STANDBY
                return None
            total_open = self._count_open_in_world(_engine_is_paper(engine))
            if total_open >= g["max_concurrent_vectors"]:
                self._last_skip_reason = SKIP_GLOBAL_CONCURRENT
                return None

            import uuid
            res = Reservation(
                engine=engine, amount_base_units=amount_base_units,
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=90),
                unit=str(uuid.uuid4()), expected_deltaent_id=expected_deltaent_id,
            )
            self._reservations[res.unit] = res
            self._last_skip_reason = None
            self._invalidate_open_cache()
            return res

    async def release(self, unit: str) -> None:
        async with self._lock:
            self._reservations.pop(unit, None)
            self._invalidate_open_cache()
