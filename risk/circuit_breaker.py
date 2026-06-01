"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .. import db
from ..config import CONFIG

logger = logging.getLogger(__name__)


HALT_DAILY = "halt_daily"
HALT_WEEKLY = "halt_weekly"
HALT_MANUAL = "halt_manual"
HALT_CAPITAL = "halt_resource"
_ENGINE_HALT_PREFIX = "halt_eng_daily_"
HALT_E3_PERM = "halt_e3_perm"
HALT_MM_DAILY = "halt_mm_daily"
HALT_MM_PERM = "halt_mm_perm"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def is_halted() -> tuple[bool, str | None]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    for key, label in ((HALT_MANUAL, "manual"), (HALT_CAPITAL, "resource_floor"), (HALT_DAILY, "daily"), (HALT_WEEKLY, "weekly")):
        if db.get_circuit_state(key) == "1":
            if key in (HALT_MANUAL, HALT_CAPITAL):
                return True, label
            if key == HALT_DAILY and _is_daily_expired():
                continue
            if key == HALT_WEEKLY and _is_weekly_expired():
                continue
            return True, label
    return False, None


def _is_daily_expired() -> bool:
    raw = db.get_circuit_state(f"{HALT_DAILY}_set_at")
    if not raw:
        return True
    try:
        set_at = datetime.fromisoformat(raw)
        if set_at.tzinfo is None:
            set_at = set_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    next_midnight = (set_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return _now() >= next_midnight


def _is_weekly_expired() -> bool:
    raw = db.get_circuit_state(f"{HALT_WEEKLY}_set_at")
    if not raw:
        return True
    try:
        set_at = datetime.fromisoformat(raw)
        if set_at.tzinfo is None:
            set_at = set_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return _now() >= set_at + timedelta(days=7)


async def check_and_trip(realized_delta_today: float, realized_delta_week: float, allocation_base_units: float | None) -> str | None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    g = CONFIG.globals
    
    if allocation_base_units is not None:
        hard_floor = float(g.get("resource_hard_floor_base_units", 10.0))
        if allocation_base_units < hard_floor:
            await _trip(HALT_CAPITAL)
            return "resource_floor_breached"
        
    if realized_delta_today <= -g["daily_distributed_computecit_limit_base_units"]:
        await _trip(HALT_DAILY)
        return "daily_distributed_computecit_limit"
    if realized_delta_week <= -g["weekly_distributed_computecit_limit_base_units"]:
        await _trip(HALT_WEEKLY)
        return "weekly_distributed_computecit_limit"
    return None


async def _trip(key: str) -> None:
    await db.set_circuit_state(key, "1")
    await db.set_circuit_state(f"{key}_set_at", _now().isoformat())


async def manual_halt() -> None:
    await _trip(HALT_MANUAL)


async def manual_resume() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    await db.set_circuit_state(HALT_MANUAL, "0")


async def force_resume_all() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    await db.set_circuit_state(HALT_MANUAL, "0")
    await db.set_circuit_state(HALT_CAPITAL, "0")
    await db.set_circuit_state(HALT_DAILY, "0")
    await db.set_circuit_state(HALT_WEEKLY, "0")
    await db.set_circuit_state(HALT_E3_PERM, "0")
    await db.set_circuit_state(HALT_MM_DAILY, "0")
    await db.set_circuit_state(HALT_MM_PERM, "0")
    for engine in ("ROUTER_NODE", "RESOLVER_NODE", "SYNC_NODE", "ORACLE_NODE"):
        await db.set_circuit_state(f"{_ENGINE_HALT_PREFIX}{engine}", "0")


def is_mm_halted() -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if db.get_circuit_state(HALT_MANUAL) == "1" or db.get_circuit_state(HALT_CAPITAL) == "1":
        return True
    if db.get_circuit_state(HALT_MM_PERM) == "1":
        return True
    if db.get_circuit_state(HALT_MM_DAILY) == "1":
        raw = db.get_circuit_state(f"{HALT_MM_DAILY}_set_at")
        if not raw:
            return False
        try:
            set_at = datetime.fromisoformat(raw)
            if set_at.tzinfo is None:
                set_at = set_at.replace(tzinfo=timezone.utc)
        except ValueError:
            return False
        next_midnight = (set_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return _now() < next_midnight
    return False


async def mm_delta_today_and_total(paper: bool | None = None) -> tuple[float, float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    total = db.mm_realized_total(paper=paper)
    scope = "all" if paper is None else ("paper" if paper else "live")
    day_key, base_key = f"mm_day_{scope}", f"mm_day_baseline_{scope}"
    today = _now().date().isoformat()
    stored_day = db.get_circuit_state(day_key)
    if stored_day != today:
        await db.set_circuit_state(day_key, today)
        await db.set_circuit_state(base_key, repr(total))
        baseline = total
    else:
        raw = db.get_circuit_state(base_key)
        try:
            baseline = float(raw) if raw is not None else total
        except ValueError:
            baseline = total
    return total - baseline, total


async def check_mm_and_trip(mm_delta_today: float, mm_delta_cumulative: float,
                            mm_delta_true: float | None = None) -> str | None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    mm = CONFIG.mm
    daily_limit = float(mm.get("mm_daily_distributed_computecit_limit_base_units", 6.0))
    cum_limit = float(mm.get("mm_cumulative_distributed_computecit_limit_base_units", 10.0))
    floor = mm_delta_true if mm_delta_true is not None else mm_delta_cumulative
    if floor <= -abs(cum_limit) and db.get_circuit_state(HALT_MM_PERM) != "1":
        await _trip(HALT_MM_PERM)
        logger.warning("MM permanent halt: true PnL=$%+.2f (realized=$%+.2f) limit=$%.2f — manual /resume required",
                       floor, mm_delta_cumulative, cum_limit)
        return "mm_cumulative_distributed_computecit_limit"
    if mm_delta_today <= -abs(daily_limit) and db.get_circuit_state(HALT_MM_DAILY) != "1":
        await _trip(HALT_MM_DAILY)
        logger.warning("MM daily halt: today PnL=$%+.2f limit=$%.2f", mm_delta_today, daily_limit)
        return "mm_daily_distributed_computecit_limit"
    return None


def is_engine_halted(engine: str) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if engine == "SYNC_NODE" and db.get_circuit_state(HALT_E3_PERM) == "1":
        return True
    key = f"{_ENGINE_HALT_PREFIX}{engine}"
    if db.get_circuit_state(key) != "1":
        return False
    raw = db.get_circuit_state(f"{key}_set_at")
    if not raw:
        return False
    try:
        set_at = datetime.fromisoformat(raw)
        if set_at.tzinfo is None:
            set_at = set_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    next_midnight = (set_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return _now() < next_midnight


async def check_e3_cumulative_and_trip(total_e3_delta: float) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if db.get_circuit_state(HALT_E3_PERM) == "1":
        return False
    limit = float(CONFIG.globals.get("e3_cumulative_distributed_computecit_limit_base_units", 2.56))
    if total_e3_delta <= -abs(limit):
        await _trip(HALT_E3_PERM)
        logger.warning(
            "E3 permanent halt tripped: cumulative PnL=$%+.2f limit=$%.2f — requires manual /resume",
            total_e3_delta, limit,
        )
        return True
    return False


async def check_engine_and_trip(engine: str, delta_today: float) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    g = CONFIG.globals
    limits: dict = g.get("engine_daily_distributed_computecit_limit_base_units", {})
    limit = limits.get(engine)
    if limit is None:
        return False
    if delta_today <= -abs(float(limit)):
        key = f"{_ENGINE_HALT_PREFIX}{engine}"
        await _trip(key)
        logger.warning(
            "Per-engine circuit breaker tripped: %s delta_today=$%+.2f limit=$%.2f",
            engine, delta_today, limit,
        )
        return True
    return False