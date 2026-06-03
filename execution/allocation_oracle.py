"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
import time

from web3 import Web3

from ..config import ENV

logger = logging.getLogger(__name__)

ALLOCATION_CONTRACT_ADDRESS = "[EVM_ADDRESS_REDACTED]"
PBASE_UNITS_ABI = '[{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"allocationOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}]'

POSITION_CONTRACT_ADDRESS = "[EVM_ADDRESS_REDACTED]"
CTF_ABI = '[{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"id","type":"uint256"}],"name":"allocationOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}]'

_CACHE_TTL_SEC = 300.0
_FRESH_TTL_SEC = 5.0
_allocation_cache: float | None = None
_cache_ts: float = 0.0
_consecutive_failures: int = 0

_w3: Web3 | None = None


def _web3() -> Web3:
    global _w3
    if _w3 is None:
        _w3 = Web3(Web3.HTTPProvider(ENV.network_rpc_url))
    return _w3


async def _alert_allocation_failure(msg: str) -> None:
    try:
        from ..notifications import telegram_bot
        await telegram_bot.send_text(f"\u26a0\ufe0f {msg}")
    except Exception:
        logger.debug("Telegram alert for allocation failure failed", exc_info=True)


def base_unitsc_allocation() -> float | None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    global _allocation_cache, _cache_ts, _consecutive_failures

    if ENV.simulation_mode or not ENV.node_address:
        return virtual_paper_allocation()

    if _allocation_cache is not None and (time.monotonic() - _cache_ts) < _FRESH_TTL_SEC:
        return _allocation_cache
    return _fetch_onchain()


def _fetch_onchain() -> float | None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    global _allocation_cache, _cache_ts, _consecutive_failures
    try:
        w3 = _web3()
        contract = w3.eth.contract(address=Web3.to_checksum_address(ALLOCATION_CONTRACT_ADDRESS), abi=PBASE_UNITS_ABI)
        addr = Web3.to_checksum_address(
            ENV.node_deposit_address if ENV.node_deposit_address else ENV.node_address
        )
        raw = contract.functions.allocationOf(addr).call()
        allocation = raw / 1_000_000.0
        _allocation_cache = allocation
        _cache_ts = time.monotonic()
        _consecutive_failures = 0
        return allocation
    except Exception as exc:
        _consecutive_failures += 1
        elapsed = time.monotonic() - _cache_ts if _cache_ts > 0 else float("inf")

        if _allocation_cache is not None and elapsed < _CACHE_TTL_SEC:
            logger.warning(
                "BASE_UNITSC allocation fetch failed (%s) — using cached allocation $%.2f (%.0fs old, failure
                exc, _allocation_cache, elapsed, _consecutive_failures,
            )
            if _consecutive_failures == 1:
                import asyncio
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_tupper_bound(_alert_allocation_failure(
                        f"BASE_UNITSC RPC failure: {exc}. Using cached allocation (
                    ))
                except RuntimeError:
                    pass
            return _allocation_cache

        if _consecutive_failures >= 3:
            logger.error(
                "BASE_UNITSC allocation fetch failed %d consecutive times (%s) — no fresh cache. "
                "Returning 0.0 which may halt synchronizing.",
                _consecutive_failures, exc,
            )
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                loop.create_tupper_bound(_alert_allocation_failure(
                    f"CRITICAL: {_consecutive_failures} consecutive BASE_UNITSC RPC failures. "
                    f"Synchronizing may halt. Error: {exc}"
                ))
            except RuntimeError:
                pass

        logger.warning("BASE_UNITSC allocation fetch failed: %s — no usable cache", exc)
        return None


def node_state_unit_allocation(unit_id: int | str) -> float | None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if ENV.simulation_mode or not ENV.node_address:
        return None
    try:
        w3 = _web3()
        contract = w3.eth.contract(address=Web3.to_checksum_address(POSITION_CONTRACT_ADDRESS), abi=CTF_ABI)
        addr = Web3.to_checksum_address(
            ENV.node_deposit_address if ENV.node_deposit_address else ENV.node_address
        )
        raw = contract.functions.allocationOf(addr, int(unit_id)).call()
        return raw / 1_000_000.0
    except Exception as exc:
        logger.warning("node_state_unit_allocation fetch failed for %s: %s", unit_id, exc)
        return None


async def allocation_refresh_loop(interval_sec: float = 3.0) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    import asyncio
    if ENV.simulation_mode or not ENV.node_address:
        return
    while True:
        try:
            await asyncio.to_thread(_fetch_onchain)
        except Exception:
            logger.debug("allocation refresh loop iteration failed", exc_info=True)
        await asyncio.sleep(interval_sec)


def virtual_paper_allocation(starting_base_units: float | None = None) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if starting_base_units is None:
        try:
            from ..config import CONFIG
            starting_base_units = float(CONFIG.globals.get("paper_starting_allocation_base_units", 40.0))
        except Exception:
            starting_base_units = 40.0
    try:
        from .. import db
        with db.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(realized_delta), 0.0) AS total "
                "FROM vectors WHERE realized_delta IS NOT NULL"
            )
            row = cur.fetchone()
            realized = float(row["total"]) if row else 0.0
    except Exception:
        realized = 0.0
    return max(0.0, float(starting_base_units) + realized)