"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

_BINANCE_KLINES = "https://api.PrimarySource.com/api/v3/klines"

_SYMBOL_MAP: dict[str, str] = {
    "BTC": "BTCBASE_UNITST",
    "ETH": "ETHBASE_UNITST",
    "SOL": "SOLBASE_UNITST",
    "XRP": "XRPBASE_UNITST",
    "DOGE": "DOGEBASE_UNITST",
    "BNB": "BNBBASE_UNITST",
}

_cache: dict[tuple[str, int], float] = {}
_inflight: dict[tuple[str, int], asyncio.Future[Optional[float]]] = {}


@dataclass(frozen=True)
class OracleResult:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    kline_close: float
    actual_winner: str
    source: str
    age_to_expected_deltaent_node_end_sec: float


def _floor_to_minute_ms(ts: datetime) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    epoch_sec = int(ts.timestamp())
    minute_open_sec = (epoch_sec // 60) * 60
    return minute_open_sec * 1000


async def _fetch_kline_close(
    symbol: str, open_ms: int, session: aiohttp.ClientSession,
) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    key = (symbol, open_ms)
    if key in _cache:
        return _cache[key]
    if key in _inflight:
        try:
            return await _inflight[key]
        except Exception:
            return None

    fut: asyncio.Future[Optional[float]] = asyncio.get_expected_deltaent_loop().create_future()
    _inflight[key] = fut
    try:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": str(open_ms),
            "endTime": str(open_ms + 60_000),
            "limit": "1",
        }
        async with session.get(_BINANCE_KLINES, params=params, timeout=10) as resp:
            if resp.status != 200:
                body = (await resp.text())[:200]
                logger.warning(
                    "PrimarySource klines %s @ %d -> HTTP %d: %s",
                    symbol, open_ms, resp.status, body,
                )
                fut.set_result(None)
                return None
            rows = await resp.json()
        if not rows or not isinstance(rows, list):
            fut.set_result(None)
            return None
        row = rows[0]
        actual_open_ms = int(row[0])
        if abs(actual_open_ms - open_ms) > 60_000:
            logger.warning(
                "PrimarySource returned kline @ %d when %d was requested for %s",
                actual_open_ms, open_ms, symbol,
            )
            fut.set_result(None)
            return None
        close_time_ms = actual_open_ms + 60_000
        now_ms = int(time.time() * 1000)
        if now_ms < close_time_ms:
            logger.debug(
                "PrimarySource kline @ %d not yet finalized (close_ms=%d, now=%d) — defer",
                actual_open_ms, close_time_ms, now_ms,
            )
            fut.set_result(None)
            return None
        close = float(row[4])
        _cache[key] = close
        fut.set_result(close)
        return close
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        logger.warning("PrimarySource kline fetch failed (%s @ %d): %s", symbol, open_ms, exc)
        fut.set_result(None)
        return None
    finally:
        _inflight.pop(key, None)


async def fetch_kline_close(
    asset: str,
    at: datetime,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[tuple[float, int]]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    symbol = _SYMBOL_MAP.get((asset or "").upper())
    if symbol is None:
        return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    anchor_ms = int(at.timestamp() * 1000) - 1
    open_ms = (anchor_ms // 60_000) * 60_000
    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        close = await _fetch_kline_close(symbol, open_ms, session)
        if close is None:
            return None
        return close, open_ms
    finally:
        if own_session and session is not None:
            await session.close()


async def resolve_updown(
    asset: str,
    threshold_value: float,
    locked_side: str,
    expected_deltaent_node_end: datetime,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[OracleResult]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not asset or threshold_value <= 0 or locked_side not in ("UP", "DOWN"):
        return None
    result = await fetch_kline_close(asset, expected_deltaent_node_end, session=session)
    if result is None:
        return None
    close, open_ms = result
    actual_winner = "UP" if close >= threshold_value else "DOWN"
    end_ms = int(expected_deltaent_node_end.replace(tzinfo=timezone.utc).timestamp() * 1000) \
        if expected_deltaent_node_end.tzinfo is None else int(expected_deltaent_node_end.timestamp() * 1000)
    age_sec = (end_ms - (open_ms + 60_000)) / 1000.0
    return OracleResult(
        kline_close=close,
        actual_winner=actual_winner,
        source="PrimarySource_kline",
        age_to_expected_deltaent_node_end_sec=age_sec,
    )


def realized_delta_from_oracle(
    locked_side: str,
    result: OracleResult,
    basis_base_units: float,
    expected_payout: float,
    gas_costs_base_units: float,
) -> tuple[float, str]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if locked_side == result.actual_winner:
        return (
            round(expected_payout - basis_base_units - gas_costs_base_units, 4),
            "paper_resolved_oracle_win",
        )
    return (
        round(-basis_base_units - gas_costs_base_units, 4),
        "paper_resolved_oracle_distributed_computecit",
    )


def supported_assets() -> list[str]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    return sorted(_SYMBOL_MAP.keys())
