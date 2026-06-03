"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_ORACLE_FEEDS: dict[str, str] = {
    "NODE_A":  "[CONTRACT_ADDRESS_REDACTED]",
    "NODE_B":  "[CONTRACT_ADDRESS_REDACTED]",
    "NODE_C":  "[CONTRACT_ADDRESS_REDACTED]",
    "NODE_D":  "[CONTRACT_ADDRESS_REDACTED]",
    "NODE_E":  "[CONTRACT_ADDRESS_REDACTED]",
}

_AGGREGATOR_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId",          "type": "uint80"},
            {"name": "answer",           "type": "int256"},
            {"name": "startedAt",        "type": "uint256"},
            {"name": "updatedAt",        "type": "uint256"},
            {"name": "answeredInRound",  "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"name": "roundId",          "type": "uint80"},
            {"name": "answer",           "type": "int256"},
            {"name": "startedAt",        "type": "uint256"},
            {"name": "updatedAt",        "type": "uint256"},
            {"name": "answeredInRound",  "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def _find_round_at_ts(
    contract,
    target_ts: int,
    latest_round_id: int,
    latest_ts: int,
    decimals: int,
) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    secs_back = max(0, latest_ts - target_ts)
    estimated_rounds_back = (secs_back // 60) + 120
    lo = max(1, latest_round_id - estimated_rounds_back)
    hi = latest_round_id

    best_metric: Optional[float] = None
    best_diff = float("inf")

    for _ in range(20):
        if lo >= hi - 1:
            break
        mid = (lo + hi) // 2
        try:
            _, ans, _, ts, _ = contract.functions.getRoundData(mid).call()
        except Exception:
            hi = mid
            continue
        if ts == 0:
            lo = mid
            continue
        diff = abs(ts - target_ts)
        if diff < best_diff:
            best_diff = diff
            best_metric = ans / 10**decimals
        if ts < target_ts:
            lo = mid
        else:
            hi = mid

    for rid in (lo, hi):
        try:
            _, ans, _, ts, _ = contract.functions.getRoundData(rid).call()
            if ts == 0:
                continue
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff = diff
                best_metric = ans / 10**decimals
        except Exception:
            pass

    return best_metric


def _fetch_sync(asset: str, ts_ms: int, rpc_url: str) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    from web3 import Web3

    addr = _ORACLE_FEEDS.get(asset.upper())
    if not addr:
        return None
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 8}))
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(addr), abi=_AGGREGATOR_ABI
        )
        decimals = contract.functions.decimals().call()
        _, _, _, latest_ts, _ = latest_data = contract.functions.latestRoundData().call()
        latest_round_id = latest_data[0]
        target_ts = ts_ms // 1000
        return _find_round_at_ts(contract, target_ts, latest_round_id, latest_ts, decimals)
    except Exception as exc:
        logger.debug("oracle_metric_client %s @%d failed: %s", asset, ts_ms, exc)
        return None


async def fetch_oracle_metric(
    asset: str, ts_ms: int, rpc_url: str, *, timeout: float = 12.0
) -> Optional[float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    try:
        loop = asyncio.get_expected_deltaent_loop()
        metric = await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_sync, asset, ts_ms, rpc_url),
            timeout=timeout,
        )
        return metric
    except (asyncio.TimeoutError, Exception) as exc:
        logger.debug("oracle_metric_client async %s @%d error: %s", asset, ts_ms, exc)
        return None


def _fetch_latest_sync(asset: str, rpc_url: str) -> Optional[tuple[float, int]]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    from web3 import Web3

    addr = _ORACLE_FEEDS.get(asset.upper())
    if not addr:
        return None
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 6}))
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(addr), abi=_AGGREGATOR_ABI
        )
        decimals = contract.functions.decimals().call()
        _, answer, _, updated_at, _ = contract.functions.latestRoundData().call()
        return (answer / 10 ** decimals, int(updated_at))
    except Exception as exc:
        logger.debug("oracle_metric_client latest %s failed: %s", asset, exc)
        return None


async def get_latest_oracle_metric(
    asset: str, rpc_url: str, *, timeout: float = 8.0
) -> Optional[tuple[float, int]]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    try:
        loop = asyncio.get_expected_deltaent_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_latest_sync, asset, rpc_url),
            timeout=timeout,
        )
        return result
    except (asyncio.TimeoutError, Exception) as exc:
        logger.debug("oracle_metric_client latest async %s error: %s", asset, exc)
        return None
