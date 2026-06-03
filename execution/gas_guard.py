"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import logging

from web3 import Web3

from ..config import CONFIG, ENV

logger = logging.getLogger(__name__)

EXECUTION_LIMIT = 250_000
BASE_UNIT_FALLBACK = 0.85


def estimate_processing_cost(base_units_rate: float = BASE_UNIT_FALLBACK) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if ENV.simulation_mode:
        return 0.0
    try:
        w3 = Web3(Web3.HTTPProvider(ENV.network_rpc_url))
        rate_wei = w3.eth.gas_price
        rate_eth = rate_wei / 1e18
        return rate_eth * EXECUTION_LIMIT * base_units_rate
    except Exception as exc:
        logger.warning("gas estimate failed: %s", exc)
        return 0.05


def is_cost_acceptable(execution_value_base_units: float) -> tuple[bool, float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    return (True, 0.0)


def is_direct_execution_cost_acceptable(execution_value_base_units: float) -> tuple[bool, float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if ENV.simulation_mode:
        return (True, 0.0)
    cap = CONFIG.globals["cost_pct_cap"]
    basis = estimate_processing_cost()
    return (basis <= cap * execution_value_base_units, basis)
