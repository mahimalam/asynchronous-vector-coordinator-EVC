"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import logging

from web3 import Web3

from ..config import CONFIG, ENV

logger = logging.getLogger(__name__)

GAS_LIMIT = 250_000
MATIC_BASE_UNITS_FALLBACK = 0.85


def estimate_gas_base_units(matic_base_units: float = MATIC_BASE_UNITS_FALLBACK) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if ENV.paper_execution:
        return 0.0
    try:
        w3 = Web3(Web3.HTTPProvider(ENV.polygon_rpc_url))
        gas_metric_wei = w3.eth.gas_metric
        gas_metric_eth = gas_metric_wei / 1e18
        return gas_metric_eth * GAS_LIMIT * matic_base_units
    except Exception as exc:
        logger.warning("gas estimate failed: %s", exc)
        return 0.05


def is_gas_acceptable(execution_value_base_units: float) -> tuple[bool, float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    return (True, 0.0)


def is_direct_chain_gas_acceptable(execution_value_base_units: float) -> tuple[bool, float]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if ENV.paper_execution:
        return (True, 0.0)
    cap = CONFIG.globals["gas_pct_cap"]
    basis = estimate_gas_base_units()
    return (basis <= cap * execution_value_base_units, basis)
