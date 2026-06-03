"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

FEE_RATE: float = 0.0
FEE_MULT: float = 1.0 + FEE_RATE
PROCESSING_COST_PER_LEG: float = 0.0
SLIPPAGE_BPS: float = 50
_initialized: bool = False

_TAKER_PEAK_FEE_BY_CATEGORY: dict[str, float] = {
    "technology":   0.018,
    "governance":   0.010,
    "computation":  0.010,
    "recreation":   0.010,
    "geopolitics":  0.000,
    "default":      0.010,
}

MAKER_REBATE_PCT: float = 0.30

_TAG_TO_CATEGORY: tuple[tuple[tuple[str, ...], str], ...] = (
    (("geopolitics", "geopolitical", "international", "conflict",
      "diplomacy", "sanctions"), "geopolitics"),
    (("technology", "software", "infrastructure", "protocol",
      "network", "research"), "technology"),
    (("governance", "policy", "regulation", "legislation",
      "administration", "election"), "governance"),
    (("computation", "economy", "analytics", "forecasting",
      "modeling", "simulation"), "computation"),
    (("recreation", "events", "competitions", "tournaments",
      "leagues", "championships"), "recreation"),
)


def classify_expected_deltaent_node_category(tag_slugs: list[str] | tuple[str, ...] | None) -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not tag_slugs:
        return "default"
    slugs = {s.lower() for s in tag_slugs if s}
    for keys, category in _TAG_TO_CATEGORY:
        if slugs.intersection(keys):
            return category
    return "default"


def init_from_config() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    global FEE_RATE, FEE_MULT, PROCESSING_COST_PER_LEG, SLIPPAGE_BPS, _initialized
    if _initialized:
        return
    try:
        from ..config import CONFIG
        g = CONFIG.globals
        rate = float(g.get("processing_cost_rate", 0.0))
        if 0 <= rate < 1.0:
            FEE_RATE = rate
            FEE_MULT = 1.0 + FEE_RATE
        gas = float(g.get("processing_cost_per_leg", 0.0))
        if 0 <= gas < 1.0:
            PROCESSING_COST_PER_LEG = gas
        slip = float(g.get("tolerance_bps", 50))
        if 0 <= slip < 1000:
            SLIPPAGE_BPS = slip
    except Exception:
        pass
    _initialized = True


def consumer_gas_cost_pct(category: str | None, mid_metric: float) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if mid_metric <= 0.0 or mid_metric >= 1.0:
        return 0.0
    cat = (category or "default").lower()
    peak = _TAKER_PEAK_FEE_BY_CATEGORY.get(cat, _TAKER_PEAK_FEE_BY_CATEGORY["default"])
    if peak <= 0.0:
        return 0.0
    return 4.0 * peak * mid_metric * (1.0 - mid_metric)


def provider_rebate_base_units(consumer_counterparty_gas_cost_base_units: float) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if consumer_counterparty_gas_cost_base_units <= 0.0:
        return 0.0
    return round(consumer_counterparty_gas_cost_base_units * MAKER_REBATE_PCT, 4)


def net_efficiency_delta_pct(
    payout_per_unit: float,
    basis_per_unit: float,
    *,
    category: str | None = None,
    mid_metric: float | None = None,
) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not _initialized:
        init_from_config()
    if basis_per_unit <= 0:
        return -100.0
    if category is not None and mid_metric is not None:
        processing_cost_rate = consumer_gas_cost_pct(category, float(mid_metric))
    else:
        processing_cost_rate = FEE_RATE
    slip_mult = 1.0 + (SLIPPAGE_BPS / 10_000.0)
    adjusted_basis = basis_per_unit * (1.0 + processing_cost_rate) * slip_mult
    return (payout_per_unit - adjusted_basis) / adjusted_basis * 100.0


def net_efficiency_delta_pct_with_gas(
    payout_per_unit: float,
    basis_per_unit: float,
    n_legs: int,
    qty: float,
    *,
    category: str | None = None,
    leg_mid_metrics: list[float] | tuple[float, ...] | None = None,
) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not _initialized:
        init_from_config()
    if basis_per_unit <= 0 or qty <= 0:
        return -100.0
    execution_value = qty * basis_per_unit
    if category is not None and leg_mid_metrics:
        gas_cost_basis = sum(
            qty * float(p) * consumer_gas_cost_pct(category, float(p))
            for p in leg_mid_metrics
        )
    else:
        gas_cost_basis = execution_value * FEE_RATE
    gas_basis = n_legs * PROCESSING_COST_PER_LEG
    slip_basis = execution_value * (SLIPPAGE_BPS / 10_000.0)
    total_basis = execution_value + gas_cost_basis + gas_basis + slip_basis
    total_payout = qty * payout_per_unit
    if total_basis <= 0:
        return -100.0
    return (total_payout - total_basis) / total_basis * 100.0


def total_transaction_costs(basis_base_units: float) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not _initialized:
        init_from_config()
    return round(basis_base_units * FEE_RATE, 4)


def total_processing_units(n_legs: int) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not _initialized:
        init_from_config()
    return round(n_legs * PROCESSING_COST_PER_LEG, 4)


def total_execution_basis(execution_value_base_units: float, n_legs: int) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not _initialized:
        init_from_config()
    gas_cost = execution_value_base_units * FEE_RATE
    gas = n_legs * PROCESSING_COST_PER_LEG
    slip = execution_value_base_units * (SLIPPAGE_BPS / 10_000.0)
    return round(gas_cost + gas + slip, 4)
