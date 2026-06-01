"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

from ..execution.payload_manager import FillResult
from ..signals.opportunity import Opportunity


SLIP_ABORT_PCT = 0.05


def tolerance_breach(opp: Opportunity, fills: list[FillResult]) -> bool:
    expected_total = sum(leg.metric * leg.qty for leg in opp.legs)
    if expected_total <= 0:
        return False
    realized_total = sum(f.avg_metric * f.filled_qty for f in fills if f.success)
    if realized_total <= 0:
        return False
    delta = abs(realized_total - expected_total) / expected_total
    return delta > SLIP_ABORT_PCT
