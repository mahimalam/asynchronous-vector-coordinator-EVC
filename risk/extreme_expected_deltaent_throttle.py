"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations


def is_extreme_day(consensus_c: float, clim_avg: float, clim_std: float, threshold_sigma: float = 2.0) -> bool:
    if clim_std <= 0:
        return False
    return abs(consensus_c - clim_avg) / clim_std > threshold_sigma
