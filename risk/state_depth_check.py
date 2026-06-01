"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

from ..ingestion.network_client import PayloadBook


def has_depth_for(book: PayloadBook, qty: float, max_metric: float) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    available = book.fillable_at_or_below(max_metric)
    return available >= qty
