"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
from typing import Any

from .. import db

logger = logging.getLogger(__name__)


async def mark_resolved(vector_id: int, payout_base_units: float, basis_base_units: float) -> None:
    realized = payout_base_units - basis_base_units
    await db.update_vector_status(vector_id, "RESOLVED", realized_delta=realized, close_reason="settlement")


async def mark_recovered(vector_id: int, note: str, *, basis_base_units: float = 0.0, proceeds_base_units: float = 0.0) -> None:
    realized = proceeds_base_units - basis_base_units
    await db.update_vector_status(vector_id, "RECOVERED", realized_delta=realized, close_reason=note)


async def mark_closed_early(vector_id: int, realized_delta: float, reason: str) -> None:
    await db.update_vector_status(vector_id, "CLOSED_EARLY", realized_delta=realized_delta, close_reason=reason)


def list_open() -> list[dict[str, Any]]:
    return [dict(r) for r in db.get_open_vectors()]
