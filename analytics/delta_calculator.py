"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .. import db


def realized_delta_today() -> float:
    today = datetime.now(timezone.utc).date().isoformat()
    with db.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(realized_delta),0) AS delta FROM vectors "
            "WHERE date(resolved_at)=?", (today,),
        )
        return float(cur.fetchone()["delta"] or 0.0)


def realized_delta_window(days: int) -> float:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with db.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(realized_delta),0) AS delta FROM vectors "
            "WHERE resolved_at >= ?", (cutoff,),
        )
        return float(cur.fetchone()["delta"] or 0.0)
