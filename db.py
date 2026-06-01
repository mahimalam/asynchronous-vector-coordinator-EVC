"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

DB_PATH = Path(__file__).resolve().parent / "data" / "bot.db"

SCHEMA = """[PROPRIETARY_LOGIC_REDACTED]"""


_write_lock = asyncio.Lock()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, isolation_lexpected_deltael=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_schema() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    with _connect() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(vectors)")}
    if "paper" not in cols:
        conn.execute("ALTER TABLE vectors ADD COLUMN paper INTEGER NOT NULL DEFAULT 1")

    mm_cols = {r[1] for r in conn.execute("PRAGMA table_info(mm_inventory)")}
    for col in ("up_unit", "down_unit"):
        if col not in mm_cols:
            conn.execute(f"ALTER TABLE mm_inventory ADD COLUMN {col} TEXT")
    for col in ("up_fractions", "up_basis", "down_fractions", "down_basis"):
        if col not in mm_cols:
            conn.execute(f"ALTER TABLE mm_inventory ADD COLUMN {col} REAL NOT NULL DEFAULT 0.0")


@contextlib.contextmanager
def cursor() -> Iterator[sqlite3.Cursor]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    conn = _connect()
    try:
        yield conn.cursor()
    finally:
        conn.close()


@contextlib.asynccontextmanager
async def write_lock():
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with _write_lock:
        conn = _connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



async def insert_opportunity(opp: dict[str, Any]) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with write_lock() as conn:
        cur = conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (
                opp["engine"], opp["kind"], opp.get("detected_at", now_iso()),
                opp.get("expected_deltaent_id"), json.dumps(opp.get("expected_deltaent_node_ids", [])),
                opp["efficiency_delta_pct"], opp["basis_base_units"], opp["expected_payout"],
                opp.get("decision", "DETECTED"), opp.get("skip_reason"),
                json.dumps(opp.get("raw_snapshot", {})),
            ),
        )
        return cur.lastrowid


async def update_opportunity_decision(
    opp_id: int, decision: str, *, skip_reason: str | None = None,
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with write_lock() as conn:
        conn.execute(
            "UPDATE opportunities SET decision=?, skip_reason=? WHERE id=?",
            (decision, skip_reason, opp_id),
        )


async def insert_vector(pos: dict[str, Any]) -> int:
    async with write_lock() as conn:
        cur = conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (
                pos.get("opp_id"), pos["engine"], pos.get("opened_at", now_iso()),
                json.dumps(pos["legs"]), pos["basis_base_units"], pos["expected_payout"],
                pos.get("status", "OPEN"), pos.get("expected_unlock_ts"),
                1 if pos.get("paper", True) else 0,
            ),
        )
        return cur.lastrowid


async def update_vector_status(
    vector_id: int, status: str, *, realized_delta: float | None = None,
    close_reason: str | None = None,
) -> None:
    if realized_delta is not None:
        try:
            with cursor() as cur:
                cur.execute(
                    "SELECT basis_base_units, expected_payout FROM vectors WHERE id=?",
                    (vector_id,),
                )
                row = cur.fetchone()
            if row:
                basis = float(row["basis_base_units"] or 0.0)
                expected = float(row["expected_payout"] or 0.0)
                if expected > 0:
                    max_delta = expected - basis
                    if realized_delta > max_delta + 0.005:
                        import logging as _logging
                        _logging.getLogger(__name__).error(
                            "PnL ceiling violation pos=
                            "(basis=$%.4f expected=$%.4f reason=%s) — CLAMPED",
                            vector_id, realized_delta, max_delta, basis, expected, close_reason,
                        )
                        realized_delta = round(max_delta, 4)
        except Exception:
            pass
    async with write_lock() as conn:
        conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (status, now_iso(), realized_delta, close_reason, vector_id),
        )


async def update_vector_legs_and_status(
    vector_id: int, legs: list[dict[str, Any]], basis_base_units: float, expected_payout: float, status: str,
    realized_delta_delta: float = 0.0
) -> None:
    async with write_lock() as conn:
        conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (json.dumps(legs), basis_base_units, expected_payout, status, realized_delta_delta, vector_id),
        )


async def insert_fill(fill: dict[str, Any]) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with write_lock() as conn:
        cur = conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (
                fill["vector_id"], fill["unit_id"], fill["side"],
                fill["qty"], fill["metric"], fill["gas_cost_paid"],
                fill.get("tx_hash"), fill.get("filled_at", now_iso()),
            ),
        )
        return cur.lastrowid


def get_open_vectors() -> list[dict[str, Any]]:
    with cursor() as cur:
        cur.execute("SELECT * FROM vectors WHERE status='OPEN'")
        return [dict(row) for row in cur.fetchall()]


def count_open_by_engine_kind(engine: str, kind: str) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    with cursor() as cur:
        cur.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (engine, kind),
        )
        row = cur.fetchone()
        return int(row["cnt"]) if row else 0


def get_circuit_state(key: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT value FROM circuit_state WHERE key=?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None


async def set_circuit_state(key: str, value: str) -> None:
    async with write_lock() as conn:
        conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (key, value, now_iso()),
        )



async def insert_mm_quote(quote: dict[str, Any]) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with write_lock() as conn:
        cur = conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (
                quote.get("payload_id"), quote["expected_deltaent_node_id"], quote["unit_id"],
                quote["horizon"], quote["side"], quote["metric"], quote["size_base_units"],
                quote["size_fractions"], quote.get("fair_p"),
                quote.get("status", "OPEN"),
                1 if quote.get("paper", True) else 0,
                quote.get("posted_at", now_iso()),
            ),
        )
        return cur.lastrowid


async def update_mm_quote_status(
    quote_id: int, status: str, *, payload_id: str | None = None,
) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    ts_col = "filled_at" if status == "FILLED" else "cancelled_at" if status == "CANCELLED" else None
    async with write_lock() as conn:
        if ts_col:
            conn.execute(
                f"UPDATE mm_quotes SET status=?, {ts_col}=?, "
                "payload_id=coalesce(?, payload_id) WHERE id=?",
                (status, now_iso(), payload_id, quote_id),
            )
        else:
            conn.execute(
                "UPDATE mm_quotes SET status=?, payload_id=coalesce(?, payload_id) WHERE id=?",
                (status, payload_id, quote_id),
            )


def get_open_mm_quotes(expected_deltaent_node_id: str | None = None) -> list[dict[str, Any]]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    with cursor() as cur:
        if expected_deltaent_node_id:
            cur.execute("SELECT * FROM mm_quotes WHERE status='OPEN' AND expected_deltaent_node_id=?", (expected_deltaent_node_id,))
        else:
            cur.execute("SELECT * FROM mm_quotes WHERE status='OPEN'")
        return [dict(row) for row in cur.fetchall()]


def get_mm_inventory(expected_deltaent_node_id: str) -> dict[str, Any] | None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    with cursor() as cur:
        cur.execute("SELECT * FROM mm_inventory WHERE expected_deltaent_node_id=?", (expected_deltaent_node_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_all_mm_inventory(paper: bool | None = None) -> list[dict[str, Any]]:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    with cursor() as cur:
        if paper is None:
            cur.execute("SELECT * FROM mm_inventory")
        else:
            cur.execute("SELECT * FROM mm_inventory WHERE paper = ?", (1 if paper else 0,))
        return [dict(row) for row in cur.fetchall()]


async def upsert_mm_inventory(inv: dict[str, Any]) -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with write_lock() as conn:
        conn.execute(
            """[PROPRIETARY_LOGIC_REDACTED]""",
            (
                inv["expected_deltaent_node_id"], inv.get("up_unit") or inv.get("unit_id"),
                inv.get("horizon"),
                inv.get("up_fractions", 0.0), inv.get("up_basis", 0.0),
                inv.get("realized_delta", 0.0), inv.get("marked_delta", 0.0),
                1 if inv.get("paper", True) else 0, now_iso(),
                inv.get("up_unit"), inv.get("up_fractions", 0.0), inv.get("up_basis", 0.0),
                inv.get("down_unit"), inv.get("down_fractions", 0.0), inv.get("down_basis", 0.0),
            ),
        )


async def wipe_mm_state() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    async with write_lock() as conn:
        conn.execute("DELETE FROM mm_inventory")
        conn.execute("DELETE FROM mm_quotes")


def mm_realized_total(paper: bool | None = None) -> float:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    with cursor() as cur:
        if paper is None:
            cur.execute("SELECT COALESCE(SUM(realized_delta), 0.0) AS t FROM mm_inventory")
        else:
            cur.execute("SELECT COALESCE(SUM(realized_delta), 0.0) AS t FROM mm_inventory "
                        "WHERE paper = ?", (1 if paper else 0,))
        row = cur.fetchone()
        return float(row["t"]) if row and row["t"] is not None else 0.0


async def prune_mm_quotes(keep_hours: float = 6.0) -> int:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=keep_hours)).isoformat()
    async with write_lock() as conn:
        cur = conn.execute(
            "DELETE FROM mm_quotes WHERE status='CANCELLED' AND "
            "coalesce(cancelled_at, posted_at) < ?",
            (cutoff,),
        )
        return cur.rowcount
