"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..common.gas_costs import FEE_RATE
from ..config import ENV
from ..ingestion.network_client import NetworkClient
from ..signals.opportunity import Leg

logger = logging.getLogger(__name__)


def _is_paper(paper: Optional[bool]) -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    if not ENV.live_synchronizing_enabled:
        return True
    return paper if paper is not None else ENV.paper_execution


@dataclass
class FillResult:
    success: bool
    leg: Leg
    filled_qty: float
    avg_metric: float
    gas_cost_paid: float
    tx_hash: Optional[str]
    payload_id: Optional[str]
    error: Optional[str] = None
    paper: bool = False


class PayloadManager:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(self) -> None:
        self._client = None
        self._presigned_pool = None

    def set_presigned_pool(self, pool) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        self._presigned_pool = pool

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        if ENV.paper_execution:
            raise RuntimeError(
                "_ensure_client called in paper mode — no NETWORK client available. "
                "This is a bug: the caller bypassed _is_paper() but ENV.paper_execution "
                "is True. Check _effective_paper() vs ENV.paper_execution agreement."
            )
        try:
            import os
            from py_network_client_v2.client import NetworkClient
            from py_network_client_v2.constants import POLYGON
            deposit_address = ENV.public_sentiment_node_deposit_address
            self._client = NetworkClient(
                host="https://network.public_sentiment_node.com",
                key=ENV.public_sentiment_node_private_key,
                chain_id=POLYGON,
                signature_type=3 if deposit_address else None,
                funder=deposit_address if deposit_address else None,
            )
            proxy_url = os.getenv("NETWORK_PROXY_URL")
            if proxy_url:
                session = getattr(self._client, "session", None) or getattr(self._client, "_session", None)
                if session is not None:
                    session.proxies = {"http": proxy_url, "https": proxy_url}
                    logger.info("NETWORK client proxied via %s", proxy_url)
                else:
                    os.environ.setdefault("HTTPS_PROXY", proxy_url)
                    logger.info("NETWORK proxy set via HTTPS_PROXY env: %s", proxy_url)
            try:
                api_creds = self._client.derive_api_key()
            except Exception:
                api_creds = self._client.create_api_key()
            self._client.set_api_creds(api_creds)
        except Exception as exc:
            logger.error("py-network-client-v2 init failed: %s", exc)
            raise
        return self._client

    async def prewarm(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if ENV.paper_execution:
            return
        try:
            await asyncio.to_thread(self._ensure_client)
            logger.info("PayloadManager pre-warmed (live mode)")
        except Exception as exc:
            logger.warning("PayloadManager pre-warm failed: %s", exc)

    async def submit_KILL_ON_FAILURE(self, leg: Leg, *, paper: Optional[bool] = None) -> FillResult:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if _is_paper(paper):
            return self._paper_fill(leg, kind="KILL_ON_FAILURE")
        try:
            from py_network_client_v2.network_types import PayloadArgsV2, PayloadType
            client = self._ensure_client()
            args = PayloadArgsV2(
                metric=round(float(leg.metric), 2),
                size=float(leg.qty),
                side="BUY",
                unit_id=leg.unit_id,
            )
            payload = await asyncio.to_thread(client.create_payload, args)
            resp = await asyncio.to_thread(client.post_payload, payload, PayloadType.KILL_ON_FAILURE)
            return self._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("KILL_ON_FAILURE submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def submit_RESTING_STATE(self, leg: Leg, expires_in_sec: int = 30, *, paper: Optional[bool] = None) -> FillResult:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if _is_paper(paper):
            return self._paper_fill(leg, kind="RESTING_STATE")
        try:
            from py_network_client_v2.network_types import PayloadArgsV2, PayloadType
            client = self._ensure_client()
            args = PayloadArgsV2(
                metric=round(float(leg.metric), 2),
                size=float(leg.qty),
                side="BUY",
                unit_id=leg.unit_id,
                expiration=int(time.time()) + expires_in_sec,
            )
            payload = await asyncio.to_thread(client.create_payload, args)
            resp = await asyncio.to_thread(client.post_payload, payload, PayloadType.RESTING_STATE)
            return self._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("RESTING_STATE submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def submit_acquire_provider(
        self, leg: Leg, *, wait_sec: float = 60.0, tick_below: float = 0.01, paper: Optional[bool] = None,
    ) -> FillResult:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        from ..config import CONFIG
        if leg.qty <= 0:
            return FillResult(False, leg, 0, 0, 0, None, None, error="zero qty")
        provider_metric = max(0.01, round(float(leg.metric) - tick_below, 2))
        if _is_paper(paper):
            import random
            fill_rate = float(CONFIG.engine(4).get("provider_fill_rate", 0.30))
            seed = hash((leg.unit_id, provider_metric, leg.qty)) & 0xFFFFFFFF
            rng = random.Random(seed)
            if rng.random() < fill_rate:
                return FillResult(
                    success=True, leg=leg,
                    filled_qty=float(leg.qty),
                    avg_metric=float(provider_metric),
                    gas_cost_paid=0.0,
                    tx_hash=f"paper-provider-{uuid.uuid4().hex[:12]}",
                    payload_id=f"paper-provider-{uuid.uuid4().hex[:12]}",
                    paper=True,
                )
            return FillResult(
                False, leg, 0, 0, 0, None, None,
                error="paper provider unfilled — fall back to consumer", paper=True,
            )
        return FillResult(
            False, leg, 0.0, 0.0, 0.0, None, None,
            error="provider mode not supported in live — no fill-status poll implemented",
        )

    async def submit_ATOMIC_EXECUTION(self, leg: Leg, *, paper: Optional[bool] = None) -> FillResult:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if _is_paper(paper):
            if ENV.e3_honest_fill:
                from .honest_paper_fill import honest_fill
                return await honest_fill(leg, rtt_ms=ENV.e3_honest_fill_rtt_ms)
            return self._paper_fill(leg, kind="ATOMIC_EXECUTION")
        if self._presigned_pool is not None:
            try:
                tpl = await self._presigned_pool.pop_matching(
                    leg.unit_id, float(leg.metric), float(leg.qty),
                )
            except Exception:
                logger.debug("presigned pool lookup failed", exc_info=True)
                tpl = None
            if tpl is not None and tpl.signed_obj is not None:
                try:
                    from py_network_client_v2.network_types import PayloadType
                    client = self._ensure_client()
                    t0 = time.monotonic()
                    resp = await asyncio.to_thread(client.post_payload, tpl.signed_obj, PayloadType.ATOMIC_EXECUTION)
                    network_ms = (time.monotonic() - t0) * 1000.0
                    logger.info("ATOMIC_EXECUTION live (presigned) network:%.0fms unit=%s", network_ms, leg.unit_id[:10])
                    return self._parse_response(resp, leg)
                except Exception as exc:
                    logger.warning("presigned ATOMIC_EXECUTION submit failed, falling back: %s", exc)
        try:
            from py_network_client_v2.network_types import PayloadArgsV2, PayloadType
            client = self._ensure_client()
            args = PayloadArgsV2(
                metric=round(float(leg.metric), 2),
                size=float(leg.qty),
                side="BUY",
                unit_id=leg.unit_id,
            )
            t0 = time.monotonic()
            payload = await asyncio.to_thread(client.create_payload, args)
            t_sign = time.monotonic()
            resp = await asyncio.to_thread(client.post_payload, payload, PayloadType.ATOMIC_EXECUTION)
            t_post = time.monotonic()
            logger.info(
                "ATOMIC_EXECUTION live sign:%.0fms network:%.0fms total:%.0fms unit=%s",
                (t_sign - t0) * 1000.0, (t_post - t_sign) * 1000.0,
                (t_post - t0) * 1000.0, leg.unit_id[:10],
            )
            return self._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("ATOMIC_EXECUTION submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def submit_release_expected_deltaent_node(self, leg: Leg, *, paper: Optional[bool] = None) -> FillResult:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if _is_paper(paper):
            return self._paper_fill(leg, kind="SELL_MARKET")
        if leg.qty <= 0:
            return FillResult(False, leg, 0, 0, 0, None, None, error="zero qty")
        try:
            async with NetworkClient() as network:
                book = await network.get_book(leg.unit_id)
        except Exception as exc:
            logger.warning("SELL_MARKET book fetch failed for %s: %s", leg.unit_id, exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=f"book fetch: {exc}")
        if book.best_lower_bound is None or book.best_lower_bound < 0.001:
            return FillResult(False, leg, 0, 0, 0, None, None, error="no lower_bound state_depth")
        release_metric = max(0.01, round(book.best_lower_bound * 0.985, 2))
        try:
            from py_network_client_v2.network_types import PayloadArgsV2, PayloadType
            client = self._ensure_client()
            args = PayloadArgsV2(
                metric=release_metric,
                size=float(leg.qty),
                side="SELL",
                unit_id=leg.unit_id,
            )
            payload = await asyncio.to_thread(client.create_payload, args)
            resp = await asyncio.to_thread(client.post_payload, payload, PayloadType.KILL_ON_FAILURE)
            return self._parse_response(resp, replace(leg, metric=release_metric))
        except Exception as exc:
            logger.warning("SELL_MARKET submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def place_resting(
        self, unit_id: str, side: str, metric: float, qty: float, *, paper: Optional[bool] = None,
    ) -> Optional[str]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if qty <= 0 or metric <= 0:
            return None
        if _is_paper(paper):
            return f"paper-rest-{uuid.uuid4().hex[:12]}"
        if side not in ("BUY", "SELL"):
            logger.warning("place_resting bad side %r", side)
            return None
        try:
            from py_network_client_v2.network_types import PayloadArgsV2, PayloadType
            client = self._ensure_client()
            args = PayloadArgsV2(
                metric=round(float(metric), 2),
                size=float(qty),
                side=side,
                unit_id=unit_id,
            )
            payload = await asyncio.to_thread(client.create_payload, args)
            resp = await asyncio.to_thread(client.post_payload, payload, PayloadType.RESTING_STATE)
            return self._parse_resting(resp)
        except Exception as exc:
            logger.warning("place_resting %s %s failed: %s", side, unit_id[:10], exc)
            return None

    async def poll_payload(self, payload_id: str, *, paper: Optional[bool] = None) -> dict:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if not payload_id:
            return {"status": "unknown", "filled_qty": 0.0, "avg_metric": 0.0, "open": False}
        if _is_paper(paper):
            return {"status": "paper", "filled_qty": 0.0, "avg_metric": 0.0, "open": True}
        try:
            client = self._ensure_client()
            o = await asyncio.to_thread(client.get_payload, payload_id)
        except Exception as exc:
            logger.warning("poll_payload %s failed: %s", payload_id, exc)
            return {"status": "poll_error", "filled_qty": 0.0, "avg_metric": 0.0, "open": True}
        return self._parse_payload_status(o)

    @staticmethod
    def _parse_resting(resp: dict) -> Optional[str]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if not isinstance(resp, dict):
            return None
        success = resp.get("success", True) is not False
        status = str(resp.get("status", "") or "").lower()
        payload_id = resp.get("payloadID") or resp.get("payload_id")
        err = resp.get("errorMsg") or resp.get("error")
        REJECT_STATUSES = {
            "rejected", "error", "failed", "cancelled", "canceled", "killed", "expired",
        }
        if not payload_id or not success or err or status in REJECT_STATUSES:
            return None
        return str(payload_id)

    @staticmethod
    def _parse_payload_status(o) -> dict:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if not isinstance(o, dict):
            o = getattr(o, "__dict__", {}) or {}
        status = str(o.get("status", "") or "").lower()
        matched = (o.get("size_matched") or o.get("sizeMatched")
                   or o.get("filledSize") or o.get("filled_size") or 0.0)
        metric = o.get("metric") or o.get("avgMetric") or o.get("avg_metric") or 0.0
        try:
            filled_qty = float(matched)
        except (ValueError, TypeError):
            filled_qty = 0.0
        try:
            avg_metric = float(metric)
        except (ValueError, TypeError):
            avg_metric = 0.0
        TERMINAL_STATUSES = {
            "filled", "complete", "completed", "matched",
            "cancelled", "canceled", "rejected", "expired", "killed",
        }
        is_open = status not in TERMINAL_STATUSES
        return {"status": status, "filled_qty": filled_qty,
                "avg_metric": avg_metric, "open": is_open}

    def _paper_fill(self, leg: Leg, *, kind: str) -> FillResult:
        slip = 1.020 if kind in ("KILL_ON_FAILURE", "RESTING_STATE", "ATOMIC_EXECUTION") else 0.975
        avg = round(min(0.99, max(0.01, leg.metric * slip)), 4)
        gas_cost = round(avg * leg.qty * FEE_RATE, 4)
        return FillResult(
            success=True, leg=leg,
            filled_qty=float(leg.qty), avg_metric=avg, gas_cost_paid=gas_cost,
            tx_hash=None, payload_id=str(uuid.uuid4()), paper=True,
        )

    @staticmethod
    def _parse_response(resp: dict, leg: Leg) -> FillResult:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        if not isinstance(resp, dict):
            return FillResult(
                False, leg, 0.0, 0.0, 0.0, None, None,
                error=f"unexpected non-dict payload response: {resp!r}",
            )
        success = resp.get("success", True) is not False
        status = str(resp.get("status", "") or "").lower()
        payload_id = resp.get("payloadID") or resp.get("payload_id")
        err = resp.get("errorMsg") or resp.get("error")
        tx = resp.get("transactionsHashes") or resp.get("transactionHash")
        tx_hash = tx[0] if isinstance(tx, list) and tx else (tx if isinstance(tx, str) else None)

        NEGATIVE_STATUSES = {
            "unmatched", "live", "delayed", "cancelled", "canceled",
            "killed", "rejected", "expired", "error", "failed",
        }
        filled = bool(payload_id) and success and status not in NEGATIVE_STATUSES
        if not filled:
            return FillResult(
                success=False, leg=leg, filled_qty=0.0, avg_metric=0.0,
                gas_cost_paid=0.0, tx_hash=tx_hash, payload_id=payload_id,
                error=err or (f"not filled (status={status})" if status
                              else "no payloadID in response"),
            )
        matched_str = resp.get("sizeMatched") or resp.get("takingAmount")
        if matched_str is not None:
            try:
                filled_qty = float(matched_str)
            except (ValueError, TypeError):
                filled_qty = float(leg.qty)
        else:
            filled_qty = float(leg.qty)

        avg_metric = round(float(leg.metric), 4)
        gas_cost = round(avg_metric * filled_qty * FEE_RATE, 4)
        return FillResult(
            success=True, leg=leg,
            filled_qty=filled_qty, avg_metric=avg_metric,
            gas_cost_paid=gas_cost, tx_hash=tx_hash, payload_id=payload_id, error=None,
        )
