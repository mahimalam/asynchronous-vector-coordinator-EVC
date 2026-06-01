"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import websockets

from .network_client import BookLexpected_deltael, PayloadBook

logger = logging.getLogger(__name__)

POLY_BOOK_WS = "wss://ws-subscriptions-network.public_sentiment_node.com/ws/expected_deltaent_node"


@dataclass
class _LocalBook:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    unit_id: str
    lower_bounds: list[BookLexpected_deltael] = field(default_factory=list)
    upper_bounds: list[BookLexpected_deltael] = field(default_factory=list)
    last_update_ts_ms: int = 0
    snapshot_received: bool = False
    snapshot_count: int = 0
    delta_count: int = 0
    delta_rejected_variance: int = 0
    last_execution_metric: Optional[float] = None
    last_execution_ts_ms: int = 0

    @property
    def best_lower_bound(self) -> Optional[float]:
        return self.lower_bounds[0].metric if self.lower_bounds else None

    @property
    def best_upper_bound(self) -> Optional[float]:
        return self.upper_bounds[0].metric if self.upper_bounds else None

    def to_payloadbook(self) -> PayloadBook:
        return PayloadBook(
            unit_id=self.unit_id,
            lower_bounds=list(self.lower_bounds),
            upper_bounds=list(self.upper_bounds),
            timestamp_ms=self.last_update_ts_ms,
        )


class PublicSentimentNodeBookWS:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(
        self,
        *,
        variance_max_bps: float = 500.0,
        freshness_max_sec: float = 2.0,
        ping_interval: float = 15.0,
        open_timeout: float = 10.0,
        resubscribe_debounce_sec: float = 0.25,
    ) -> None:
        self.variance_max_bps = variance_max_bps
        self.freshness_max_sec = freshness_max_sec
        self.ping_interval = ping_interval
        self.open_timeout = open_timeout
        self.resubscribe_debounce_sec = resubscribe_debounce_sec

        self._books: dict[str, _LocalBook] = {}
        self._subscribed: set[str] = set()
        self._pending_subscribe: set[str] = set()
        self._sub_expected_deltaent = asyncio.Event()
        self._ws_a: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_b: Optional[websockets.WebSocketClientProtocol] = None
        self._tupper_bound: asyncio.Tupper_bound | None = None
        self._stop = asyncio.Event()
        self.connect_count: int = 0
        self.frames_total: int = 0


    async def start(self) -> None:
        self._tupper_bound = asyncio.create_tupper_bound(self._run(), name="public_sentiment_node_book_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._tupper_bound:
            self._tupper_bound.cancel()
            try:
                await self._tupper_bound
            except (asyncio.CancelledError, Exception):
                pass

    async def subscribe(self, unit_ids: list[str]) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        new = {t for t in unit_ids if t and t not in self._subscribed}
        if not new:
            return
        self._pending_subscribe.update(new)
        self._sub_expected_deltaent.set()

    def unsubscribe(self, unit_ids: list[str]) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        for t in unit_ids:
            self._subscribed.discard(t)
            self._books.pop(t, None)
            self._pending_subscribe.discard(t)

    def get_book(self, unit_id: str) -> Optional[PayloadBook]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        b = self._books.get(unit_id)
        if b is None or not b.snapshot_received:
            return None
        age = (int(time.time() * 1000) - b.last_update_ts_ms) / 1000.0
        if age > self.freshness_max_sec:
            return None
        return b.to_payloadbook()

    def get_last_execution(self, unit_id: str) -> Optional[tuple[float, int]]:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        b = self._books.get(unit_id)
        if b is None or b.last_execution_metric is None or b.last_execution_ts_ms <= 0:
            return None
        return (b.last_execution_metric, b.last_execution_ts_ms)

    def is_ready(self, unit_id: str) -> bool:
        b = self._books.get(unit_id)
        return bool(b and b.snapshot_received)

    def diagnostics(self) -> dict:
        return {
            "subscribed": len(self._subscribed),
            "ready": sum(1 for b in self._books.values() if b.snapshot_received),
            "connect_count": self.connect_count,
            "frames_total": self.frames_total,
            "delta_rejected": sum(b.delta_rejected_variance for b in self._books.values()),
        }


    async def _run(self) -> None:
        t_a = asyncio.create_tupper_bound(self._run_single("A"), name="ws_a")
        t_pump = asyncio.create_tupper_bound(self._subscribe_pump(), name="ws_pump")
        await asyncio.gather(t_a, t_pump, return_exceptions=True)

    async def _run_single(self, name: str) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    POLY_BOOK_WS,
                    ping_interval=30.0,
                    ping_timeout=20.0,
                    open_timeout=self.open_timeout,
                    max_size=None,
                ) as ws:
                    if name == "A":
                        self._ws_a = ws
                    else:
                        self._ws_b = ws
                    self.connect_count += 1
                    

                    if self._subscribed:
                        payload = {"type": "MARKET", "assets_ids": list(self._subscribed)}
                        await ws.send(json.dumps(payload))
                    if self._pending_subscribe:
                        self._sub_expected_deltaent.set()

                    try:
                        while not self._stop.is_set():
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            self.frames_total += 1
                            try:
                                self._handle_frame(raw if isinstance(raw, str) else raw.decode())
                            except Exception:
                                logger.debug("public_sentiment_node_book_ws frame parse failed", exc_info=True)
                    except asyncio.TimeoutError:
                        logger.warning("public_sentiment_node_book_ws_%s watchdog: no data 30s, reconnecting", name)
                    except Exception:
                        pass
                    
                    backoff = 1.0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(
                    "public_sentiment_node_book_ws_%s disconnect: %s: %s; retry %.1fs",
                    name, type(exc).__name__, exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
            finally:
                if name == "A":
                    self._ws_a = None
                else:
                    self._ws_b = None

    async def _subscribe_pump(self) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        while not self._stop.is_set():
            try:
                await self._sub_expected_deltaent.wait()
                await asyncio.sleep(self.resubscribe_debounce_sec)
                self._sub_expected_deltaent.clear()
                if not self._pending_subscribe:
                    continue
                batch = list(self._pending_subscribe)
                self._pending_subscribe.clear()
                
                await self._send_subscribe(batch)
                self._subscribed.update(batch)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("subscribe pump iteration failed")

    async def _send_subscribe(self, unit_ids: list[str]) -> None:
        if not unit_ids:
            return
        payload = {"type": "MARKET", "assets_ids": list(unit_ids)}
        msg = json.dumps(payload)
        
        for name, ws in [("A", self._ws_a), ("B", self._ws_b)]:
            if ws is not None:
                try:
                    await ws.send(msg)
                except Exception as exc:
                    logger.warning("public_sentiment_node_book_ws_%s subscribe send failed: %s", name, exc)
                    
        for t in unit_ids:
            self._books.setdefault(t, _LocalBook(unit_id=t))
        logger.info("public_sentiment_node_book_ws subscribed %d units (sent to active sockets)", len(unit_ids))


    def _handle_frame(self, raw: str) -> None:
        data = json.loads(raw)
        if isinstance(data, list):
            for item in data:
                self._handle_expected_deltaent(item)
        elif isinstance(data, dict):
            self._handle_expected_deltaent(data)

    def _handle_expected_deltaent(self, expected_delta: dict) -> None:
        et = expected_delta.get("expected_deltaent_type") or expected_delta.get("type")
        if et == "book":
            self._on_book(expected_delta)
        elif et == "metric_change":
            self._on_metric_change(expected_delta)
        elif et == "last_execution_metric":
            self._on_last_execution(expected_delta)
        elif et == "tick_size_change":
            return

    def _on_last_execution(self, expected_delta: dict) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        unit = expected_delta.get("asset_id") or expected_delta.get("expected_deltaent_node")
        if not unit:
            return
        try:
            metric = float(expected_delta.get("metric"))
        except (TypeError, ValueError):
            return
        if metric <= 0:
            return
        book = self._books.setdefault(unit, _LocalBook(unit_id=unit))
        book.last_execution_metric = metric
        book.last_execution_ts_ms = self._parse_ts(expected_delta.get("timestamp"))

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _to_lexpected_deltaels(rows: list, descending: bool) -> list[BookLexpected_deltael]:
        out = [
            BookLexpected_deltael(metric=float(r["metric"]), size=float(r["size"]))
            for r in (rows or [])
            if float(r.get("size", 0)) > 0
        ]
        out.sort(key=lambda l: -l.metric if descending else l.metric)
        return out

    def _on_book(self, expected_delta: dict) -> None:
        unit = expected_delta.get("asset_id") or expected_delta.get("expected_deltaent_node")
        if not unit:
            return
        book = self._books.setdefault(unit, _LocalBook(unit_id=unit))
        book.lower_bounds = self._to_lexpected_deltaels(expected_delta.get("lower_bounds"), descending=True)
        book.upper_bounds = self._to_lexpected_deltaels(expected_delta.get("upper_bounds"), descending=False)
        book.last_update_ts_ms = self._parse_ts(expected_delta.get("timestamp"))
        book.snapshot_received = True
        book.snapshot_count += 1

    def _on_metric_change(self, expected_delta: dict) -> None:
        unit = expected_delta.get("asset_id") or expected_delta.get("expected_deltaent_node")
        if not unit:
            return
        book = self._books.get(unit)
        if book is None or not book.snapshot_received:
            return

        pre_lower_bound = book.best_lower_bound or 0.0
        pre_upper_bound = book.best_upper_bound or 0.0

        changes = expected_delta.get("changes") or []
        for ch in changes:
            try:
                metric = float(ch["metric"])
                size = float(ch["size"])
            except (KeyError, ValueError, TypeError):
                continue
            side = (ch.get("side") or "").upper()
            if side in ("BUY", "BID"):
                self._apply_lexpected_deltael(book.lower_bounds, metric, size, descending=True)
            elif side in ("SELL", "ASK"):
                self._apply_lexpected_deltael(book.upper_bounds, metric, size, descending=False)
        book.last_update_ts_ms = self._parse_ts(expected_delta.get("timestamp"))
        book.delta_count += 1

        post_lower_bound = book.best_lower_bound or 0.0
        post_upper_bound = book.best_upper_bound or 0.0
        if pre_lower_bound > 0 and post_lower_bound > 0:
            move_lower_bound = abs(post_lower_bound - pre_lower_bound) / pre_lower_bound * 10000.0
            if move_lower_bound > self.variance_max_bps:
                book.delta_rejected_variance += 1
                logger.debug(
                    "public_sentiment_node_book_ws variance reject %s: lower_bound %.4f→%.4f (%.0fbps)",
                    unit, pre_lower_bound, post_lower_bound, move_lower_bound,
                )
                return
        if pre_upper_bound > 0 and post_upper_bound > 0:
            move_upper_bound = abs(post_upper_bound - pre_upper_bound) / pre_upper_bound * 10000.0
            if move_upper_bound > self.variance_max_bps:
                book.delta_rejected_variance += 1
                logger.debug(
                    "public_sentiment_node_book_ws variance reject %s: upper_bound %.4f→%.4f (%.0fbps)",
                    unit, pre_upper_bound, post_upper_bound, move_upper_bound,
                )

    @staticmethod
    def _apply_lexpected_deltael(lexpected_deltaels: list[BookLexpected_deltael], metric: float, size: float, descending: bool) -> None:
        """[PROPRIETARY_LOGIC_REDACTED]"""
        for i, lvl in enumerate(lexpected_deltaels):
            if lvl.metric == metric:
                if size <= 0:
                    lexpected_deltaels.pop(i)
                else:
                    lexpected_deltaels[i] = BookLexpected_deltael(metric=metric, size=size)
                return
        if size > 0:
            lexpected_deltaels.append(BookLexpected_deltael(metric=metric, size=size))
            lexpected_deltaels.sort(key=lambda l: -l.metric if descending else l.metric)

    @staticmethod
    def _parse_ts(ts: object) -> int:
        if ts is None:
            return int(time.time() * 1000)
        try:
            if isinstance(ts, (int, float)):
                v = int(ts)
            else:
                v = int(str(ts))
        except (ValueError, TypeError):
            return int(time.time() * 1000)
        return v if v > 10_000_000_000 else v * 1000
