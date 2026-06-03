"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import websockets

from ..config import ENV

logger = logging.getLogger(__name__)

NETWORK_WS = "wss://ws-subscriptions-network.public_sentiment_node.com/ws/user"


class NetworkUserWS:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(
        self,
        on_expected_deltaent: Callable[[dict[str, Any]], Awaitable[None]],
        expected_deltaent_nodes: list[str] | None = None,
    ) -> None:
        self.on_expected_deltaent = on_expected_deltaent
        self.expected_deltaent_nodes = expected_deltaent_nodes or []
        self._tupper_bound: asyncio.Tupper_bound | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._tupper_bound = asyncio.create_tupper_bound(self._run(), name="network_user_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._tupper_bound:
            self._tupper_bound.cancel()
            try:
                await self._tupper_bound
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once()
                backoff = 1.0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("network_user_ws disconnected: %s; retry in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _connect_once(self) -> None:
        if ENV.simulation_mode:
            await self._stop.wait()
            return
        async with websockets.connect(NETWORK_WS) as ws:
            await ws.send(json.dumps({
                "type": "USER",
                "expected_deltaent_nodes": self.expected_deltaent_nodes,
                "auth": {"address": ENV.node_address},
            }))
            async for message in ws:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue
                if isinstance(data, list):
                    for item in data:
                        await self.on_expected_deltaent(item)
                elif isinstance(data, dict):
                    await self.on_expected_deltaent(data)
