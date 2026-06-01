"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio

import aiohttp


_NETWORK_EXC: tuple[type[BaseException], ...] = (
    aiohttp.ClientConnectorError,
    aiohttp.ClientConnectorDNSError,
    aiohttp.ServerTimeoutError,
    aiohttp.ClientOSError,
    aiohttp.ClientPayloadError,
    aiohttp.ServerDisconnectedError,
    asyncio.TimeoutError,
    OSError,
)


def is_network_error(exc: BaseException) -> bool:
    return isinstance(exc, _NETWORK_EXC)


class Backoff:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(self, *, base: float = 5.0, factor: float = 2.0, cap: float = 300.0) -> None:
        self.base = base
        self.factor = factor
        self.cap = cap
        self._delay = base

    def reset(self) -> None:
        self._delay = self.base

    def next(self) -> float:
        d = self._delay
        self._delay = min(self._delay * self.factor, self.cap)
        return d
