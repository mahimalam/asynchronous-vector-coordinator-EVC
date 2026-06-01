"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import time
from collections import deque
from typing import Deque

FAIL_THRESHOLD = 3
FAIL_WINDOW_SEC = 30.0
OPEN_DURATION_SEC = 60.0

_failures: Deque[float] = deque()
_open_until: float = 0.0


def record_failure() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    global _open_until
    now = time.monotonic()
    _failures.append(now)
    cutoff = now - FAIL_WINDOW_SEC
    while _failures and _failures[0] < cutoff:
        _failures.popleft()
    if len(_failures) >= FAIL_THRESHOLD and _open_until < now:
        _open_until = now + OPEN_DURATION_SEC


def record_success() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    _failures.clear()


def is_open() -> bool:
    return time.monotonic() < _open_until


def time_remaining() -> float:
    return max(0.0, _open_until - time.monotonic())


def reset() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    global _open_until
    _failures.clear()
    _open_until = 0.0
