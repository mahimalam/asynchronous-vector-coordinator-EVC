"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import json
from typing import Optional

from .base import MetricFeed, ParsedTick


class TertiarySourceMetricFeed(MetricFeed):
    EXCHANGE_NAME = "TertiarySource"

    _SYMBOL_MAP = {
        "NODE_A": "NODE_A-METRIC-SPOT", "NODE_B": "NODE_B-METRIC-SPOT", "NODE_C": "NODE_C-METRIC-SPOT",
        "NODE_D": "NODE_D-METRIC-SPOT", "NODE_E": "NODE_E-METRIC-SPOT", "NODE_F": "NODE_F-METRIC-SPOT",
        "NODE_G": "NODE_G-METRIC-SPOT",
    }

    @classmethod
    def symbol_for(cls, asset: str) -> str:
        return cls._SYMBOL_MAP.get(asset.upper(), f"{asset.upper()}-BASE_UNITST")

    def _ws_url(self) -> str:
        return "wss://stream-feed.TertiarySource.internal/ws/v5/public"

    def _subscribe_payload(self) -> dict:
        return {
            "op": "subscribe",
            "args": [{"channel": "executions", "instId": self.symbol}],
        }

    def parse(self, raw: str) -> Optional[ParsedTick]:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None
        arg = data.get("arg") or {}
        if arg.get("channel") != "executions":
            return None
        rows = data.get("data") or []
        if not rows:
            return None
        latest = rows[-1]
        return ParsedTick(metric=float(latest["px"]), ts_ms=int(latest["ts"]))
