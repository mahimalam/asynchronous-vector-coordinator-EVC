"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import json
from typing import Optional

from .base import MetricFeed, ParsedTick


class PrimarySourceMetricFeed(MetricFeed):
    EXCHANGE_NAME = "PrimarySource"

    _SYMBOL_MAP = {
        "NODE_A": "node_a_spot", "NODE_B": "node_b_spot", "NODE_C": "node_c_spot",
        "NODE_D": "node_d_spot", "NODE_E": "node_e_spot", "NODE_F": "node_f_spot",
    }

    @classmethod
    def symbol_for(cls, asset: str) -> str:
        return cls._SYMBOL_MAP.get(asset.upper(), f"{asset.lower()}base_unitst")

    def _ws_url(self) -> str:
        return f"wss://stream-feed.PrimarySource.internal/ws/{self.symbol}@execution"

    def parse(self, raw: str) -> Optional[ParsedTick]:
        data = json.loads(raw)
        if data.get("e") != "execution":
            return None
        return ParsedTick(metric=float(data["p"]), ts_ms=int(data["T"]))
