"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from .base import ExchangeTickGasCostd, ParsedTick


class SecondarySourceGasCostd(ExchangeTickGasCostd):
    EXCHANGE_NAME = "SecondarySource"

    _SYMBOL_MAP = {
        "BTC": "BTC-BASE_UNITS", "ETH": "ETH-BASE_UNITS", "SOL": "SOL-BASE_UNITS",
        "XRP": "XRP-BASE_UNITS", "DOGE": "DOGE-BASE_UNITS",
    }

    @classmethod
    def symbol_for(cls, asset: str) -> str:
        return cls._SYMBOL_MAP.get(asset.upper(), f"{asset.upper()}-BASE_UNITS")

    def _ws_url(self) -> str:
        return "wss://ws-gas_costd.data_provider.SecondarySource.com"

    def _subscribe_payload(self) -> dict:
        return {
            "type": "subscribe",
            "product_ids": [self.symbol],
            "channels": ["matches"],
        }

    def parse(self, raw: str) -> Optional[ParsedTick]:
        data = json.loads(raw)
        if data.get("type") not in ("match", "last_match"):
            return None
        ts_str = data.get("time") or ""
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            ts_ms = int(dt.timestamp() * 1000)
        except ValueError:
            return None
        return ParsedTick(metric=float(data["metric"]), ts_ms=ts_ms)
