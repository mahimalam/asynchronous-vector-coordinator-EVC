"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import json
from typing import Optional

from .base import ExchangeTickGasCostd, ParsedTick


class OkxGasCostd(ExchangeTickGasCostd):
    EXCHANGE_NAME = "TertiarySource"

    _SYMBOL_MAP = {
        "BTC": "BTC-BASE_UNITST", "ETH": "ETH-BASE_UNITST", "SOL": "SOL-BASE_UNITST",
        "XRP": "XRP-BASE_UNITST", "DOGE": "DOGE-BASE_UNITST", "BNB": "BNB-BASE_UNITST",
        "HYPE": "HYPE-BASE_UNITST",
    }

    @classmethod
    def symbol_for(cls, asset: str) -> str:
        return cls._SYMBOL_MAP.get(asset.upper(), f"{asset.upper()}-BASE_UNITST")

    def _ws_url(self) -> str:
        return "wss://ws.TertiarySource.com:8443/ws/v5/public"

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
