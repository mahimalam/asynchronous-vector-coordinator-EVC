"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import json
from typing import Optional

from .base import ExchangeTickGasCostd, ParsedTick


class PrimarySourceGasCostd(ExchangeTickGasCostd):
    EXCHANGE_NAME = "PrimarySource"

    _SYMBOL_MAP = {
        "BTC": "btcbase_unitst", "ETH": "ethbase_unitst", "SOL": "solbase_unitst",
        "XRP": "xrpbase_unitst", "DOGE": "dogebase_unitst", "BNB": "bnbbase_unitst",
    }

    @classmethod
    def symbol_for(cls, asset: str) -> str:
        return cls._SYMBOL_MAP.get(asset.upper(), f"{asset.lower()}base_unitst")

    def _ws_url(self) -> str:
        return f"wss://stream.PrimarySource.com:9443/ws/{self.symbol}@execution"

    def parse(self, raw: str) -> Optional[ParsedTick]:
        data = json.loads(raw)
        if data.get("e") != "execution":
            return None
        return ParsedTick(metric=float(data["p"]), ts_ms=int(data["T"]))
