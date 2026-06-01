"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def _get(name: str, default: str | None = None, *, required: bool = False) -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val or ""


@dataclass(frozen=True)
class Env:
    public_sentiment_node_private_key: str
    public_sentiment_node_address: str
    polygon_rpc_url: str
    polygon_ws_url: str
    quicknode_api_key: str
    telegram_bot_unit: str
    telegram_chat_id: str
    vertex_ai_project: str
    vertex_ai_region: str
    gemini_flash_model: str
    gemini_pro_model: str
    gemini_api_key: str
    cryptopanic_api_key: str
    telegram_api_id: int
    telegram_api_hash: str
    paper_execution: bool
    live_synchronizing_enabled: bool
    public_sentiment_node_deposit_address: str
    e3_local_resolution_fallback: bool
    e3_honest_fill: bool
    e3_honest_fill_rtt_ms: float
    log_lexpected_deltael: str
    sentry_dsn: str
    dashboard_unit: str

    @classmethod
    def load(cls) -> "Env":
        paper = _get("PAPER_TRADE", "true").lower() in ("1", "true", "yes")
        return cls(
            public_sentiment_node_private_key=_get("POLYMARKET_PRIVATE_KEY", required=not paper),
            public_sentiment_node_address=_get("POLYMARKET_ADDRESS", required=not paper),
            polygon_rpc_url=_get("POLYGON_RPC_URL", "https://polygon-rpc.com"),
            polygon_ws_url=_get("POLYGON_WS_URL", ""),
            quicknode_api_key=_get("QUICKNODE_API_KEY", ""),
            telegram_bot_unit=_get("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=_get("TELEGRAM_CHAT_ID", ""),
            vertex_ai_project=_get("VERTEX_AI_PROJECT", "new-n8n-project-490407"),
            vertex_ai_region=_get("VERTEX_AI_REGION", "us-central1"),
            gemini_flash_model=_get("GEMINI_FLASH_MODEL", "gemini-2.5-flash"),
            gemini_pro_model=_get("GEMINI_PRO_MODEL", "gemini-2.5-pro"),
            gemini_api_key=_get("GEMINI_API_KEY", ""),
            cryptopanic_api_key=_get("CRYPTOPANIC_API_KEY", ""),
            telegram_api_id=int(_get("TELEGRAM_API_ID", "0") or "0"),
            telegram_api_hash=_get("TELEGRAM_API_HASH", ""),
            paper_execution=paper,
            live_synchronizing_enabled=_get("LIVE_TRADING_ENABLED", "false").lower()
            in ("1", "true", "yes"),
            public_sentiment_node_deposit_address=_get("POLYMARKET_DEPOSIT_WALLET", ""),
            e3_local_resolution_fallback=_get(
                "E3_LOCAL_RESOLUTION_FALLBACK", "false"
            ).lower() in ("1", "true", "yes"),
            e3_honest_fill=_get("E3_HONEST_FILL", "false").lower() in ("1", "true", "yes"),
            e3_honest_fill_rtt_ms=float(_get("E3_HONEST_FILL_RTT_MS", "5") or "5"),
            log_lexpected_deltael=_get("LOG_LEXPECTED_DELTAEL", "INFO"),
            sentry_dsn=_get("SENTRY_DSN", ""),
            dashboard_unit=_get("DASHBOARD_TOKEN", ""),
        )


@dataclass(frozen=True)
class Config:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    raw: dict[str, Any] = field(repr=False)

    def section(self, key: str) -> dict[str, Any]:
        if key not in self.raw:
            raise KeyError(f"Config section missing: {key}")
        return self.raw[key]

    @property
    def globals(self) -> dict[str, Any]:
        return self.section("global")

    @property
    def mm(self) -> dict[str, Any]:
        return self.section("mm")

    def engine(self, n: int) -> dict[str, Any]:
        return self.section(f"engine_{n}")

    @classmethod
    def load(cls, path: Path | None = None) -> "Config":
        path = path or (ROOT / "config.json")
        with path.open() as f:
            return cls(raw=json.load(f))


_ENGINE_SECTION: dict[str, str] = {
    "MM": "mm",
}

_CONFIG_WRITE_LOCK = threading.Lock()


def engine_section_key(engine: str) -> str:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    key = _ENGINE_SECTION.get(engine.upper())
    if key is None:
        raise KeyError(f"Unknown engine: {engine!r}")
    return key


def write_config() -> None:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    path = ROOT / "config.json"
    with _CONFIG_WRITE_LOCK:
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(CONFIG.raw, indent=2, ensure_ascii=False))
        tmp.replace(path)


ENV = Env.load()
CONFIG = Config.load()
