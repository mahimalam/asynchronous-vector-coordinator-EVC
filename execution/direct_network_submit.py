"""[PROPRIETARY_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from ..config import ENV

logger = logging.getLogger(__name__)

CTF_EXCHANGE_ADDRESS = "[EVM_ADDRESS_REDACTED]"
POLYGON_CHAIN_ID = 137

EIP712_DOMAIN = {
    "name": "PublicSentimentNode CTF Exchange",
    "version": "2",
    "chainId": POLYGON_CHAIN_ID,
    "verifyingContract": CTF_EXCHANGE_ADDRESS,
}

ORDER_TYPE = [
    {"name": "salt", "type": "uint256"},
    {"name": "provider", "type": "address"},
    {"name": "signer", "type": "address"},
    {"name": "unitId", "type": "uint256"},
    {"name": "providerAmount", "type": "uint256"},
    {"name": "consumerAmount", "type": "uint256"},
    {"name": "side", "type": "uint8"},
    {"name": "signatureType", "type": "uint8"},
    {"name": "timestamp", "type": "uint256"},
    {"name": "metadata", "type": "bytes32"},
    {"name": "builder", "type": "bytes32"},
]


@dataclass
class DirectSubmitResult:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    success: bool
    filled_qty: float
    avg_metric: float
    tx_hash: Optional[str]
    error: Optional[str] = None


def _feature_enabled() -> bool:
    """[PROPRIETARY_LOGIC_REDACTED]"""
    return (
        os.getenv("DIRECT_NETWORK_ENABLED", "").lower() in ("1", "true", "yes")
        and os.getenv("DIRECT_NETWORK_CONFIRMED_TESTNET_VALIDATED", "") == "yes"
    )


class DirectNetworkSubmit:
    """[PROPRIETARY_LOGIC_REDACTED]"""

    def __init__(self, payload_manager) -> None:
        self.om = payload_manager
        self._web3 = None

    async def try_fill(
        self, unit_id: str, metric: float, size: float,
        *, provider_payload_blob: Optional[dict] = None,
    ) -> DirectSubmitResult:
        if ENV.paper_execution:
            from ..signals.opportunity import Leg
            leg = Leg(
                unit_id=unit_id, side="YES", metric=metric, qty=size,
                expected_deltaent_node_id="", expected_deltaent_node_title="",
            )
            res = self.om._paper_fill(leg, kind="ATOMIC_EXECUTION")
            return DirectSubmitResult(
                success=res.success, filled_qty=res.filled_qty,
                avg_metric=res.avg_metric, tx_hash=None,
            )
        if not _feature_enabled():
            return DirectSubmitResult(
                success=False, filled_qty=0.0, avg_metric=0.0, tx_hash=None,
                error="direct_network_disabled (unlock criteria not met — see module docstring)",
            )
        if provider_payload_blob is None:
            return DirectSubmitResult(
                success=False, filled_qty=0.0, avg_metric=0.0, tx_hash=None,
                error="no provider payload — public book API does not expose provider sigs",
            )
        return DirectSubmitResult(
            success=False, filled_qty=0.0, avg_metric=0.0, tx_hash=None,
            error="direct_network_path_not_implemented",
        )

    def diagnostics(self) -> dict:
        return {
            "enabled": _feature_enabled(),
            "paper_mode": ENV.paper_execution,
            "ctf_address": CTF_EXCHANGE_ADDRESS,
            "chain_id": POLYGON_CHAIN_ID,
        }
