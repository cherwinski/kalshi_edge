"""Kalshi trading client wrapper."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from kalshi_python import ApiClient, Configuration
try:
    from kalshi_python.api.trading_api import TradingApi  # type: ignore
except ImportError:  # pragma: no cover - optional SDK surface
    TradingApi = None  # type: ignore

from ..config import get_kalshi_creds, get_kalshi_env

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@dataclass
class OrderRequest:
    market_ticker: str
    side: str  # "yes" or "no"
    size: int
    price: Optional[float]  # limit price in dollars, or None for market if supported


class ExecutionClient:
    """Lightweight wrapper for the Kalshi Trading API."""

    def __init__(self) -> None:
        env = get_kalshi_env()
        api_key_id, api_key_secret = get_kalshi_creds()

        configuration = Configuration()
        configuration.host = DEMO_BASE_URL if env == "demo" else LIVE_BASE_URL

        self.api_client = ApiClient(configuration=configuration)
        self.api_client.set_kalshi_auth(api_key_id, api_key_secret)
        self.trading_api = TradingApi(self.api_client) if TradingApi else None

    def place_order(self, order: OrderRequest) -> Dict[str, Any]:
        """Place an order via the Trading API.

        Note: order payloads vary by SDK version; this method may need adjustment
        if/when live execution is enabled. For now, raise if unsupported.
        """

        raise NotImplementedError(
            "Live order placement is not wired yet; TradingApi unavailable or not implemented. Keep EXECUTION_MODE=simulate."
        )

    def get_open_exposure_usd(self) -> float:
        """Return open exposure; stubbed to 0 for now."""

        # TODO: query positions/exposure from Kalshi account.
        return 0.0


__all__ = ["ExecutionClient", "OrderRequest"]
