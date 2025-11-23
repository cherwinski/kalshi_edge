"""Kalshi trading client wrapper."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from kalshi_python import ApiClient, Configuration
from kalshi_python.api.portfolio_api import PortfolioApi
from kalshi_python.models import CreateOrderRequest

from ..config import get_kalshi_creds, get_kalshi_env

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@dataclass
class OrderRequest:
    market_ticker: str
    side: str  # "yes" or "no"
    size: int
    price: Optional[float]  # limit price in dollars, or None for market if supported
    direction: str = "buy"  # "buy" or "sell"


class ExecutionClient:
    """Lightweight wrapper for the Kalshi Trading/Portfolio API."""

    def __init__(self) -> None:
        env = get_kalshi_env()
        api_key_id, api_key_secret = get_kalshi_creds()

        configuration = Configuration()
        configuration.host = DEMO_BASE_URL if env == "demo" else LIVE_BASE_URL

        self.api_client = ApiClient(configuration=configuration)
        self.api_client.set_kalshi_auth(api_key_id, api_key_secret)
        self.portfolio_api = PortfolioApi(self.api_client)

    def place_order(self, order: OrderRequest) -> Dict[str, Any]:
        """Place an order via the Trading API."""

        side = order.side.lower()
        direction = order.direction.lower()
        if side not in ("yes", "no"):
            raise ValueError("order.side must be 'yes' or 'no'")
        if direction not in ("buy", "sell"):
            raise ValueError("order.direction must be 'buy' or 'sell'")

        price_cents: Optional[int] = None
        if order.price is not None:
            price_cents = max(1, min(99, int(round(order.price * 100))))

        # API expects lowercase enum values for side/action/type.
        req_kwargs: Dict[str, Any] = {
            "ticker": order.market_ticker,
            "side": side,
            "action": direction,
            "type": "limit",
            "count": int(order.size),
        }
        if side == "yes":
            req_kwargs["yes_price"] = price_cents
        else:
            req_kwargs["no_price"] = price_cents

        create_req = CreateOrderRequest(**req_kwargs)
        # Manually call the API to avoid nesting the body under "create_order_request".
        resp, _, _ = self.api_client.call_api(
            "/portfolio/orders",
            "POST",
            body=create_req.to_dict(),
            response_type=None,
            _check_return_type=False,
        )
        order_obj = resp.get("order") if isinstance(resp, dict) else None

        avg_price_raw = (
            getattr(order_obj, "yes_price", None) if side == "yes" else getattr(order_obj, "no_price", None)
        )
        avg_price = (avg_price_raw / 100.0) if avg_price_raw is not None else None

        return {
            "order_id": getattr(order_obj, "order_id", None),
            "status": getattr(order_obj, "status", None),
            "filled_size": getattr(order_obj, "count", None),
            "avg_price": avg_price,
            "raw": resp.to_dict() if hasattr(resp, "to_dict") else resp,
        }

    def get_open_exposure_usd(self) -> float:
        """Return open exposure; stubbed to 0 for now."""

        # TODO: query positions/exposure from Kalshi account.
        return 0.0


__all__ = ["ExecutionClient", "OrderRequest"]
