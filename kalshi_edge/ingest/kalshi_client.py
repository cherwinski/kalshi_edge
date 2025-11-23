"""Kalshi REST client wrappers."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from ..config import Environment, load_settings
from ..util.logging import get_logger

LOGGER = get_logger(__name__)
BASE_URLS = {
    Environment.DEMO: "https://demo.api.kalshi.com/v1",
    Environment.SANDBOX: "https://demo.api.kalshi.com/v1",
    Environment.LIVE: "https://api.kalshi.com/v1",
}


class KalshiClient:
    """Lightweight Kalshi REST client placeholder."""

    def __init__(self, api_key: str, api_secret: str, env: Environment | str = Environment.SANDBOX):
        if isinstance(env, str):
            env = Environment(env)
        self.api_key = api_key
        self.api_secret = api_secret
        self.env = env
        self.base_url = BASE_URLS[self.env]

    @classmethod
    def from_env(cls) -> "KalshiClient":
        """Instantiate a client using environment variables."""

        settings = load_settings()
        return cls(settings.api_key, settings.api_secret, settings.environment)

    def _headers(self) -> Dict[str, str]:
        # TODO: swap to real Kalshi auth headers (bearer token, etc.) when available.
        return {
            "User-Agent": "kalshi-edge/0.1",
            "X-API-KEY": self.api_key,
            "X-API-SECRET": self.api_secret,
        }

    def _request(self, method: str, path: str, *, params: Dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.request(method, url, headers=self._headers(), params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            LOGGER.error("Kalshi API error %s: %s", exc.response.status_code, exc.response.text)
            raise
        except httpx.HTTPError as exc:  # pragma: no cover - defensive branch
            LOGGER.error("Kalshi API request failed: %s", exc)
            raise

    def list_markets(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List markets with optional pagination cursor."""

        params: Dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor
        # TODO: adjust endpoint path based on Kalshi docs if different.
        return self._request("GET", "/markets", params=params)

    def get_market(self, market_id: str) -> Dict[str, Any]:
        """Fetch a single market record."""

        return self._request("GET", f"/markets/{market_id}")

    def get_market_prices(
        self,
        market_id: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch price snapshots for a market."""

        params: Dict[str, Any] = {}
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()
        payload = self._request("GET", f"/markets/{market_id}/prices", params=params)
        return payload.get("prices", payload)


__all__ = ["KalshiClient"]
