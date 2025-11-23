"""Wrapper around the official Kalshi Python SDK."""
from __future__ import annotations

import os
from datetime import datetime
import json
from typing import Iterable, List, Optional
from urllib.parse import urlencode

from kalshi_python import ApiClient, Configuration
from kalshi_python.api.markets_api import MarketsApi

from ..config import get_kalshi_creds, get_kalshi_env
from ..util.logging import get_logger

LOGGER = get_logger(__name__)

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiSDKClient:
    """Lightweight helper to talk to Kalshi via the official SDK."""

    def __init__(self) -> None:
        env = get_kalshi_env()
        api_key_id, api_key_secret = get_kalshi_creds()

        configuration = Configuration()
        configuration.host = DEMO_BASE_URL if env == "demo" else LIVE_BASE_URL
        # Allow opting out of SSL verification if a corporate MITM is intercepting traffic.
        # Default stays secure; set KALSHI_VERIFY_SSL=false to disable verification.
        verify_ssl_env = (os.getenv("KALSHI_VERIFY_SSL") or "true").lower()
        configuration.verify_ssl = verify_ssl_env not in ("0", "false", "no")
        self.request_timeout = float(os.getenv("KALSHI_HTTP_TIMEOUT", "30"))

        self.api_client = ApiClient(configuration=configuration)
        # SDK expects the secret to be the path to the private key; adjust if Kalshi changes auth.
        self.api_client.set_kalshi_auth(api_key_id, api_key_secret)
        self.markets_api = MarketsApi(self.api_client)

    def iter_markets(self, status: Optional[str] = None, limit: int = 500) -> Iterable[dict]:
        """Yield markets with cursor pagination."""

        cursor: Optional[str] = None
        while True:
            response = self.markets_api.get_markets(limit=limit, cursor=cursor, status=status)
            payload = response.to_dict() if hasattr(response, "to_dict") else response
            markets: List[dict] = payload.get("markets") or []
            for market in markets:
                yield market

            cursor = payload.get("cursor")
            if not cursor:
                break

    def iter_markets_allow_invalid(
        self, status: Optional[str] = None, limit: int = 500
    ) -> Iterable[dict]:
        """Yield markets, tolerating Kalshi enum drift by bypassing Pydantic validation."""

        cursor: Optional[str] = None
        while True:
            params = {"limit": limit, "cursor": cursor, "status": status}
            query = urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{self.api_client.configuration.host}/markets?{query}"
            raw = self.api_client.call_api(
                method="GET",
                url=url,
                header_params=dict(self.api_client.default_headers),
                _request_timeout=self.request_timeout,
            )
            body = raw.data or raw.read()
            if not body:
                raise RuntimeError(f"Empty response from {url} (status={raw.status})")
            payload = json.loads(body.decode("utf-8"))
            markets = payload.get("markets") or []
            LOGGER.info(
                "Fetched %d markets (status=%s, cursor=%s)", len(markets), status, cursor
            )
            for market in markets:
                yield market
            cursor = payload.get("cursor")
            if not cursor:
                break
    def _normalize_interval(self, interval: str | int) -> str:
        """Normalize interval formats ('1h', '5m', 60) into minute strings expected by API."""
        if isinstance(interval, (int, float)):
            return str(int(interval))
        raw = str(interval).strip().lower()
        if raw.endswith("h"):
            return str(int(float(raw[:-1]) * 60))
        if raw.endswith("m"):
            return str(int(float(raw[:-1])))
        return str(int(raw))

    def get_market_candles(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        interval: Optional[str | int] = None,
    ) -> List[dict]:
        """Fetch candlesticks for a market."""

        def _ts(val: Optional[int | datetime]) -> Optional[int]:
            if val is None:
                return None
            if isinstance(val, datetime):
                return int(val.timestamp())
            return int(val)

        period_interval = self._normalize_interval(interval or "60")
        response = self.markets_api.get_market_candlesticks(
            ticker=series_ticker,
            market_ticker=market_ticker,
            start_ts=_ts(start_ts),
            end_ts=_ts(end_ts),
            period_interval=period_interval,
        )
        payload = response.to_dict() if hasattr(response, "to_dict") else response
        return payload.get("candlesticks") or []


__all__ = ["KalshiSDKClient"]
