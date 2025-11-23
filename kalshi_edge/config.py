"""Configuration helpers for kalshi_edge."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from typing import Literal, Optional, Tuple

ExecutionMode = Literal["simulate", "live"]


class Environment(str, Enum):
    """Deployment environment for Kalshi API access."""

    DEMO = "demo"
    SANDBOX = "sandbox"
    LIVE = "live"


@dataclass(slots=True)
class Settings:
    """Runtime application settings."""

    database_url: str
    api_key: str
    api_secret: str
    environment: Environment


_cached_settings: Optional[Settings] = None


def load_settings(force_reload: bool = False) -> Settings:
    """Load settings from environment variables."""

    global _cached_settings
    if _cached_settings is not None and not force_reload:
        return _cached_settings

    database_url = os.getenv("DATABASE_URL")
    api_key = os.getenv("KALSHI_API_KEY_ID") or os.getenv("KALSHI_API_KEY")
    api_secret = os.getenv("KALSHI_API_KEY_SECRET") or os.getenv("KALSHI_API_SECRET")
    environment_raw = os.getenv("KALSHI_ENV", Environment.DEMO.value)

    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    if not api_key or not api_secret:
        raise RuntimeError("Kalshi API credentials are not set")

    try:
        normalized_env = environment_raw.lower()
        if normalized_env == Environment.SANDBOX.value:
            normalized_env = Environment.DEMO.value
        environment = Environment(normalized_env)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise RuntimeError(
            "KALSHI_ENV must be 'demo' or 'live'"
        ) from exc

    _cached_settings = Settings(
        database_url=database_url,
        api_key=api_key,
        api_secret=api_secret,
        environment=environment,
    )
    return _cached_settings


def get_kalshi_env() -> str:
    """Return Kalshi environment string (`demo` or `live`)."""

    env_raw = os.getenv("KALSHI_ENV", Environment.DEMO.value).lower()
    if env_raw == Environment.SANDBOX.value:
        env_raw = Environment.DEMO.value
    if env_raw not in (Environment.DEMO.value, Environment.LIVE.value):
        raise RuntimeError("KALSHI_ENV must be 'demo' or 'live'")
    return env_raw


def get_kalshi_creds() -> Tuple[str, str]:
    """Fetch Kalshi API key id and secret (secret is the private key path)."""

    api_key_id = os.getenv("KALSHI_API_KEY_ID") or os.getenv("KALSHI_API_KEY")
    api_key_secret = os.getenv("KALSHI_API_KEY_SECRET") or os.getenv("KALSHI_API_SECRET")
    if not api_key_id or not api_key_secret:
        raise RuntimeError("Kalshi API credentials are not set")
    return api_key_id, api_key_secret


def get_execution_mode() -> ExecutionMode:
    """Return execution mode; default to simulate for safety."""

    mode = (os.getenv("EXECUTION_MODE") or "simulate").lower()
    if mode not in ("simulate", "live"):
        mode = "simulate"
    return mode  # type: ignore[return-value]


def get_risk_limits() -> dict:
    """Load simple USD risk caps for execution."""

    def _get_float(name: str, default: float) -> float:
        val = os.getenv(name)
        if not val:
            return default
        try:
            return float(val)
        except ValueError:
            return default

    return {
        "max_risk_per_trade": _get_float("MAX_RISK_PER_TRADE_USD", 10.0),
        "max_risk_per_market": _get_float("MAX_RISK_PER_MARKET_USD", 50.0),
        "max_risk_total": _get_float("MAX_RISK_TOTAL_USD", 200.0),
    }


__all__ = [
    "Environment",
    "Settings",
    "ExecutionMode",
    "load_settings",
    "get_kalshi_env",
    "get_kalshi_creds",
    "get_execution_mode",
    "get_risk_limits",
]
