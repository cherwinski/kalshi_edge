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


def get_initial_bankroll_usd() -> float:
    """Configured starting capital used when no account snapshot is available."""

    try:
        return float(os.getenv("INITIAL_BANKROLL_USD", "1000.0"))
    except ValueError:
        return 1000.0


def get_max_risk_fraction_per_trade() -> float:
    """Maximum fraction of bankroll to risk on a single trade (e.g., 0.03 = 3%)."""

    try:
        # Default to a conservative 1.5% until we have better signal confidence.
        return float(os.getenv("MAX_RISK_FRACTION_PER_TRADE", "0.015"))
    except ValueError:
        return 0.015


def get_take_profit_factor() -> float:
    """Multiple of entry price required to trigger take-profit exits."""

    try:
        return float(os.getenv("TAKE_PROFIT_FACTOR", "4.0"))
    except ValueError:
        return 4.0


def get_pro_longshot_take_profit_factor() -> float:
    """Take-profit factor specifically for pro long-shot entries (default 2.2x)."""

    try:
        return float(os.getenv("PRO_LONGSHOT_TP_FACTOR", "2.2"))
    except ValueError:
        return 2.2


def get_current_bankroll_usd(conn) -> float:
    """
    Return current bankroll/equity.
    - If account_pnl exists and has rows, use the latest total_equity.
    - Otherwise, fall back to INITIAL_BANKROLL_USD.
    """

    initial = get_initial_bankroll_usd()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT total_equity FROM account_pnl ORDER BY as_of_date DESC LIMIT 1;"
            )
            row = cur.fetchone()
        if row is None:
            return initial
        return float(row[0])
    except Exception:
        return initial


__all__ = [
    "Environment",
    "Settings",
    "ExecutionMode",
    "load_settings",
    "get_kalshi_env",
    "get_kalshi_creds",
    "get_execution_mode",
    "get_risk_limits",
    "get_initial_bankroll_usd",
    "get_max_risk_fraction_per_trade",
    "get_take_profit_factor",
    "get_current_bankroll_usd",
]
