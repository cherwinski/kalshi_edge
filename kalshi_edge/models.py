"""Pydantic models for Kalshi entities."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class Market(BaseModel):
    """Represents a Kalshi market."""

    market_id: str = Field(..., description="Kalshi market identifier")
    name: str
    category: Optional[str] = Field(None, description="Optional market category")
    resolution: Optional[str] = Field(None, description="YES/NO or other resolution")
    resolved_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class PriceSnapshot(BaseModel):
    """Represents a price snapshot for a market."""

    market_id: str
    timestamp: datetime
    bid_yes: Optional[float] = None
    ask_yes: Optional[float] = None
    last_yes: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = Field(None, alias="openInterest")


class TradeResult(BaseModel):
    """Represents an executed trade during backtesting."""

    market_id: str
    entry_price: float
    resolution: str
    profit: float
    entry_timestamp: datetime


__all__ = ["Market", "PriceSnapshot", "TradeResult"]
