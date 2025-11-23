"""Test utilities for faking database access."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterable, List


class FakeCursor:
    def __init__(self, markets: List[dict], prices: Dict[str, List[dict]]):
        self._markets = markets
        self._prices = prices
        self._result: List[dict] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - no cleanup needed
        return None

    def execute(self, query: str, params: Iterable | None = None) -> None:
        params = list(params or [])
        if "FROM markets" in query:
            self._result = list(self._markets)
            return

        if "FROM prices" in query:
            market_id = params[0] if params else None
            rows = list(self._prices.get(market_id, []))
            if "timestamp <=" in query and len(params) > 1:
                cutoff = params[1]
                rows = [row for row in rows if row["timestamp"] <= cutoff]
            if "ORDER BY timestamp DESC" in query:
                rows = sorted(rows, key=lambda r: r["timestamp"], reverse=True)
            else:
                rows = sorted(rows, key=lambda r: r["timestamp"])
            if "LIMIT 1" in query:
                self._result = rows[:1]
            else:
                self._result = rows
            return

        self._result = []

    def fetchall(self) -> List[dict]:
        return list(self._result)

    def fetchone(self) -> dict | None:
        return self._result[0] if self._result else None


class FakeConnection:
    def __init__(self, markets: List[dict], prices: Dict[str, List[dict]]):
        self._markets = markets
        self._prices = prices

    def cursor(self, cursor_factory=None):  # pragma: no cover - signature parity only
        return FakeCursor(self._markets, self._prices)

    def close(self) -> None:  # pragma: no cover - compatibility hook
        return None

    def commit(self) -> None:  # pragma: no cover - unused but present for completeness
        return None


def patch_connection_ctx(monkeypatch, module, markets: List[dict], prices: Dict[str, List[dict]]) -> None:
    """Monkeypatch a module's connection_ctx to use fake data."""

    @contextmanager
    def fake_ctx():
        yield FakeConnection(markets, prices)

    monkeypatch.setattr(module, "connection_ctx", fake_ctx)
