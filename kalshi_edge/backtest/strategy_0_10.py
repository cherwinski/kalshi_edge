from __future__ import annotations

# pytest monkeypatch compatibility: expose a connection_ctx symbol
try:
    from kalshi_edge.db import connection_ctx as _connection_ctx
except Exception:  # pragma: no cover - tests will monkeypatch
    _connection_ctx = None  # type: ignore[assignment]

connection_ctx = _connection_ctx

"""Backtest the "buy at 0.10 YES" strategy."""
import argparse
import csv
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ..util.logging import get_logger
from .strategy_threshold import run_threshold_backtest
from . import strategy_threshold as _threshold_mod

LOGGER = get_logger(__name__)
DEFAULT_THRESHOLD = 0.10


def run_backtest(threshold: float = DEFAULT_THRESHOLD) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # Ensure threshold engine uses our (monkeypatchable) connection_ctx during tests.
    _threshold_mod.connection_ctx = connection_ctx  # type: ignore[attr-defined]
    # For the 0.10 strategy we treat this as buying YES when price is <= threshold.
    old_ge = _threshold_mod.operator.ge
    try:
        _threshold_mod.operator.ge = _threshold_mod.operator.le  # type: ignore[attr-defined]
        return run_threshold_backtest(threshold=threshold, direction="yes")
    finally:  # pragma: no cover - restore for safety
        _threshold_mod.operator.ge = old_ge


def _print_summary(summary: Dict[str, Any]) -> None:
    LOGGER.info("Executed %d trades", summary["num_trades"])
    print(
        "\n".join(
            [
                f"Threshold: {summary['threshold']:.2f}",
                f"Trades: {summary['num_trades']}",
                f"Win rate: {summary['win_rate'] * 100:.2f}%",
                f"Average entry: {summary['average_entry_price']:.4f}",
                f"Average profit: {summary['average_profit']:.4f}",
                f"Total profit: {summary['total_profit']:.4f}",
                f"Max drawdown: {summary['max_drawdown']:.4f}",
            ]
        )
    )


def _write_trades_csv(path: Path, trades: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["market_id", "entry_timestamp", "entry_price", "resolution", "profit"]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)
    LOGGER.info("Wrote %d trades to %s", len(trades), path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the 0.10 YES backtest")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--csv-path", type=Path, default=None, help="Optional CSV output path")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    summary, trades = run_backtest(args.threshold)
    _print_summary(summary)
    if args.csv_path:
        _write_trades_csv(args.csv_path, trades)
