"""Calibration analysis for Kalshi market probabilities."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from psycopg2.extras import RealDictCursor

from ..db import connection_ctx
from ..util.logging import get_logger
from .common import compute_mid_price

LOGGER = get_logger(__name__)
DEFAULT_BINS = 10
OUT_DIR = Path("out")
EXTREME_BIN_EDGES = [0.0, 0.02, 0.05, 0.10, 0.20, 0.40, 0.60, 0.80, 0.90, 0.95, 0.98, 1.0]


def _init_buckets_from_edges(edges: List[float]) -> List[Dict[str, Any]]:
    if len(edges) < 2:
        raise ValueError("At least two edges are required")
    buckets: List[Dict[str, Any]] = []
    for low, high in zip(edges[:-1], edges[1:]):
        if high <= low:
            raise ValueError("Bin edges must be strictly increasing")
        buckets.append(
            {
                "bucket_low": low,
                "bucket_high": high,
                "n": 0,
                "n_yes": 0,
                "p_mkt_sum": 0.0,
                "p_mkt_avg": None,
                "p_true": None,
            }
        )
    return buckets


def _bucket_edges(num_bins: int) -> List[Dict[str, Any]]:
    step = 1.0 / num_bins
    edges = [i * step for i in range(num_bins + 1)]
    return _init_buckets_from_edges(edges)


def _bucket_index(p_mkt: float, num_bins: int) -> int:
    if p_mkt <= 0:
        return 0
    if p_mkt >= 1:
        return num_bins - 1
    return min(int(p_mkt * num_bins), num_bins - 1)


def _bucket_from_edges(p_mkt: float, edges: List[float]) -> int:
    if p_mkt <= edges[0]:
        return 0
    if p_mkt >= edges[-1]:
        return len(edges) - 2
    for idx, (low, high) in enumerate(zip(edges[:-1], edges[1:])):
        inclusive_high = idx == (len(edges) - 2)
        if low <= p_mkt < high or (inclusive_high and p_mkt <= high):
            return idx
    return len(edges) - 2


def _latest_price(cursor: RealDictCursor, market_id: str, resolved_at: Optional[Any]) -> Optional[Dict[str, Any]]:
    if resolved_at:
        cursor.execute(
            """
            SELECT timestamp, bid_yes, ask_yes, last_yes, volume, open_interest
            FROM prices
            WHERE market_id = %s AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (market_id, resolved_at),
        )
    else:
        cursor.execute(
            """
            SELECT timestamp, bid_yes, ask_yes, last_yes, volume, open_interest
            FROM prices
            WHERE market_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (market_id,),
        )
    return cursor.fetchone()


def _compute_calibration_generic(
    buckets: List[Dict[str, Any]],
    selector: Callable[[float], int],
) -> List[Dict[str, Any]]:
    with connection_ctx() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT market_id, resolution, resolved_at
                FROM markets
                WHERE resolution IS NOT NULL
                """
            )
            markets = cursor.fetchall()
            for market in markets:
                price_row = _latest_price(cursor, market["market_id"], market["resolved_at"])
                if not price_row:
                    continue
                p_mkt = compute_mid_price(price_row)
                if p_mkt is None:
                    continue
                idx = selector(p_mkt)
                bucket = buckets[idx]
                bucket["n"] += 1
                if (market["resolution"] or "").upper() == "YES":
                    bucket["n_yes"] += 1
                bucket["p_mkt_sum"] += p_mkt

    for bucket in buckets:
        n = bucket["n"]
        bucket["p_mkt_avg"] = (bucket["p_mkt_sum"] / n) if n else None
        bucket["p_true"] = (bucket["n_yes"] / n) if n else None
        del bucket["p_mkt_sum"]
    return buckets


def compute_calibration(num_bins: int = DEFAULT_BINS) -> List[Dict[str, Any]]:
    buckets = _bucket_edges(num_bins)
    return _compute_calibration_generic(buckets, lambda p: _bucket_index(p, num_bins))


def compute_calibration_with_bins(bin_edges: List[float]) -> List[Dict[str, Any]]:
    buckets = _init_buckets_from_edges(bin_edges)
    return _compute_calibration_generic(buckets, lambda p: _bucket_from_edges(p, bin_edges))


def _write_csv(buckets: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["bucket_low", "bucket_high", "n", "n_yes", "p_mkt_avg", "p_true"]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(buckets)
    LOGGER.info("Wrote calibration CSV to %s", path)


def _write_plot(buckets: List[Dict[str, Any]], path: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:  # pragma: no cover - optional dependency
        LOGGER.warning("matplotlib not installed; skipping plot generation")
        return

    xs = [bucket["bucket_low"] + (bucket["bucket_high"] - bucket["bucket_low"]) / 2 for bucket in buckets]
    ys = [bucket["p_true"] if bucket["p_true"] is not None else 0 for bucket in buckets]

    plt.figure(figsize=(6, 6))
    plt.plot(xs, xs, linestyle="--", color="gray", label="Perfect calibration")
    plt.plot(xs, ys, marker="o", label="Observed")
    plt.xlabel("Market-implied probability")
    plt.ylabel("Empirical YES frequency")
    plt.title("Kalshi Calibration")
    plt.legend()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    LOGGER.info("Wrote calibration plot to %s", path)


def _print_table(buckets: List[Dict[str, Any]]) -> None:
    header = f"{'Bucket':<12} {'Count':<6} {'YES':<6} {'p_mkt':<8} {'p_true':<8}"
    print(header)
    print("-" * len(header))
    for bucket in buckets:
        label = f"{bucket['bucket_low']:.2f}-{bucket['bucket_high']:.2f}"
        p_mkt = bucket["p_mkt_avg"] if bucket["p_mkt_avg"] is not None else 0.0
        p_true = bucket["p_true"] if bucket["p_true"] is not None else 0.0
        print(f"{label:<12} {bucket['n']:<6d} {bucket['n_yes']:<6d} {p_mkt:<8.2f} {p_true:<8.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute calibration curves")
    parser.add_argument("--bins", type=int, default=DEFAULT_BINS, help="Number of equal-width probability buckets")
    parser.add_argument("--extreme-bins", action="store_true", help="Use finer bins near 0 and 1")
    parser.add_argument("--csv", type=Path, default=OUT_DIR / "calibration.csv", help="Output CSV path")
    parser.add_argument("--plot", type=Path, default=OUT_DIR / "calibration.png", help="Output PNG path")
    args = parser.parse_args()

    if args.extreme_bins:
        buckets = compute_calibration_with_bins(EXTREME_BIN_EDGES)
    else:
        buckets = compute_calibration(args.bins)
    _print_table(buckets)
    _write_csv(buckets, args.csv)
    _write_plot(buckets, args.plot)


if __name__ == "__main__":
    main()
