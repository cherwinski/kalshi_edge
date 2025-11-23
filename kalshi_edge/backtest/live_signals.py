"""Estimate expected value for potential trades using calibration curves."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Sequence, Tuple

from ..util.logging import get_logger
from .calibration import DEFAULT_BINS, compute_calibration

LOGGER = get_logger(__name__)


def _load_bins(csv_path: Path | None, num_bins: int) -> List[dict]:
    if csv_path and csv_path.exists():
        LOGGER.info("Loading calibration bins from %s", csv_path)
        with csv_path.open() as handle:
            reader = csv.DictReader(handle)
            bins = []
            for row in reader:
                bins.append(
                    {
                        "bucket_low": float(row["bucket_low"]),
                        "bucket_high": float(row["bucket_high"]),
                        "n": int(row["n"]),
                        "n_yes": int(row["n_yes"]),
                        "p_mkt_avg": float(row["p_mkt_avg"]) if row["p_mkt_avg"] else None,
                        "p_true": float(row["p_true"]) if row["p_true"] else None,
                    }
                )
            return bins
    LOGGER.info("Computing calibration bins on the fly")
    return compute_calibration(num_bins)


def _bucket_midpoints(bins: Sequence[dict]) -> List[Tuple[float, float]]:
    points: List[Tuple[float, float]] = []
    for bucket in bins:
        if bucket["p_true"] is None:
            continue
        mid = bucket["bucket_low"] + (bucket["bucket_high"] - bucket["bucket_low"]) / 2
        points.append((mid, bucket["p_true"]))
    return sorted(points, key=lambda item: item[0])


def estimate_p_true(p_mkt: float, bins: Sequence[dict] | None = None) -> float:
    bins_list = list(bins) if bins is not None else compute_calibration(DEFAULT_BINS)
    points = _bucket_midpoints(bins_list)
    if not points:
        raise RuntimeError("Calibration bins missing p_true values; run calibration first")

    if p_mkt <= points[0][0]:
        return points[0][1]
    if p_mkt >= points[-1][0]:
        return points[-1][1]

    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if x0 <= p_mkt <= x1:
            if x1 == x0:
                return y0
            weight = (p_mkt - x0) / (x1 - x0)
            return y0 + weight * (y1 - y0)
    return points[-1][1]


def expected_value_yes(price: float, bins: Sequence[dict] | None = None) -> float:
    p_true = estimate_p_true(price, bins)
    return p_true * (1.0 - price) + (1.0 - p_true) * (-price)


def main() -> None:
    parser = argparse.ArgumentParser(description="Estimate EV from calibration data")
    parser.add_argument("--price", type=float, required=True, help="Current YES price")
    parser.add_argument("--bins", type=int, default=DEFAULT_BINS, help="Number of calibration buckets to compute if CSV missing")
    parser.add_argument("--csv", type=Path, default=None, help="Optional calibration CSV path")
    args = parser.parse_args()

    bins = _load_bins(args.csv, args.bins)
    p_true = estimate_p_true(args.price, bins)
    ev = expected_value_yes(args.price, bins)
    print(f"Market price: {args.price:.2f}")
    print(f"Estimated true probability: {p_true:.4f}")
    print(f"Expected value per YES contract: {ev:.4f}")


if __name__ == "__main__":
    main()
