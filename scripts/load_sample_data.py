"""CLI for loading synthetic sample data into the database."""
from __future__ import annotations

from kalshi_edge import db
from kalshi_edge.util.sample_data import load_sample_markets, load_sample_prices


def main() -> None:
    with db.get_connection() as conn:
        markets_inserted = load_sample_markets(conn)
        prices_inserted = load_sample_prices(conn)
    print(f"Inserted {markets_inserted} markets and {prices_inserted} price rows (existing rows skipped).")


if __name__ == "__main__":
    main()
