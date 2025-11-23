"""Database connection utilities and migration runner."""
from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import Iterator, List

import psycopg2
from psycopg2.extensions import connection as PGConnection

from .config import load_settings

LOGGER = logging.getLogger(__name__)
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def get_connection() -> PGConnection:
    """Create a new psycopg2 connection using the configured DATABASE_URL."""

    settings = load_settings()
    return psycopg2.connect(settings.database_url)


@contextlib.contextmanager
def connection_ctx() -> Iterator[PGConnection]:
    """Context manager that yields a PostgreSQL connection."""

    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def _load_migration_files() -> List[Path]:
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def run_migrations() -> None:
    """Execute SQL migrations located in the migrations directory."""

    sql_files = _load_migration_files()
    if not sql_files:
        LOGGER.info("No migration files found in %s", MIGRATIONS_DIR)
        return

    with connection_ctx() as conn:
        with conn.cursor() as cursor:
            for sql_path in sql_files:
                LOGGER.info("Running migration %s", sql_path.name)
                cursor.execute(sql_path.read_text())
        conn.commit()
    LOGGER.info("Migrations complete.")


__all__ = ["get_connection", "connection_ctx", "run_migrations"]
