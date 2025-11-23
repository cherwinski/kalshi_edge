"""Logging helpers."""
from __future__ import annotations

import logging
from typing import Optional


def setup_logging(level: int = logging.INFO) -> None:
    """Configure a basic logging handler once."""

    if getattr(setup_logging, "_configured", False):  # pragma: no cover
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    setup_logging._configured = True  # type: ignore[attr-defined]


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger, ensuring logging is initialized."""

    setup_logging()
    return logging.getLogger(name)


__all__ = ["get_logger", "setup_logging"]
