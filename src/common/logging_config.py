"""Centralised logging configuration."""

import logging
import sys


def setup_logging(name: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """Return a pre-configured logger."""
    logger = logging.getLogger(name or __name__)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
