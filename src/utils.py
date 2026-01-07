"""Logging and helper functions."""

import logging
import sys
from pathlib import Path


def setup_logging(
    log_level: int = logging.INFO,
    log_file: str | Path | None = None,
) -> logging.Logger:
    """Set up logging configuration.

    Args:
        log_level: Logging level (default: INFO).
        log_file: Optional path to log file.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("ldj_pipeline")
    logger.setLevel(log_level)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(console_format)
        logger.addHandler(file_handler)

    return logger

