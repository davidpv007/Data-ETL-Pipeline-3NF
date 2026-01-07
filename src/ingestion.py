"""Phase 1: CSV ingestion logic."""

import polars as pl
from pathlib import Path


def load_csv(file_path: str | Path) -> pl.DataFrame:
    """Load CSV file into a Polars DataFrame.

    Args:
        file_path: Path to the CSV file.

    Returns:
        Polars DataFrame with the loaded data.
    """
    return pl.read_csv(file_path)

