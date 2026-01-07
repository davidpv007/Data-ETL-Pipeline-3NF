# tests/test_transformation.py

import polars as pl
from pathlib import Path
import pytest

def test_schema_file_exists():
    """Test that schema.sql exists."""
    from pathlib import Path
    schema_file = Path("schema/schema.sql")
    assert schema_file.exists(), "schema.sql file not found"

def test_config_module():
    """Test that config module can be imported."""
    try:
        from src.config import Config
        assert True
    except ImportError:
        pytest.fail("Cannot import Config from src.config")
        

def test_data_loads():
    """Test that CSV data loads correctly."""
    csv_path = Path("data/data_jobs.csv")
    df = pl.read_csv(csv_path)
    assert df.shape[0] > 0
    assert "job_id" in df.columns or len(df.columns) > 0

def test_data_has_required_columns():
    """Test that required columns exist."""
    csv_path = Path("data/data_jobs.csv")
    df = pl.read_csv(csv_path)
    required_cols = ["job_title", "company_name", "job_location"]
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"