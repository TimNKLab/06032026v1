import pytest
import os
import sys
import polars as pl

# Add project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.data_validator import DataValidator

def test_validate_schema():
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    validator = DataValidator()
    assert validator.validate_schema(df, ["a", "b"]) is True
    assert validator.validate_schema(df, ["a", "c"]) is False

def test_validate_no_nulls():
    df = pl.DataFrame({"a": [1, 2, None]})
    validator = DataValidator()
    assert validator.validate_no_nulls(df, ["a"]) is False
    assert len(validator.get_errors()) > 0

def test_validate_row_count():
    df = pl.DataFrame({"a": [1]})
    validator = DataValidator()
    assert validator.validate_row_count(df, min_rows=1) is True
    assert validator.validate_row_count(df, min_rows=2) is False
