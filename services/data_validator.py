from typing import Any, Dict, List
import polars as pl
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Data quality validation framework."""
    
    def __init__(self):
        self.errors: List[str] = []
    
    def validate_schema(self, df: pl.DataFrame, expected_columns: List[str]) -> bool:
        """Validate DataFrame has expected columns."""
        missing = set(expected_columns) - set(df.columns)
        if missing:
            self.errors.append(f"Missing columns: {missing}")
            return False
        return True
    
    def validate_no_nulls(self, df: pl.DataFrame, columns: List[str]) -> bool:
        """Validate specified columns have no null values."""
        for col in columns:
            null_count = df[col].null_count()
            if null_count > 0:
                self.errors.append(f"Column {col} has {null_count} null values")
                return False
        return True
    
    def validate_row_count(self, df: pl.DataFrame, min_rows: int = 1) -> bool:
        """Validate DataFrame has minimum row count."""
        if len(df) < min_rows:
            self.errors.append(f"DataFrame has only {len(df)} rows, expected at least {min_rows}")
            return False
        return True
    
    def validate_date_range(self, df: pl.DataFrame, date_col: str, 
                           min_date: str, max_date: str) -> bool:
        """Validate date column is within expected range."""
        dates = df[date_col].to_list()
        if not dates:
            self.errors.append(f"Date column {date_col} is empty")
            return False
        
        df_min = min(dates)
        df_max = max(dates)
        
        if df_min < min_date or df_max > max_date:
            self.errors.append(f"Date range {df_min} to {df_max} outside expected {min_date} to {max_date}")
            return False
        return True
    
    def get_errors(self) -> List[str]:
        """Get all validation errors."""
        return self.errors
    
    def clear_errors(self) -> None:
        """Clear validation errors."""
        self.errors = []
