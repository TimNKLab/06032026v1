from datetime import date
from typing import Dict, Optional
import pandas as pd
import time
from .duckdb_connector import (
    query_profit_trends as duckdb_query_profit_trends,
    query_profit_by_product as duckdb_query_profit_by_product,
    query_profit_summary as duckdb_query_profit_summary,
    query_profit_revenue_by_category as duckdb_query_profit_revenue_by_category,
    query_profit_drilldown as duckdb_query_profit_drilldown
)


def query_profit_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query profit trends - uses DuckDB aggregates."""
    return duckdb_query_profit_trends(start_date, end_date, period)


def query_profit_by_product(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products by profit - uses DuckDB aggregates."""
    return duckdb_query_profit_by_product(start_date, end_date, limit)


def query_profit_summary(start_date: date, end_date: date) -> Dict:
    """Get profit summary - uses DuckDB aggregates."""
    return duckdb_query_profit_summary(start_date, end_date)


def query_profit_revenue_by_category(start_date: date, end_date: date) -> Dict[str, Dict[str, float]]:
    """Query profit revenue by category - uses DuckDB aggregates."""
    return duckdb_query_profit_revenue_by_category(start_date, end_date)


def query_profit_drilldown(start_date: date, end_date: date, product_id: Optional[int] = None) -> pd.DataFrame:
    """Drill-down to line-level profit details - uses DuckDB fact table."""
    return duckdb_query_profit_drilldown(start_date, end_date, product_id)
