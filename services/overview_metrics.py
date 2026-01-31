from datetime import date
from typing import Dict

from services.duckdb_connector import query_overview_summary


def get_total_overview_summary(target_date_start: date, target_date_end: date = None) -> Dict:
    if not isinstance(target_date_start, date):
        target_date_start = date.today()
    if target_date_end is None:
        target_date_end = target_date_start

    try:
        # Use DuckDB for faster queries
        return query_overview_summary(target_date_start, target_date_end)
    except Exception as e:
        print(f"DuckDB query failed in get_total_overview_summary: {e}")
        return {
            'target_date_start': target_date_start,
            'target_date_end': target_date_end,
            'today_amount': 0.0,
            'today_qty': 0.0,
            'prev_amount': 0.0,
            'categories_nested': {},
            'brands_nested': {},
        }
