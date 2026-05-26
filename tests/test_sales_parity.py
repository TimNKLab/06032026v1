"""Test parity between DuckDB and SQLite sales queries."""
import sys
sys.path.insert(0, 'd:/NKLabs/Plotly/nkdash')

from datetime import date
import pandas as pd


def test_sales_query_parity():
    """Test that SQLite queries return same results as DuckDB aggregates."""
    from services.duckdb_connector import get_duckdb_connection
    from services.sales_metrics import get_sales_trends_data
    from services.sqlite_manager import SQLiteManager
    
    # Initialize SQLite and refresh MVs first
    manager = SQLiteManager()
    manager.initialize_db()
    conn = manager.get_writer_conn()
    
    # Refresh sales MVs from DuckDB (full refresh to load all data)
    manager.refresh_mv("mv_sales_daily", "sales", conn)
    manager.refresh_mv("mv_sales_by_product", "sales", conn)
    manager.refresh_mv("mv_sales_by_principal", "sales", conn)
    
    conn.close()
    
    start_date = date(2025, 5, 1)
    end_date = date(2025, 5, 7)
    
    # Get DuckDB results from aggregate view (source)
    duckdb_conn = get_duckdb_connection()
    duckdb_query = """
    SELECT date, COALESCE(SUM(revenue), 0) as revenue
    FROM agg_sales_daily
    WHERE date >= ? AND date <= ?
    GROUP BY date
    ORDER BY date
    """
    duckdb_results = duckdb_conn.execute(duckdb_query, [start_date, end_date]).fetchdf()
    
    # Get SQLite results from MV (destination)
    sqlite_results = get_sales_trends_data(start_date, end_date)
    
    # Compare results
    assert len(duckdb_results) == len(sqlite_results), f"Row count mismatch: DuckDB {len(duckdb_results)} vs SQLite {len(sqlite_results)}"
    for idx, duckdb_row in duckdb_results.iterrows():
        # Convert DuckDB datetime to date string for comparison
        duckdb_date_str = str(duckdb_row['date']).split()[0] if ' ' in str(duckdb_row['date']) else str(duckdb_row['date'])
        sqlite_row = sqlite_results.iloc[idx]
        sqlite_date_str = str(sqlite_row['date'])
        
        assert duckdb_date_str == sqlite_date_str, f"Date mismatch: DuckDB {duckdb_date_str} vs SQLite {sqlite_date_str}"
        assert abs(duckdb_row['revenue'] - sqlite_row['revenue']) < 0.01, f"Revenue mismatch on {duckdb_date_str}: DuckDB {duckdb_row['revenue']} vs SQLite {sqlite_row['revenue']}"


if __name__ == "__main__":
    test_sales_query_parity()
    print("Sales query parity test passed!")
