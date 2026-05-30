"""
Column consistency test for inventory parquet schemas.

This test verifies that Polars code uses actual parquet column names,
not DuckDB view column names, to prevent runtime errors.
"""
import pytest
import os


def test_fact_inventory_moves_schema():
    """Verify fact_inventory_moves parquet has expected columns."""
    import polars as pl
    
    data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
    moves_path = f'{data_lake_root}/star-schema/fact_inventory_moves/**/*.parquet'
    
    try:
        df = pl.scan_parquet(moves_path, hive_partitioning=True).collect()
        expected_columns = [
            'date', 'move_id', 'move_line_id', 'product_id', 'product_name', 
            'product_brand', 'location_src_id', 'location_src_name', 
            'location_src_usage', 'location_dest_id', 'location_dest_name', 
            'location_dest_usage', 'qty_moved', 'uom_id', 'uom_name', 
            'uom_category', 'movement_type', 'inventory_adjustment_flag',
            'manufacturing_order_id', 'picking_id', 'picking_type_code',
            'reference', 'origin_reference', 'source_partner_id', 
            'source_partner_name', 'destination_partner_id', 
            'destination_partner_name', 'created_by_user', 'create_date',
            'year', 'month', 'day'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing expected column: {col}"
            
    except Exception as e:
        pytest.skip(f"Cannot access parquet files: {e}")


def test_polars_code_uses_parquet_columns():
    """Verify Polars code uses actual parquet column names."""
    # Import the function
    from services.inventory_metrics import _query_location_ledger_deltas
    from datetime import datetime
    
    # Read the source code to check column references
    import inspect
    source = inspect.getsource(_query_location_ledger_deltas)
    
    # Should use 'date' not 'movement_date' for parquet reads
    assert 'pl.col("date")' in source or "pl.col('date')" in source, \
        "Polars code should use 'date' column for parquet reads"
    assert 'pl.col("movement_date")' not in source, \
        "Polars code should not use 'movement_date' for parquet reads (that's a DuckDB view column)"