"""
QA/validation tests for profit ETL materialization.
Run with: python -m pytest tests/test_profit_etl.py -v
"""
import os
import tempfile
import datetime as dt
from pathlib import Path
from unittest.mock import patch, MagicMock

import polars as pl
import pytest

# Add root to path so we can import etl_tasks
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_tasks import (
    _build_product_cost_events,
    _build_product_cost_latest_daily,
    _build_sales_lines_profit,
    _build_profit_aggregates,
    _partition_path,
    _write_partitioned,
    _tax_multiplier_expr,
)


@pytest.fixture
def temp_data_dir():
    """Temporary directory for test parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_purchases_df():
    """Sample fact_purchases data."""
    return pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 4,
        'move_id': [101, 102, 103, 104],
        'move_line_id': [1001, 1002, 1003, 1004],
        'product_id': [1, 2, 1, 3],
        'actual_price': [10.0, 20.0, 12.0, -5.0],  # one negative price (bonus)
        'quantity': [5.0, 3.0, 2.0, 1.0],
        'tax_id': [5, 7, 2, 5],
    })


@pytest.fixture
def sample_sales_df():
    """Sample fact_sales data."""
    return pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 3,
        'order_id': [501, 502, 503],
        'line_id': [2001, 2002, 2003],
        'product_id': [1, 2, 4],
        'quantity': [2.0, 1.0, 3.0],
        'revenue': [25.0, 22.2, 60.0],
    })


@pytest.fixture
def sample_invoice_sales_df():
    """Sample fact_invoice_sales data."""
    return pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 2,
        'move_id': [601, 602],
        'move_line_id': [3001, 3002],
        'product_id': [1, 2],
        'quantity': [1.0, 2.0],
        'price_unit': [15.0, 18.0],
        'tax_id': [7, 5],
    })


def test_tax_multiplier_expr():
    """Test tax multiplier logic."""
    df = pl.DataFrame({'tax_id': [5, 2, 7, 6, 99]})
    result = df.with_columns(
        multiplier=_tax_multiplier_expr('tax_id')
    ).to_dict(as_series=False)
    assert result['multiplier'] == [1.0, 1.0, 1.11, 1.11, 1.0]


def test_build_product_cost_events_basic(temp_data_dir, sample_purchases_df):
    """Test cost events extraction from purchases."""
    target_date = '2025-03-15'
    base_path = os.path.join(temp_data_dir, 'star-schema', 'fact_purchases')
    os.makedirs(_partition_path(base_path, target_date), exist_ok=True)
    sample_purchases_df.write_parquet(
        os.path.join(_partition_path(base_path, target_date), 'fact_purchases_2025-03-15.parquet')
    )

    # Mock paths to point to temp dir
    with patch('etl_tasks.STAR_SCHEMA_PATH', os.path.join(temp_data_dir, 'star-schema')):
        result = _build_product_cost_events(target_date)

    expected = pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 3,
        'product_id': [1, 2, 1],
        'cost_unit_tax_in': [10.0, 22.2, 12.0],  # tax applied
        'source_move_id': [101, 102, 103],
        'source_tax_id': [5, 7, 2],
    })
    # Sort for comparison and compare as dicts
    result_sorted = result.sort('product_id')
    expected_sorted = expected.sort('product_id')
    # Use pytest.approx for floating point comparison
    for col in result_sorted.columns:
        if result_sorted[col].dtype in [pl.Float64, pl.Float32]:
            assert all(result_sorted[col].to_list()[i] == pytest.approx(expected_sorted[col].to_list()[i]) 
                   for i in range(len(result_sorted)))
        else:
            assert result_sorted[col].to_list() == expected_sorted[col].to_list()


def test_build_product_cost_events_excludes_negatives(temp_data_dir):
    """Negative/zero actual_price or quantity should be excluded."""
    target_date = '2025-03-15'
    bad_df = pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 4,
        'move_id': [101, 102, 103, 104],
        'product_id': [1, 2, 3, 4],
        'actual_price': [10.0, -5.0, 0.0, 8.0],
        'quantity': [5.0, 3.0, 2.0, 0.0],
        'tax_id': [5, 5, 5, 5],
    })
    base_path = os.path.join(temp_data_dir, 'star-schema', 'fact_purchases')
    os.makedirs(_partition_path(base_path, target_date), exist_ok=True)
    bad_df.write_parquet(
        os.path.join(_partition_path(base_path, target_date), 'fact_purchases_2025-03-15.parquet')
    )
    with patch('etl_tasks.STAR_SCHEMA_PATH', os.path.join(temp_data_dir, 'star-schema')):
        result = _build_product_cost_events(target_date)
    # Only row 1 (positive price/quantity) should remain
    assert len(result) == 1
    assert result.to_dict(as_series=False)['product_id'] == [1]


def test_build_sales_lines_profit_merges_cost(temp_data_dir, sample_sales_df, sample_invoice_sales_df):
    """Test sales-line profit joins cost snapshot."""
    target_date = '2025-03-15'

    # Write sales data
    base_path = os.path.join(temp_data_dir, 'star-schema')
    for name, df in [('fact_sales', sample_sales_df), ('fact_invoice_sales', sample_invoice_sales_df)]:
        part_path = _partition_path(os.path.join(base_path, name), target_date)
        os.makedirs(part_path, exist_ok=True)
        df.write_parquet(os.path.join(part_path, f'{name}_2025-03-15.parquet'))

    # Mock cost snapshot
    cost_df = pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 4,
        'product_id': [1, 2, 4, 99],  # include extra product to test left join
        'cost_unit_tax_in': [10.0, 20.0, 15.0, 50.0],
        'source_move_id': [101, 102, 103, 999],
        'source_tax_id': [5, 7, 5, 5],
    })
    cost_path = os.path.join(base_path, 'fact_product_cost_latest_daily')
    os.makedirs(_partition_path(cost_path, target_date), exist_ok=True)
    cost_df.write_parquet(os.path.join(_partition_path(cost_path, target_date), 'fact_product_cost_latest_daily_2025-03-15.parquet'))

    with patch('etl_tasks.STAR_SCHEMA_PATH', os.path.join(temp_data_dir, 'star-schema')), \
         patch('etl_tasks.FACT_PRODUCT_COST_LATEST_DAILY_PATH', os.path.join(temp_data_dir, 'star-schema', 'fact_product_cost_latest_daily')):
        result = _build_sales_lines_profit(target_date)

    # Verify revenue_tax_in includes tax
    # POS: revenue already tax-inclusive
    # Invoice: price_unit * quantity * tax_multiplier
    assert len(result) == 5
    # Spot check a POS line (product 1, qty 2, revenue 25)
    pos_line = result.filter((result['txn_id'] == 501) & (result['line_id'] == 2001))
    assert pos_line['revenue_tax_in'][0] == 25.0
    assert pos_line['cogs_tax_in'][0] == 20.0  # 10 * 2
    assert pos_line['gross_profit'][0] == 5.0

    # Spot check an invoice line (product 2, qty 2, price 18, tax 5 -> multiplier 1.0)
    inv_line = result.filter((result['txn_id'] == 602) & (result['line_id'] == 3002))
    assert inv_line['revenue_tax_in'][0] == 36.0  # 18 * 2 * 1.0
    assert inv_line['cogs_tax_in'][0] == 40.0  # 20 * 2
    assert inv_line['gross_profit'][0] == -4.0


def test_build_profit_aggregates():
    """Test daily and by-product profit aggregation."""
    profit_df = pl.DataFrame({
        'date': [dt.date(2025, 3, 15)] * 4,
        'txn_id': [1, 2, 2, 3],
        'line_id': [10, 20, 21, 30],
        'product_id': [1, 2, 2, 1],
        'quantity': [2, 1, 1, 3],
        'revenue_tax_in': [25.0, 22.2, 11.1, 45.0],
        'cogs_tax_in': [20.0, 20.0, 20.0, 30.0],
        'gross_profit': [5.0, 2.2, -8.9, 15.0],
    })
    daily, by_product = _build_profit_aggregates(profit_df)

    # Daily aggregates
    assert len(daily) == 1
    assert daily['date'][0] == dt.date(2025, 3, 15)
    assert daily['revenue_tax_in'][0] == pytest.approx(103.3)
    assert daily['cogs_tax_in'][0] == pytest.approx(90.0)
    assert daily['gross_profit'][0] == pytest.approx(13.3)
    assert daily['transactions'][0] == 3
    assert daily['lines'][0] == 4

    # By-product aggregates
    assert len(by_product) == 2
    prod1 = by_product.filter(by_product['product_id'] == 1)
    prod2 = by_product.filter(by_product['product_id'] == 2)
    assert prod1['gross_profit'][0] == pytest.approx(20.0)  # 5 + 15
    assert prod2['gross_profit'][0] == pytest.approx(-6.7)  # 2.2 - 8.9


def test_write_partitioned(temp_data_dir):
    """Test partitioned parquet write helper."""
    df = pl.DataFrame({'x': [1, 2], 'y': ['a', 'b']})
    target_date = '2025-03-15'
    base_path = os.path.join(temp_data_dir, 'test')
    result_path = _write_partitioned(df, base_path, target_date, 'test_table')
    # Normalize path separators for cross-platform comparison
    expected_path = os.path.normpath(os.path.join(base_path, 'year=2025', 'month=03', 'day=15', 'test_table_2025-03-15.parquet'))
    assert os.path.normpath(result_path) == expected_path
    assert os.path.isfile(result_path)
    # Verify content
    written = pl.read_parquet(result_path)
    assert written.to_dict(as_series=False) == df.to_dict(as_series=False)


def test_partition_path():
    """Test partition path helper."""
    base = '/data/star-schema/fact_sales'
    date = '2025-03-15'
    expected = '/data/star-schema/fact_sales/year=2025/month=03/day=15'
    assert _partition_path(base, date) == expected


if __name__ == '__main__':
    # Quick smoke test
    pytest.main([__file__, '-v'])
