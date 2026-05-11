"""Tests for inventory cost-based valuation."""
import pytest
from datetime import date
from services.inventory_metrics import get_inventory_costs, get_stock_levels_ledger, get_abc_analysis


def test_cost_query_returns_data():
    """Verify cost data exists for Feb 2025."""
    as_of_date = date(2025, 2, 28)
    cost_df = get_inventory_costs(as_of_date)

    assert not cost_df.empty, "No cost data found for Feb 28, 2025"
    assert 'product_id' in cost_df.columns
    assert 'cost_unit_tax_in' in cost_df.columns
    assert (cost_df['cost_unit_tax_in'] >= 0).all(), "Negative costs found"


def test_cost_query_returns_expected_columns():
    """Verify cost query returns expected columns."""
    as_of_date = date(2025, 2, 28)
    cost_df = get_inventory_costs(as_of_date)

    expected_cols = {'product_id', 'cost_unit_tax_in'}
    actual_cols = set(cost_df.columns)
    assert expected_cols.issubset(actual_cols), f"Missing columns: {expected_cols - actual_cols}"


def test_cost_query_as_of_date_filtering():
    """Verify cost query respects as_of_date parameter."""
    # Query as of Feb 28 should have data
    feb_df = get_inventory_costs(date(2025, 2, 28))

    # Query as of Feb 10 (earlier) should have less or equal data
    feb_10_df = get_inventory_costs(date(2025, 2, 10))

    # Feb 28 should have same or more products than Feb 10
    assert len(feb_df) >= len(feb_10_df), \
        f"Feb 28 has {len(feb_df)} products but Feb 10 has {len(feb_10_df)}"


def test_cost_values_are_reasonable():
    """Verify cost values are within reasonable range."""
    as_of_date = date(2025, 2, 28)
    cost_df = get_inventory_costs(as_of_date)

    if cost_df.empty:
        pytest.skip("No cost data available")

    # Check for extreme outliers (costs should typically be under 1M IDR)
    max_cost = cost_df['cost_unit_tax_in'].max()
    assert max_cost < 10_000_000, f"Suspiciously high cost found: {max_cost}"

    # Check that most products have non-zero costs
    zero_cost_pct = (cost_df['cost_unit_tax_in'] == 0).mean()
    assert zero_cost_pct < 0.5, f"{zero_cost_pct:.1%} of products have zero cost"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
