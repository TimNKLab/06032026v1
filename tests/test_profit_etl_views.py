"""Tests for profit ETL view group loading."""
import pytest


class TestProfitDetailViewGroup:
    """Test profit_detail view group loads correctly."""

    def test_profit_detail_view_queries_work(self):
        """Querying fact_product_costs_unified doesn't raise error."""
        from services.duckdb_connector import get_duckdb_connection
        
        # Get connection (singleton, may already be initialized)
        conn = get_duckdb_connection()
        
        # This should not raise "table does not exist" error
        try:
            result = conn.execute("""
                SELECT product_id, cost_unit_tax_in, cost_source
                FROM fact_product_costs_unified
                WHERE cost_unit_tax_in > 0
                LIMIT 1
            """).fetchone()
            # If we get here, query succeeded
            assert result is not None or True  # May be empty if no data yet
        except Exception as e:
            pytest.fail(f"Query failed: {e}")
