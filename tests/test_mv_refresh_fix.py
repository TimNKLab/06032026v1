"""Tests for MV refresh fix — stale data bug."""
import pytest
from unittest.mock import MagicMock
from datetime import date


class TestGetMvRefreshInfo:
    """Test _get_mv_refresh_info returns correct (needs_full, max_date, count)."""

    def test_returns_full_refresh_when_table_missing(self):
        """When MV table doesn't exist, needs_full=True."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        # information_schema.tables returns 0 (table doesn't exist)
        mock_conn.execute.return_value.fetchone.return_value = (0,)

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is True
        assert max_date is None
        assert count == 0

    def test_returns_max_date_when_table_exists(self):
        """When MV exists, returns max_date from actual table."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        # First call: information_schema check → table exists (count=1)
        # Second call: SELECT MAX(date) → returns a date
        mock_conn.execute.return_value.fetchone.side_effect = [
            (1,),                    # table exists
            (date(2025, 9, 30),),    # max date
        ]

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is False
        assert max_date == date(2025, 9, 30)

    def test_returns_full_refresh_when_max_date_is_none(self):
        """When MV exists but is empty (max_date=None), needs_full=True."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [
            (1,),    # table exists
            (None,), # max date is None (empty table)
        ]

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is True
        assert max_date is None

    def test_returns_full_refresh_on_exception(self):
        """On any exception, returns (True, None, 0) to trigger full refresh."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("DB error")

        needs_full, max_date, count = manager._get_mv_refresh_info(
            mock_conn, "mv_profit_daily", "/fake/path"
        )

        assert needs_full is True
        assert max_date is None


class TestClearCaches:
    """Test cache clearing functions exist and work."""

    def test_clear_profit_caches_clears_lru(self):
        """clear_profit_caches() clears lru_cache on all profit query fns."""
        from services.profit_metrics import (
            query_profit_trends, query_profit_summary, clear_profit_caches
        )
        # Attempt a call to populate cache (may fail due to no DB, but cache tracks it)
        try:
            query_profit_trends(date(2025, 1, 1), date(2025, 1, 31))
        except Exception:
            pass

        clear_profit_caches()

        # After clear, currsize should be 0
        assert query_profit_trends.cache_info().currsize == 0
        assert query_profit_summary.cache_info().currsize == 0

    def test_clear_sales_caches_exists_and_callable(self):
        """clear_sales_caches() is importable and callable without error."""
        from services.duckdb_connector import clear_sales_caches
        # Should not raise even if cache is empty
        clear_sales_caches()

    def test_clear_sales_caches_clears_lru(self):
        """clear_sales_caches() clears lru_cache on sales query fns."""
        from services.duckdb_connector import query_sales_trends, clear_sales_caches
        # Attempt a call to populate cache
        try:
            query_sales_trends(date(2025, 1, 1), date(2025, 1, 31))
        except Exception:
            pass

        clear_sales_caches()

        assert query_sales_trends.cache_info().currsize == 0


class TestEnsureMaterializedViewsForceReload:
    """Test force_reload bypasses tracking set."""

    def test_force_reload_bypasses_tracking_set(self):
        """force_reload=True should call _load_materialized_views even if MV already tracked."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        manager._materialized_views = {"mv_profit_daily"}  # already tracked
        manager._connection = MagicMock()

        load_called_with = []

        def fake_load(conn, views):
            load_called_with.extend(views)

        manager._load_materialized_views = fake_load
        manager.get_connection = lambda: manager._connection

        manager.ensure_materialized_views({"mv_profit_daily"}, force_reload=True)

        assert "mv_profit_daily" in load_called_with

    def test_normal_load_skips_tracked_mvs(self):
        """Without force_reload, already-tracked MVs are skipped."""
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager.__new__(DuckDBManager)
        manager._lock = __import__('threading').Lock()
        manager._materialized_views = {"mv_profit_daily"}  # already tracked
        manager._connection = MagicMock()

        load_called_with = []

        def fake_load(conn, views):
            load_called_with.extend(views)

        manager._load_materialized_views = fake_load
        manager.get_connection = lambda: manager._connection

        manager.ensure_materialized_views({"mv_profit_daily"}, force_reload=False)

        # Should NOT call _load_materialized_views since MV already tracked
        assert "mv_profit_daily" not in load_called_with
