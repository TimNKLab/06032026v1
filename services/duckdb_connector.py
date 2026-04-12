import duckdb
import os
import logging
import threading
import time
from datetime import date, timedelta
from typing import Dict, Optional
from functools import lru_cache
import pandas as pd
from .cache import cache
from .versioned_cache import versioned_cache

logger = logging.getLogger(__name__)


class DuckDBManager:
    _instance: Optional['DuckDBManager'] = None
    _connection: Optional[duckdb.DuckDBPyConnection] = None
    _lock = threading.Lock()
    _initialized = False
    _initialized_groups: set[str] = set()
    _materialized_views: set[str] = set()  # Track which MVs are loaded

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            with self._lock:
                if self._connection is None:
                    # Use persistent DuckDB so materialized views survive restarts
                    data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
                    db_path = f"{data_lake}/cache/nkdash.duckdb"
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                    
                    conn = duckdb.connect(database=db_path)
                    setup_start = time.time()
                    print(f"[duckdb] connecting to {db_path}...")
                    
                    # Check if materialized views already exist (from previous run)
                    existing_mvs = set()
                    try:
                        tables = conn.execute("SHOW TABLES").fetchall()
                        existing_mvs = {t[0] for t in tables if t[0].startswith('mv_')}
                        print(f"[duckdb] existing materialized views: {sorted(existing_mvs)}")
                    except Exception:
                        pass
                    
                    preload_all = os.environ.get("PRELOAD_ALL_DUCKDB_VIEWS") in {"1", "true", "True", "yes", "YES"}
                    if preload_all:
                        self._setup_views(conn, groups={"all"})
                        self._initialized_groups = {"all"}
                    else:
                        # Default: only what Overview needs (fast), avoid Gunicorn timeouts.
                        self._setup_views(conn, groups={"overview"})
                        self._initialized_groups = {"overview"}
                    
                    # Track which MVs are already loaded
                    self._materialized_views = existing_mvs
                    
                    print(f"[duckdb] views ready in {time.time() - setup_start:.3f}s")
                    self._connection = conn
                    self._initialized = True
        return self._connection

    def ensure_view_groups(self, groups: set[str]) -> None:
        """Ensure the requested view groups exist on the singleton connection."""
        if not groups:
            return

        conn = self.get_connection()
        with self._lock:
            if "all" in self._initialized_groups:
                return

            needed = set(groups) - set(self._initialized_groups)
            if not needed:
                return

            start = time.time()
            print(f"[duckdb] ensuring view groups: {sorted(needed)}")
            self._setup_views(conn, groups=needed)
            self._initialized_groups |= needed
            print(f"[duckdb] ensured groups in {time.time() - start:.3f}s")

    def ensure_materialized_views(self, views: set[str]) -> None:
        """Ensure materialized views are loaded into memory for ultra-fast queries."""
        if not views:
            return

        conn = self.get_connection()
        with self._lock:
            needed = views - self._materialized_views
            if not needed:
                return

            start = time.time()
            print(f"[duckdb] loading materialized views: {sorted(needed)}")
            self._load_materialized_views(conn, needed)
            self._materialized_views |= needed
            print(f"[duckdb] materialized views ready in {time.time() - start:.3f}s")

    def _get_mv_refresh_info(self, conn: duckdb.DuckDBPyConnection, mv_name: str, parquet_path: str) -> tuple:
        """Get refresh info for incremental MV loading.
        Returns: (needs_full_refresh, max_date_in_mv, new_files_count)
        """
        try:
            # Check if MV exists and get max date
            result = conn.execute(f"""
                SELECT COUNT(*), MAX(date) 
                FROM information_schema.tables 
                WHERE table_name = '{mv_name}'
            """).fetchone()
            
            if result[0] == 0:
                return (True, None, 0)  # MV doesn't exist, needs full load
            
            max_mv_date = conn.execute(f"SELECT MAX(date) FROM {mv_name}").fetchone()[0]
            
            # Count new parquet files since last refresh
            new_files = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet_metadata('{parquet_path}/**/*.parquet')
                WHERE filename > (SELECT MAX(filename) FROM {mv_name}_metadata) 
                    OR NOT EXISTS (SELECT 1 FROM {mv_name}_metadata)
            """).fetchone()[0]
            
            return (False, max_mv_date, new_files)
        except Exception:
            return (True, None, 0)  # Error, do full refresh

    def _load_materialized_views(self, conn: duckdb.DuckDBPyConnection, views: set[str]) -> None:
        """Load data from parquet views into native DuckDB tables for speed.
        Uses incremental refresh - only loads new/changed partitions.
        """
        data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
        
        # Ensure metadata tracking table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mv_refresh_metadata (
                view_name VARCHAR PRIMARY KEY,
                last_refresh_date TIMESTAMP,
                max_data_date DATE,
                row_count BIGINT,
                refresh_type VARCHAR  -- 'full' or 'incremental'
            )
        """)

        # Materialized view: Daily sales aggregates (most queried)
        if "mv_sales_daily" in views:
            agg_sales_daily_path = f"{data_lake}/star-schema/agg_sales_daily"
            needs_full, max_date, _ = self._get_mv_refresh_info(conn, "mv_sales_daily", agg_sales_daily_path)
            
            if needs_full or max_date is None:
                conn.execute(f"""
                    CREATE OR REPLACE TABLE mv_sales_daily AS
                    SELECT
                        COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                        )) AS date,
                        COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                        COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                        COALESCE(TRY_CAST(items_sold AS DOUBLE), 0) AS items_sold,
                        COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                    FROM read_parquet('{agg_sales_daily_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                    WHERE revenue IS NOT NULL OR transactions IS NOT NULL
                """)
                refresh_type = 'full'
            else:
                # Incremental: load only dates > max_date in MV
                conn.execute(f"""
                    INSERT INTO mv_sales_daily
                    SELECT
                        COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                        )) AS date,
                        COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                        COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                        COALESCE(TRY_CAST(items_sold AS DOUBLE), 0) AS items_sold,
                        COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                    FROM read_parquet('{agg_sales_daily_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                    WHERE (revenue IS NOT NULL OR transactions IS NOT NULL)
                      AND COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                        )) > '{max_date}'
                """)
                refresh_type = 'incremental'
            
            # Update metadata
            conn.execute(f"""
                INSERT OR REPLACE INTO mv_refresh_metadata 
                SELECT 'mv_sales_daily', NOW(), MAX(date), COUNT(*), '{refresh_type}'
                FROM mv_sales_daily
            """)
            print(f"[duckdb] mv_sales_daily refreshed ({refresh_type})")

        # Materialized view: Sales by product
        if "mv_sales_by_product" in views:
            agg_by_product_path = f"{data_lake}/star-schema/agg_sales_daily_by_product"
            needs_full, max_date, _ = self._get_mv_refresh_info(conn, "mv_sales_by_product", agg_by_product_path)
            
            if needs_full or max_date is None:
                conn.execute(f"""
                    CREATE OR REPLACE TABLE mv_sales_by_product AS
                    SELECT
                        COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                        )) AS date,
                        COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                        COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                        COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                        COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                    FROM read_parquet('{agg_by_product_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                    WHERE revenue IS NOT NULL
                """)
            else:
                # For incremental, we'd need to handle duplicates - use INSERT OR REPLACE pattern
                conn.execute(f"""
                    CREATE OR REPLACE TABLE mv_sales_by_product AS
                    SELECT * FROM mv_sales_by_product WHERE date <= '{max_date}'
                    UNION ALL
                    SELECT
                        COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                        )) AS date,
                        COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                        COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                        COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                        COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                    FROM read_parquet('{agg_by_product_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                    WHERE revenue IS NOT NULL
                      AND COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                            CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                        )) > '{max_date}'
                """)

        # Materialized view: Sales by principal
        if "mv_sales_by_principal" in views:
            agg_by_principal_path = f"{data_lake}/star-schema/agg_sales_daily_by_principal"
            conn.execute(f"""
                CREATE OR REPLACE TABLE mv_sales_by_principal AS
                SELECT
                    COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                        CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                        CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                        CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                    )) AS date,
                    COALESCE(principal, 'Unknown') AS principal,
                    COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_by_principal_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE revenue IS NOT NULL
            """)

        # Materialized view: Daily profit aggregates
        if "mv_profit_daily" in views:
            agg_profit_path = f"{data_lake}/star-schema/agg_profit_daily"
            conn.execute(f"""
                CREATE OR REPLACE TABLE mv_profit_daily AS
                SELECT
                    COALESCE(TRY_CAST(date AS DATE), MAKE_DATE(
                        CAST(SPLIT_PART(SPLIT_PART(filename, 'year=', 2), '/', 1) AS INTEGER),
                        CAST(SPLIT_PART(SPLIT_PART(filename, 'month=', 2), '/', 1) AS INTEGER),
                        CAST(SPLIT_PART(SPLIT_PART(filename, 'day=', 2), '/', 1) AS INTEGER)
                    )) AS date,
                    COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,
                    COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,
                    COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_profit_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE revenue_tax_in IS NOT NULL
            """)

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_data_paths() -> tuple:
        """Cache data paths to avoid repeated env lookups."""
        data_lake = os.environ.get('DATA_LAKE_ROOT') or os.environ.get('DATA_LAKE_PATH', '/data-lake')
        return (
            f"{data_lake}/star-schema/fact_sales",
            f"{data_lake}/star-schema/fact_invoice_sales",
            f"{data_lake}/star-schema/fact_purchases",
            f"{data_lake}/star-schema/fact_inventory_moves",
            f"{data_lake}/star-schema/fact_stock_on_hand_snapshot",
            f"{data_lake}/star-schema/fact_product_cost_events",
            f"{data_lake}/star-schema/fact_product_cost_latest_daily",
            f"{data_lake}/star-schema/fact_product_beginning_costs",
            f"{data_lake}/star-schema/fact_product_legacy_costs",
            f"{data_lake}/star-schema/fact_product_costs_unified",
            f"{data_lake}/star-schema/fact_sales_lines_profit",
            f"{data_lake}/star-schema/agg_profit_daily",
            f"{data_lake}/star-schema/agg_profit_daily_by_product",
            f"{data_lake}/star-schema/agg_sales_daily",
            f"{data_lake}/star-schema/agg_sales_daily_by_product",
            f"{data_lake}/star-schema/agg_sales_daily_by_principal",
            f"{data_lake}/star-schema/dim_products.parquet",
            f"{data_lake}/star-schema/dim_categories.parquet",
            f"{data_lake}/star-schema/dim_brands.parquet",
            f"{data_lake}/star-schema/dim_taxes.parquet",
        )

    def _setup_views(self, conn: duckdb.DuckDBPyConnection, groups: set[str]) -> None:
        """Setup DuckDB views - fails fast on errors.

        Groups:
        - overview: only agg_profit_daily* (needed by home/Overview)
        - dims: dim_* views
        - sales_agg: agg_sales_daily* (FAST - for Sales page default queries)
        - sales: fact_sales, fact_invoice_sales, fact_purchases, fact_sales_all (SLOW - for drilldowns)
        - inventory: fact_inventory_moves, fact_stock_on_hand_snapshot
        - profit_detail: cost/profit detail views (cost events, snapshots, sales_lines_profit)
        - all: everything
        """
        if not groups:
            return

        # Check which views already exist to avoid recreating them
        existing_views = set()
        try:
            result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'").fetchall()
            existing_views = {r[0] for r in result}
            if existing_views:
                print(f"[duckdb] existing views: {sorted(existing_views)[:20]}...")
        except Exception:
            pass

        if "all" in groups:
            groups = {"overview", "dims", "sales_agg", "sales", "inventory", "profit_detail"}
        (
            fact_path,
            fact_invoice_path,
            fact_purchases_path,
            fact_inventory_moves_path,
            fact_stock_snapshot_path,
            cost_events_path,
            cost_latest_path,
            beginning_costs_path,
            legacy_costs_path,
            unified_costs_path,
            sales_profit_path,
            agg_profit_daily_path,
            agg_profit_daily_by_product_path,
            agg_sales_daily_path,
            agg_sales_daily_by_product_path,
            agg_sales_daily_by_principal_path,
            dim_products,
            dim_categories,
            dim_brands,
            dim_taxes,
        ) = self._get_data_paths()

        def _parquet_columns(parquet_path: str) -> set:
            start = time.time()
            try:
                rows = conn.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
                ).fetchall()
                # DESCRIBE returns rows like: (column_name, column_type, null, key, default, extra)
                cols = {r[0] for r in rows if r and r[0]}
                print(f"[duckdb] describe {os.path.basename(parquet_path)} in {time.time() - start:.3f}s")
                return cols
            except Exception:
                print(f"[duckdb] describe failed for {parquet_path} after {time.time() - start:.3f}s")
                return set()

        def _try_create_view(view_name: str, create_sql: str, fallback_sql: str) -> None:
            """Create a view; if underlying parquet data is missing, fall back to an empty view.

            This avoids expensive recursive filesystem scans during startup on bind-mounted volumes.
            Skips creation if view already exists (for faster restarts with persistent DB).
            """
            # Skip if view already exists
            if view_name in existing_views:
                return

            start = time.time()
            try:
                conn.execute(create_sql)
                print(f"[duckdb] view {view_name} created in {time.time() - start:.3f}s")
            except Exception as exc:
                print(
                    f"[duckdb] view {view_name} fallback (create failed) after "
                    f"{time.time() - start:.3f}s: {exc}"
                )
                conn.execute(fallback_sql)

        products_cols = set()
        categories_cols = set()
        brands_cols = set()

        if "dims" in groups:
            products_cols = _parquet_columns(dim_products)
            categories_cols = _parquet_columns(dim_categories)
            brands_cols = _parquet_columns(dim_brands)

        product_name_col = None
        product_category_col = None
        product_parent_category_col = None
        product_brand_col = None

        product_barcode_col = None
        product_sku_col = None

        category_leaf_col = None
        category_parent_col = None

        brand_name_col = None

        if "dims" in groups:
            product_name_col = (
                "product_name" if "product_name" in products_cols
                else "name" if "name" in products_cols
                else None
            )
            product_category_col = (
                "product_category" if "product_category" in products_cols
                else "category_name" if "category_name" in products_cols
                else None
            )
            product_parent_category_col = (
                "product_parent_category" if "product_parent_category" in products_cols
                else "parent_category_name" if "parent_category_name" in products_cols
                else None
            )
            product_brand_col = (
                "product_brand" if "product_brand" in products_cols
                else "brand_name" if "brand_name" in products_cols
                else None
            )

            product_barcode_col = (
                "product_barcode" if "product_barcode" in products_cols
                else "barcode" if "barcode" in products_cols
                else None
            )
            product_sku_col = (
                "product_sku" if "product_sku" in products_cols
                else "default_code" if "default_code" in products_cols
                else None
            )

            category_leaf_col = (
                "product_category" if "product_category" in categories_cols
                else "category_name" if "category_name" in categories_cols
                else None
            )
            category_parent_col = (
                "product_parent_category" if "product_parent_category" in categories_cols
                else "parent_category_name" if "parent_category_name" in categories_cols
                else None
            )

            brand_name_col = (
                "product_brand" if "product_brand" in brands_cols
                else "brand_name" if "brand_name" in brands_cols
                else None
            )

        # Use TRY_CAST and COALESCE in view definition instead of DESCRIBE
        # This handles missing columns gracefully at query time
        if "sales" in groups:
            _try_create_view(
                "fact_sales",
                f"""
                CREATE OR REPLACE VIEW fact_sales AS
                SELECT 
                    date,
                    COALESCE(TRY_CAST(order_id AS BIGINT), 0) AS order_id,
                    COALESCE(order_ref, '') AS order_ref,
                    COALESCE(TRY_CAST(pos_config_id AS BIGINT), 0) AS pos_config_id,
                    COALESCE(TRY_CAST(cashier_id AS BIGINT), 0) AS cashier_id,
                    COALESCE(TRY_CAST(customer_id AS BIGINT), 0) AS customer_id,
                    COALESCE(payment_method_ids, '') AS payment_method_ids,
                    COALESCE(TRY_CAST(line_id AS BIGINT), 0) AS line_id,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue
                FROM read_parquet('{fact_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_sales AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(0 AS BIGINT) AS order_id,
                    CAST('' AS VARCHAR) AS order_ref,
                    CAST(0 AS BIGINT) AS pos_config_id,
                    CAST(0 AS BIGINT) AS cashier_id,
                    CAST(0 AS BIGINT) AS customer_id,
                    CAST('' AS VARCHAR) AS payment_method_ids,
                    CAST(0 AS BIGINT) AS line_id,
                    CAST(0 AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS DOUBLE) AS revenue
                WHERE FALSE
                """,
            )

        if "sales" in groups:
            _try_create_view(
                "fact_invoice_sales",
                f"""
                CREATE OR REPLACE VIEW fact_invoice_sales AS
                SELECT 
                    TRY_CAST(date AS TIMESTAMP) AS date,
                    COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,
                    COALESCE(move_name, '') AS move_name,
                    COALESCE(TRY_CAST(customer_id AS BIGINT), 0) AS customer_id,
                    COALESCE(customer_name, '') AS customer_name,
                    COALESCE(TRY_CAST(move_line_id AS BIGINT), 0) AS move_line_id,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(price_unit AS DOUBLE), 0) AS price_unit,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(tax_id AS BIGINT), 0) AS tax_id,
                    COALESCE(tax_ids_json, '[]') AS tax_ids_json,
                    FALSE AS is_free_item
                FROM read_parquet('{fact_invoice_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_invoice_sales AS
                SELECT
                    CAST(NULL AS TIMESTAMP) AS date,
                    CAST(0 AS BIGINT) AS move_id,
                    CAST('' AS VARCHAR) AS move_name,
                    CAST(0 AS BIGINT) AS customer_id,
                    CAST('' AS VARCHAR) AS customer_name,
                    CAST(0 AS BIGINT) AS move_line_id,
                    CAST(0 AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS price_unit,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS BIGINT) AS tax_id,
                    CAST('[]' AS VARCHAR) AS tax_ids_json,
                    CAST(FALSE AS BOOLEAN) AS is_free_item
                WHERE FALSE
            """,
        )

        if "sales" in groups:
            _try_create_view(
                "fact_purchases",
                f"""
                CREATE OR REPLACE VIEW fact_purchases AS
                SELECT 
                    TRY_CAST(date AS TIMESTAMP) AS date,
                    COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,
                    COALESCE(move_name, '') AS move_name,
                    COALESCE(TRY_CAST(vendor_id AS BIGINT), 0) AS vendor_id,
                    COALESCE(vendor_name, '') AS vendor_name,
                    TRY_CAST(purchase_order_id AS BIGINT) AS purchase_order_id,
                    COALESCE(purchase_order_name, '') AS purchase_order_name,
                    COALESCE(TRY_CAST(move_line_id AS BIGINT), 0) AS move_line_id,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(price_unit AS DOUBLE), 0) AS price_unit,
                    COALESCE(TRY_CAST(actual_price AS DOUBLE), 0) AS actual_price,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    TRY_CAST(tax_id AS BIGINT) AS tax_id,
                    COALESCE(tax_name, '') AS tax_name,
                    COALESCE(tax_ids_json, '[]') AS tax_ids_json,
                    COALESCE(TRY_CAST(is_free_item AS BOOLEAN), FALSE) AS is_free_item
                FROM read_parquet('{fact_purchases_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_purchases AS
                SELECT
                    CAST(NULL AS TIMESTAMP) AS date,
                    CAST(0 AS BIGINT) AS move_id,
                    CAST('' AS VARCHAR) AS move_name,
                    CAST(0 AS BIGINT) AS vendor_id,
                    CAST('' AS VARCHAR) AS vendor_name,
                    CAST(NULL AS BIGINT) AS purchase_order_id,
                    CAST('' AS VARCHAR) AS purchase_order_name,
                    CAST(0 AS BIGINT) AS move_line_id,
                    CAST(0 AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS price_unit,
                    CAST(0 AS DOUBLE) AS actual_price,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(NULL AS BIGINT) AS tax_id,
                    CAST('' AS VARCHAR) AS tax_name,
                    CAST('[]' AS VARCHAR) AS tax_ids_json,
                    CAST(FALSE AS BOOLEAN) AS is_free_item
                WHERE FALSE
            """,
        )

        if "inventory" in groups:
            _try_create_view(
                "fact_inventory_moves",
                f"""
                CREATE OR REPLACE VIEW fact_inventory_moves AS
                SELECT
                    TRY_CAST(date AS TIMESTAMP) AS movement_date,
                    COALESCE(TRY_CAST(move_id AS BIGINT), 0) AS move_id,
                    TRY_CAST(move_line_id AS BIGINT) AS move_line_id,
                    TRY_CAST(product_id AS BIGINT) AS product_id,
                    COALESCE(product_name, '') AS product_name,
                    COALESCE(product_brand, '') AS product_brand,
                    TRY_CAST(location_src_id AS BIGINT) AS location_src_id,
                    COALESCE(location_src_name, '') AS location_src_name,
                    TRY_CAST(location_dest_id AS BIGINT) AS location_dest_id,
                    COALESCE(location_dest_name, '') AS location_dest_name,
                    COALESCE(TRY_CAST(qty_moved AS DOUBLE), 0) AS qty_moved,
                    TRY_CAST(uom_id AS BIGINT) AS uom_id,
                    COALESCE(uom_name, '') AS uom_name,
                    COALESCE(uom_category, '') AS uom_category,
                    COALESCE(movement_type, '') AS movement_type,
                    COALESCE(TRY_CAST(inventory_adjustment_flag AS BOOLEAN), FALSE) AS inventory_adjustment_flag,
                    TRY_CAST(manufacturing_order_id AS BIGINT) AS manufacturing_order_id,
                    TRY_CAST(picking_id AS BIGINT) AS picking_id,
                    COALESCE(picking_type_code, '') AS picking_type_code,
                    COALESCE(reference, '') AS reference,
                    COALESCE(origin_reference, '') AS origin_reference,
                    TRY_CAST(source_partner_id AS BIGINT) AS source_partner_id,
                    COALESCE(source_partner_name, '') AS source_partner_name,
                    TRY_CAST(destination_partner_id AS BIGINT) AS destination_partner_id,
                    COALESCE(destination_partner_name, '') AS destination_partner_name,
                    TRY_CAST(created_by_user AS BIGINT) AS created_by_user,
                    TRY_CAST(create_date AS TIMESTAMP) AS create_date
                FROM read_parquet('{fact_inventory_moves_path}/**/*.parquet', union_by_name=True, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_inventory_moves AS
                SELECT
                    CAST(NULL AS TIMESTAMP) AS movement_date,
                    CAST(0 AS BIGINT) AS move_id,
                    CAST(NULL AS BIGINT) AS move_line_id,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST('' AS VARCHAR) AS product_name,
                    CAST('' AS VARCHAR) AS product_brand,
                    CAST(NULL AS BIGINT) AS location_src_id,
                    CAST('' AS VARCHAR) AS location_src_name,
                    CAST(NULL AS BIGINT) AS location_dest_id,
                    CAST('' AS VARCHAR) AS location_dest_name,
                    CAST(0 AS DOUBLE) AS qty_moved,
                    CAST(NULL AS BIGINT) AS uom_id,
                    CAST('' AS VARCHAR) AS uom_name,
                    CAST('' AS VARCHAR) AS uom_category,
                    CAST('' AS VARCHAR) AS movement_type,
                    CAST(FALSE AS BOOLEAN) AS inventory_adjustment_flag,
                    CAST(NULL AS BIGINT) AS manufacturing_order_id,
                    CAST(NULL AS BIGINT) AS picking_id,
                    CAST('' AS VARCHAR) AS picking_type_code,
                    CAST('' AS VARCHAR) AS reference,
                    CAST('' AS VARCHAR) AS origin_reference,
                    CAST(NULL AS BIGINT) AS source_partner_id,
                    CAST('' AS VARCHAR) AS source_partner_name,
                    CAST(NULL AS BIGINT) AS destination_partner_id,
                    CAST('' AS VARCHAR) AS destination_partner_name,
                    CAST(NULL AS BIGINT) AS created_by_user,
                    CAST(NULL AS TIMESTAMP) AS create_date
                WHERE FALSE
            """,
        )

        if "inventory" in groups:
            _try_create_view(
                "fact_stock_on_hand_snapshot",
                f"""
                CREATE OR REPLACE VIEW fact_stock_on_hand_snapshot AS
                SELECT
                    TRY_CAST(snapshot_date AS DATE) AS snapshot_date,
                    COALESCE(TRY_CAST(quant_id AS BIGINT), 0) AS quant_id,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    TRY_CAST(location_id AS BIGINT) AS location_id,
                    TRY_CAST(lot_id AS BIGINT) AS lot_id,
                    TRY_CAST(owner_id AS BIGINT) AS owner_id,
                    TRY_CAST(company_id AS BIGINT) AS company_id,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(reserved_quantity AS DOUBLE), 0) AS reserved_quantity
                FROM read_parquet('{fact_stock_snapshot_path}/**/*.parquet', union_by_name=True, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_stock_on_hand_snapshot AS
                SELECT
                    CAST(NULL AS DATE) AS snapshot_date,
                    CAST(NULL AS BIGINT) AS quant_id,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(NULL AS BIGINT) AS location_id,
                    CAST(NULL AS BIGINT) AS lot_id,
                    CAST(NULL AS BIGINT) AS owner_id,
                    CAST(NULL AS BIGINT) AS company_id,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS DOUBLE) AS reserved_quantity
                WHERE FALSE
            """,
        )

        if "profit_detail" in groups:
            _try_create_view(
                "fact_product_cost_events",
                f"""
                CREATE OR REPLACE VIEW fact_product_cost_events AS
                SELECT
                    TRY_CAST(date AS DATE) AS date,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,
                    COALESCE(TRY_CAST(source_move_id AS BIGINT), 0) AS source_move_id,
                    COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id
                FROM read_parquet('{cost_events_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_product_cost_events AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS cost_unit_tax_in,
                    CAST(NULL AS BIGINT) AS source_move_id,
                    CAST(NULL AS BIGINT) AS source_tax_id
                WHERE FALSE
            """,
        )

        if "profit_detail" in groups:
            _try_create_view(
                "fact_product_cost_latest_daily",
                f"""
                CREATE OR REPLACE VIEW fact_product_cost_latest_daily AS
                SELECT
                    TRY_CAST(date AS DATE) AS date,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,
                    COALESCE(TRY_CAST(source_move_id AS BIGINT), 0) AS source_move_id,
                    COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id
                FROM read_parquet('{cost_latest_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_product_cost_latest_daily AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS cost_unit_tax_in,
                    CAST(NULL AS BIGINT) AS source_move_id,
                    CAST(NULL AS BIGINT) AS source_tax_id
                WHERE FALSE
            """,
        )

        if "profit_detail" in groups:
            _try_create_view(
                "fact_product_beginning_costs",
                f"""
                CREATE OR REPLACE VIEW fact_product_beginning_costs AS
                SELECT
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,
                    COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id,
                    COALESCE(TRY_CAST(effective_date AS DATE), CAST('2025-02-10' AS DATE)) AS effective_date,
                    COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) AS is_active,
                    COALESCE(notes, '') AS notes
                FROM read_parquet('{beginning_costs_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) = TRUE
                """,
                """
                CREATE OR REPLACE VIEW fact_product_beginning_costs AS
                SELECT
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS cost_unit_tax_in,
                    CAST(NULL AS BIGINT) AS source_tax_id,
                    CAST('2025-02-10' AS DATE) AS effective_date,
                    CAST(TRUE AS BOOLEAN) AS is_active,
                    CAST('' AS VARCHAR) AS notes
                WHERE FALSE
            """,
        )

        # Create view for legacy costs table
        if "profit_detail" in groups:
            _try_create_view(
                "fact_product_legacy_costs",
                f"""
                CREATE OR REPLACE VIEW fact_product_legacy_costs AS
                SELECT
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,
                    COALESCE(TRY_CAST(source_tax_id AS BIGINT), 0) AS source_tax_id,
                    COALESCE(TRY_CAST(effective_date AS DATE), CAST('2025-02-10' AS DATE)) AS effective_date,
                    COALESCE(TRY_CAST(cost_source AS VARCHAR), 'unknown') AS cost_source,
                    COALESCE(TRY_CAST(priority AS INTEGER), 3) AS priority,
                    COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) AS is_active,
                    COALESCE(TRY_CAST(created_at AS TIMESTAMP), CAST('2025-02-10 00:00:00' AS TIMESTAMP)) AS created_at,
                    COALESCE(notes, '') AS notes
                FROM read_parquet('{legacy_costs_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE COALESCE(TRY_CAST(is_active AS BOOLEAN), TRUE) = TRUE
                """,
                """
                CREATE OR REPLACE VIEW fact_product_legacy_costs AS
                SELECT
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS cost_unit_tax_in,
                    CAST(NULL AS BIGINT) AS source_tax_id,
                    CAST('2025-02-10' AS DATE) AS effective_date,
                    CAST('unknown' AS VARCHAR) AS cost_source,
                    CAST(3 AS INTEGER) AS priority,
                    CAST(TRUE AS BOOLEAN) AS is_active,
                    CAST('2025-02-10 00:00:00' AS TIMESTAMP) AS created_at,
                    CAST('' AS VARCHAR) AS notes
                WHERE FALSE
            """,
        )

        # Create unified costs view with priority logic
        if "profit_detail" in groups:
            conn.execute(f"""
            CREATE OR REPLACE VIEW fact_product_costs_unified AS
            WITH latest_costs AS (
                SELECT 
                    product_id,
                    cost_unit_tax_in,
                    source_tax_id,
                    'latest_purchase' as cost_source,
                    1 as priority,
                    date as effective_date
                FROM read_parquet('{cost_latest_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE cost_unit_tax_in > 0
            ),
            legacy_costs AS (
                SELECT 
                    product_id,
                    cost_unit_tax_in,
                    source_tax_id,
                    cost_source,
                    priority,
                    effective_date
                FROM fact_product_legacy_costs
                WHERE is_active = TRUE AND cost_unit_tax_in > 0
            ),
            all_costs AS (
                SELECT * FROM latest_costs
                UNION ALL
                SELECT * FROM legacy_costs
            ),
            ranked_costs AS (
                SELECT 
                    product_id,
                    cost_unit_tax_in,
                    source_tax_id,
                    cost_source,
                    effective_date,
                    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY priority ASC, effective_date DESC) as rn
                FROM all_costs
            )
            SELECT 
                product_id,
                cost_unit_tax_in,
                source_tax_id,
                cost_source,
                effective_date
            FROM ranked_costs
            WHERE rn = 1
            """)

        if "profit_detail" in groups:
            _try_create_view(
                "fact_sales_lines_profit",
                f"""
                CREATE OR REPLACE VIEW fact_sales_lines_profit AS
                SELECT
                    TRY_CAST(date AS DATE) AS date,
                    COALESCE(TRY_CAST(txn_id AS BIGINT), 0) AS txn_id,
                    COALESCE(TRY_CAST(line_id AS BIGINT), 0) AS line_id,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,
                    COALESCE(TRY_CAST(cost_unit_tax_in AS DOUBLE), 0) AS cost_unit_tax_in,
                    COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,
                    COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,
                    COALESCE(TRY_CAST(source_cost_move_id AS BIGINT), 0) AS source_cost_move_id,
                    COALESCE(TRY_CAST(source_cost_tax_id AS BIGINT), 0) AS source_cost_tax_id
                FROM read_parquet('{sales_profit_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW fact_sales_lines_profit AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(NULL AS BIGINT) AS txn_id,
                    CAST(NULL AS BIGINT) AS line_id,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS DOUBLE) AS revenue_tax_in,
                    CAST(0 AS DOUBLE) AS cost_unit_tax_in,
                    CAST(0 AS DOUBLE) AS cogs_tax_in,
                    CAST(0 AS DOUBLE) AS gross_profit,
                    CAST(NULL AS BIGINT) AS source_cost_move_id,
                    CAST(NULL AS BIGINT) AS source_cost_tax_id
                WHERE FALSE
            """,
        )

        if "overview" in groups:
            _try_create_view(
                "agg_profit_daily",
                f"""
                CREATE OR REPLACE VIEW agg_profit_daily AS
                SELECT
                    COALESCE(
                        TRY_CAST(date AS DATE),
                        MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))
                    ) AS date,
                    COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,
                    COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,
                    COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_profit_daily_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW agg_profit_daily AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(0 AS DOUBLE) AS revenue_tax_in,
                    CAST(0 AS DOUBLE) AS cogs_tax_in,
                    CAST(0 AS DOUBLE) AS gross_profit,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS BIGINT) AS transactions,
                    CAST(0 AS BIGINT) AS lines
                WHERE FALSE
            """,
        )

        if "overview" in groups:
            _try_create_view(
                "agg_profit_daily_by_product",
                f"""
                CREATE OR REPLACE VIEW agg_profit_daily_by_product AS
                SELECT
                    COALESCE(
                        TRY_CAST(date AS DATE),
                        MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))
                    ) AS date,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(revenue_tax_in AS DOUBLE), 0) AS revenue_tax_in,
                    COALESCE(TRY_CAST(cogs_tax_in AS DOUBLE), 0) AS cogs_tax_in,
                    COALESCE(TRY_CAST(gross_profit AS DOUBLE), 0) AS gross_profit,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_profit_daily_by_product_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW agg_profit_daily_by_product AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS revenue_tax_in,
                    CAST(0 AS DOUBLE) AS cogs_tax_in,
                    CAST(0 AS DOUBLE) AS gross_profit,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS BIGINT) AS lines
                WHERE FALSE
            """,
        )

        # Fast sales aggregates - use "sales_agg" group for sub-second response
        if "sales_agg" in groups:
            _try_create_view(
                "agg_sales_daily",
                f"""
                CREATE OR REPLACE VIEW agg_sales_daily AS
                SELECT
                    COALESCE(
                        TRY_CAST(date AS DATE),
                        MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))
                    ) AS date,
                    COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                    COALESCE(TRY_CAST(transactions AS BIGINT), 0) AS transactions,
                    COALESCE(TRY_CAST(items_sold AS DOUBLE), 0) AS items_sold,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_sales_daily_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW agg_sales_daily AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(0 AS DOUBLE) AS revenue,
                    CAST(0 AS BIGINT) AS transactions,
                    CAST(0 AS DOUBLE) AS items_sold,
                    CAST(0 AS BIGINT) AS lines
                WHERE FALSE
                """,
            )

        if "sales_agg" in groups:
            _try_create_view(
                "agg_sales_daily_by_product",
                f"""
                CREATE OR REPLACE VIEW agg_sales_daily_by_product AS
                SELECT
                    COALESCE(
                        TRY_CAST(date AS DATE),
                        MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))
                    ) AS date,
                    COALESCE(TRY_CAST(product_id AS BIGINT), 0) AS product_id,
                    COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_sales_daily_by_product_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW agg_sales_daily_by_product AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(NULL AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS revenue,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS BIGINT) AS lines
                WHERE FALSE
                """,
            )

        if "sales_agg" in groups:
            _try_create_view(
                "agg_sales_daily_by_principal",
                f"""
                CREATE OR REPLACE VIEW agg_sales_daily_by_principal AS
                SELECT
                    COALESCE(
                        TRY_CAST(date AS DATE),
                        MAKE_DATE(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER))
                    ) AS date,
                    COALESCE(principal, 'Unknown') AS principal,
                    COALESCE(TRY_CAST(revenue AS DOUBLE), 0) AS revenue,
                    COALESCE(TRY_CAST(quantity AS DOUBLE), 0) AS quantity,
                    COALESCE(TRY_CAST(lines AS BIGINT), 0) AS lines
                FROM read_parquet('{agg_sales_daily_by_principal_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                """,
                """
                CREATE OR REPLACE VIEW agg_sales_daily_by_principal AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST('Unknown' AS VARCHAR) AS principal,
                    CAST(0 AS DOUBLE) AS revenue,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(0 AS BIGINT) AS lines
                WHERE FALSE
                """,
            )

        if "sales" in groups:
            _try_create_view(
                "fact_sales_all",
                f"""
                CREATE OR REPLACE VIEW fact_sales_all AS
                SELECT
                    MAKE_DATE(year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS date,
                    order_id AS txn_id,
                    line_id AS line_id,
                    product_id,
                    revenue,
                    quantity,
                    year,
                    month,
                    day,
                    order_ref
                FROM read_parquet('{fact_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE COALESCE(order_ref, '') != '/' OR order_ref IS NULL
                UNION ALL
                SELECT
                    MAKE_DATE(year, CAST(month AS INTEGER), CAST(day AS INTEGER)) AS date,
                    move_id AS txn_id,
                    move_line_id AS line_id,
                    product_id,
                    price_unit * quantity AS revenue,
                    quantity,
                    year,
                    month,
                    day,
                    move_name AS order_ref
                FROM read_parquet('{fact_invoice_path}/**/*.parquet', union_by_name=True, hive_partitioning=1, filename=true)
                WHERE COALESCE(move_name, '') != '/' OR move_name IS NULL
                """,
                """
                CREATE OR REPLACE VIEW fact_sales_all AS
                SELECT
                    CAST(NULL AS DATE) AS date,
                    CAST(0 AS BIGINT) AS txn_id,
                    CAST(0 AS BIGINT) AS line_id,
                    CAST(0 AS BIGINT) AS product_id,
                    CAST(0 AS DOUBLE) AS revenue,
                    CAST(0 AS DOUBLE) AS quantity,
                    CAST(NULL AS INTEGER) AS year,
                    CAST(NULL AS VARCHAR) AS month,
                    CAST(NULL AS VARCHAR) AS day,
                    CAST('' AS VARCHAR) AS order_ref
                WHERE FALSE
            """,
        )

        if "dims" in groups:
            barcode_expr = "''"
            if product_barcode_col:
                barcode_expr = (
                    "CASE "
                    f"WHEN {product_barcode_col} IS NULL THEN '' "
                    f"WHEN LOWER(TRIM(CAST({product_barcode_col} AS VARCHAR))) = 'false' THEN '' "
                    f"ELSE TRIM(CAST({product_barcode_col} AS VARCHAR)) "
                    "END"
                )

            _try_create_view(
                "dim_products",
                f"""
                CREATE OR REPLACE VIEW dim_products AS
                SELECT
                    product_id,
                    {f"COALESCE({product_name_col}, '')" if product_name_col else "''"} AS product_name,
                    {product_category_col if product_category_col else "NULL"} AS product_category,
                    {product_parent_category_col if product_parent_category_col else "NULL"} AS product_parent_category,
                    {f"COALESCE({product_brand_col}, '')" if product_brand_col else "''"} AS product_brand,
                    {barcode_expr} AS product_barcode,
                    {barcode_expr} AS barcode,
                    {f"COALESCE({product_sku_col}, '')" if product_sku_col else "''"} AS product_sku
                FROM read_parquet('{dim_products}', union_by_name=True)
                """,
                """
                CREATE OR REPLACE VIEW dim_products AS
                SELECT
                    CAST(0 AS BIGINT) AS product_id,
                    CAST('' AS VARCHAR) AS product_name,
                    CAST(NULL AS VARCHAR) AS product_category,
                    CAST(NULL AS VARCHAR) AS product_parent_category,
                    CAST('' AS VARCHAR) AS product_brand,
                    CAST('' AS VARCHAR) AS product_barcode,
                    CAST('' AS VARCHAR) AS barcode,
                    CAST('' AS VARCHAR) AS product_sku
                WHERE FALSE
            """,
        )

        if "dims" in groups:
            _try_create_view(
                "dim_categories",
                f"""
                CREATE OR REPLACE VIEW dim_categories AS
                SELECT
                    {category_leaf_col if category_leaf_col else "NULL"} AS product_category,
                    {category_parent_col if category_parent_col else "NULL"} AS product_parent_category
                FROM read_parquet('{dim_categories}', union_by_name=True)
                """,
                """
                CREATE OR REPLACE VIEW dim_categories AS
                SELECT
                    CAST(NULL AS VARCHAR) AS product_category,
                    CAST(NULL AS VARCHAR) AS product_parent_category
                WHERE FALSE
            """,
        )

        if "dims" in groups:
            _try_create_view(
                "dim_brands",
                f"""
                CREATE OR REPLACE VIEW dim_brands AS
                SELECT
                    {f"COALESCE({brand_name_col}, '')" if brand_name_col else "''"} AS product_brand
                FROM read_parquet('{dim_brands}', union_by_name=True)
                """,
                """
                CREATE OR REPLACE VIEW dim_brands AS
                SELECT
                    CAST('' AS VARCHAR) AS product_brand
                WHERE FALSE
            """,
        )

        if "dims" in groups:
            if os.path.exists(dim_taxes):
                conn.execute(f"""
                    CREATE OR REPLACE VIEW dim_taxes AS
                    SELECT
                        COALESCE(TRY_CAST(tax_id AS BIGINT), 0) AS tax_id,
                        COALESCE(tax_name, '') AS tax_name
                    FROM read_parquet('{dim_taxes}', union_by_name=True)
                """)
            else:
                conn.execute("""
                    CREATE OR REPLACE VIEW dim_taxes AS
                    SELECT
                        CAST(NULL AS BIGINT) AS tax_id,
                        CAST('' AS VARCHAR) AS tax_name
                    WHERE FALSE
                """)

        logger.info("DuckDB views created successfully")


def ensure_duckdb_view_groups(groups: set[str]) -> None:
    """Public helper for callers (metrics/pages) to ensure required view groups exist."""
    DuckDBManager().ensure_view_groups(groups)


def ensure_materialized_views(views: set[str]) -> None:
    """Public helper to load materialized views for ultra-fast queries."""
    DuckDBManager().ensure_materialized_views(views)


def query_sales_by_principal(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    ensure_duckdb_view_groups({"sales_agg"})
    conn = DuckDBManager().get_connection()

    query = """
        SELECT 
            principal,
            SUM(revenue) as revenue
        FROM agg_sales_daily_by_principal
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY principal
        ORDER BY revenue DESC
        LIMIT ?
    """

    query_start = time.time()
    try:
        result = conn.execute(query, [start_date, end_date, int(limit)]).fetchdf()
        print(f"[TIMING] query_sales_by_principal: {time.time() - query_start:.3f}s")
        print(f"[DEBUG] Returned columns: {list(result.columns)}")
        print(f"[DEBUG] Number of columns: {len(result.columns)}")
        print(f"[DEBUG] Sample row: {result.iloc[0].to_dict() if not result.empty else 'No rows'}")
        return result
    except Exception as exc:
        print(f"[TIMING] query_sales_by_principal FAILED: {time.time() - query_start:.3f}s")
        logger.exception("DuckDB query failed in query_sales_by_principal: %s", exc)
        return pd.DataFrame(columns=["principal", "revenue"])


# Module-level connection getter
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    return DuckDBManager().get_connection()


@versioned_cache(ttl=3600, key_prefix="sales_trends")
@lru_cache(maxsize=32)
def query_sales_trends(start_date: date, end_date: date, period: str = 'daily') -> pd.DataFrame:
    """Query sales trends - uses pre-aggregated daily data for speed."""
    ensure_duckdb_view_groups({"sales_agg"})
    conn = get_duckdb_connection()

    trunc_map = {'daily': 'day', 'weekly': 'week', 'monthly': 'month'}
    if period not in trunc_map:
        raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")

    trunc_expr = trunc_map[period]

    query = f"""
    WITH date_series AS (
        SELECT date_trunc('{trunc_expr}', date) as period_start,
               date_trunc('{trunc_expr}', date) + interval '1 {trunc_expr}' as period_end
        FROM generate_series(
            date_trunc('{trunc_expr}', ?::date)::timestamp,
            date_trunc('{trunc_expr}', ?::date)::timestamp,
            interval '1 {trunc_expr}'
        ) as t(date)
    )
    SELECT 
        ds.period_start as date,
        COALESCE(SUM(a.revenue), 0) as revenue,
        COALESCE(SUM(a.transactions), 0) as transactions,
        COALESCE(SUM(a.items_sold), 0) as items_sold,
        CASE 
            WHEN SUM(a.transactions) > 0 
            THEN SUM(a.revenue) / SUM(a.transactions) 
            ELSE 0 
        END as avg_transaction_value
    FROM date_series ds
    LEFT JOIN agg_sales_daily a ON 
        a.date >= ds.period_start AND 
        a.date < ds.period_end
    GROUP BY ds.period_start
    ORDER BY ds.period_start
    """
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date]).fetchdf()
    print(f"[TIMING] query_sales_trends: {time.time() - query_start:.3f}s")
    return result


def query_hourly_sales_pattern(target_date: date) -> pd.DataFrame:
    """Query hourly sales - pre-generates all hours in SQL."""
    ensure_duckdb_view_groups({"sales"})
    conn = get_duckdb_connection()

    query = """
    WITH hours AS (SELECT unnest(range(7, 24)) as hour),
    sales AS (
        SELECT 
            EXTRACT(HOUR FROM date)::INT as hour,
            SUM(revenue) as revenue,
            COALESCE(NULLIF(COUNT(DISTINCT txn_id), 0), COUNT(*)) as transactions
        FROM fact_sales_all
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
          AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23
        GROUP BY 1
    )
    SELECT h.hour, COALESCE(s.revenue, 0) as revenue, COALESCE(s.transactions, 0) as transactions
    FROM hours h LEFT JOIN sales s ON h.hour = s.hour
    ORDER BY h.hour
    """
    query_start = time.time()
    result = conn.execute(query, [target_date, target_date]).fetchdf()
    print(f"[TIMING] query_hourly_sales_pattern: {time.time() - query_start:.3f}s")
    return result


@versioned_cache(ttl=3600, key_prefix="top_products")
@lru_cache(maxsize=32)
def query_top_products(start_date: date, end_date: date, limit: int = 20) -> pd.DataFrame:
    """Query top products - optimized: aggregate by product_id first, then join."""
    ensure_duckdb_view_groups({"sales_agg", "dims"})
    conn = get_duckdb_connection()

    query = """
    WITH product_agg AS (
        SELECT 
            product_id,
            SUM(quantity) as quantity_sold,
            SUM(revenue) as total_unit_price
        FROM agg_sales_daily_by_product
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY product_id
        ORDER BY total_unit_price DESC
        LIMIT ?
    )
    SELECT 
        COALESCE(p.product_name, 'Product ' || s.product_id::VARCHAR) as product_name,
        COALESCE(p.product_category, 'Unknown Category') as category,
        s.quantity_sold,
        s.total_unit_price
    FROM product_agg s
    LEFT JOIN dim_products p ON s.product_id = p.product_id
    ORDER BY s.total_unit_price DESC
    """
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date, limit]).fetchdf()
    print(f"[TIMING] query_top_products: {time.time() - query_start:.3f}s")
    return result


@versioned_cache(ttl=3600, key_prefix="revenue_comparison")
@lru_cache(maxsize=32)
def query_revenue_comparison(start_date: date, end_date: date) -> Dict:
    """Compare revenue - SINGLE query for both periods using FILTER."""
    ensure_duckdb_view_groups({"sales_agg"})
    conn = get_duckdb_connection()

    period_days = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=period_days)
    prev_end = start_date - timedelta(days=1)

    query = """
    SELECT 
        SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as curr_rev,
        SUM(transactions) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as curr_txn,
        SUM(items_sold) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as curr_items,
        SUM(revenue) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as prev_rev,
        SUM(transactions) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as prev_txn,
        SUM(items_sold) FILTER (WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY) as prev_items
    FROM agg_sales_daily
    WHERE date >= ?::DATE AND date < ?::DATE + INTERVAL 1 DAY
    """
    
    params = [
        # Current period filters
        start_date, end_date,  # cur_rev
        start_date, end_date,  # cur_txn
        start_date, end_date,  # cur_items
        # Previous period filters
        prev_start, prev_end,  # prev_rev
        prev_start, prev_end,  # prev_txn
        prev_start, prev_end,  # prev_items
        # Overall filter
        prev_start, end_date
    ]

    query_start = time.time()
    row = conn.execute(query, params).fetchone()
    print(f"[TIMING] query_revenue_comparison: {time.time() - query_start:.3f}s")
    cur_rev, cur_txn, cur_items, prev_rev, prev_txn, prev_items = [v or 0 for v in row]

    # Calculate averages
    cur_atv = cur_rev / cur_txn if cur_txn else 0
    prev_atv = prev_rev / prev_txn if prev_txn else 0

    def calc_delta(cur: float, prev: float) -> tuple:
        delta = cur - prev
        pct = (delta / prev * 100) if prev else 0
        return delta, pct

    rev_d, rev_p = calc_delta(cur_rev, prev_rev)
    txn_d, txn_p = calc_delta(cur_txn, prev_txn)
    atv_d, atv_p = calc_delta(cur_atv, prev_atv)
    items_d, items_p = calc_delta(cur_items, prev_items)

    return {
        'current': {
            'revenue': cur_rev,
            'transactions': cur_txn,
            'items_sold': cur_items,
            'avg_transaction_value': cur_atv
        },
        'previous': {
            'revenue': prev_rev,
            'transactions': prev_txn,
            'items_sold': prev_items,
            'avg_transaction_value': prev_atv
        },
        'deltas': {
            'revenue': rev_d, 'revenue_pct': rev_p,
            'transactions': txn_d, 'transactions_pct': txn_p,
            'items_sold': items_d, 'items_sold_pct': items_p,
            'avg_transaction_value': atv_d, 'avg_transaction_value_pct': atv_p
        }
    }


def query_hourly_sales_heatmap(start_date: date, end_date: date) -> pd.DataFrame:
    """Query hourly heatmap data."""
    ensure_duckdb_view_groups({"sales"})
    conn = get_duckdb_connection()

    query = """
    SELECT
        date_trunc('day', date)::DATE as date,
        EXTRACT(HOUR FROM date)::INT as hour,
        SUM(revenue) as revenue
    FROM fact_sales_all
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
      AND EXTRACT(HOUR FROM date) BETWEEN 7 AND 23
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    query_start = time.time()
    result = conn.execute(query, [start_date, end_date]).fetchdf()
    print(f"[TIMING] query_hourly_sales_heatmap: {time.time() - query_start:.3f}s")
    return result


@lru_cache(maxsize=32)
def query_overview_summary(start_date: date, end_date: date) -> Dict:
    """Get overview summary - uses pre-aggregated data for speed."""
    ensure_duckdb_view_groups({"sales_agg", "dims"})
    conn = get_duckdb_connection()

    # Single combined query using pre-aggregated data
    query = """
    WITH base AS (
        SELECT a.product_id, a.revenue, a.quantity,
               COALESCE(p.product_parent_category, 'Unknown') as parent_cat,
               COALESCE(p.product_category, 'Unknown') as cat,
               COALESCE(p.product_brand, 'Unknown') as brand
        FROM agg_sales_daily_by_product a
        LEFT JOIN dim_products p ON a.product_id = p.product_id
        WHERE a.date >= ? AND a.date < ? + INTERVAL 1 DAY
    ),
    summary AS (SELECT SUM(revenue) as rev, SUM(quantity) as qty FROM base),
    by_cat AS (SELECT parent_cat, cat, SUM(revenue) as rev FROM base GROUP BY 1, 2),
    by_brand AS (SELECT parent_cat, cat, brand, SUM(revenue) as rev FROM base GROUP BY 1, 2, 3)
    SELECT 'summary' as type, NULL as c1, NULL as c2, NULL as c3, rev, qty FROM summary
    UNION ALL SELECT 'cat', parent_cat, cat, NULL, rev, NULL FROM by_cat
    UNION ALL SELECT 'brand', parent_cat, cat, brand, rev, NULL FROM by_brand
    """

    query_start = time.time()
    results = conn.execute(query, [start_date, end_date]).fetchall()
    print(f"[TIMING] query_overview_summary: {time.time() - query_start:.3f}s")

    categories_nested = {}
    brands_nested = {}
    total_rev = total_qty = 0

    for row in results:
        rtype, c1, c2, c3, rev, qty = row
        rev = float(rev or 0)
        
        if rtype == 'summary':
            total_rev, total_qty = rev, float(qty or 0)
        elif rtype == 'cat':
            categories_nested.setdefault(c1, {})[c2] = rev
        elif rtype == 'brand':
            brands_nested.setdefault(c1, {}).setdefault(c2, {})[c3] = rev

    return {
        'target_date_start': start_date,
        'target_date_end': end_date,
        'today_amount': total_rev,
        'today_qty': total_qty,
        'prev_amount': 0,
        'categories_nested': categories_nested,
        'brands_nested': brands_nested,
    }
