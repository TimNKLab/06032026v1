#!/usr/bin/env python3
"""
ETL Data Manager - GUI tool for detecting missing data and backfilling.

This script provides a unified interface to:
1. Scan data lake for missing/empty partitions across facts, dims, and aggregates
2. Select date ranges for backfill operations
3. Run incremental updates or full refreshes for selected datasets

Usage:
    python scripts/etl_data_manager.py

Supported datasets:
    Facts: POS Sales, Invoice Sales, Purchases, Inventory Moves, Stock Quants, Profit
    Dims:  Products, Categories, Brands, Cashiers, Taxes, Locations, UOMs, Partners
    Aggs:  Sales Aggregates (daily, by product, by principal), Profit Aggregates
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple, Callable
import threading
import json
import os
import sys
from pathlib import Path
import glob

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Lazy imports for GUI (tkinter not available in Docker)
tk = None
ttk = None
messagebox = None
scrolledtext = None

def _ensure_tk():
    """Lazy load tkinter only when GUI is needed."""
    global tk, ttk, messagebox, scrolledtext
    if tk is None:
        import tkinter as tk_module
        from tkinter import ttk as ttk_module, messagebox as msg_module, scrolledtext as scroll_module
        tk = tk_module
        ttk = ttk_module
        messagebox = msg_module
        scrolledtext = scroll_module

from etl import config as etl_config
from services.etl_ops import scan_dataset_partitions, scan_dimension_files, parse_date

# Import Docker Compose runner (for Docker-based ETL)
try:
    from services.docker_compose_runner import run_compose_exec_with_output
    DOCKER_RUNNER_AVAILABLE = True
except ImportError:
    DOCKER_RUNNER_AVAILABLE = False

# Import for MV scanning
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

# Import force refresh modules (for cascading backfill)
try:
    import force_refresh_pos_data
    import force_refresh_dimensions
    import force_refresh_stock_quants
    import force_refresh_purchase_data
    FORCE_REFRESH_AVAILABLE = True
except ImportError:
    FORCE_REFRESH_AVAILABLE = False

# Docker ETL configuration (to avoid Windows/DuckDB permission issues)
DOCKER_ETL_ENABLED = os.environ.get("ETL_DATA_MANAGER_USE_DOCKER") in {"1", "true", "True", "yes", "YES"}
DOCKER_ETL_SERVICE = os.environ.get("ETL_DATA_MANAGER_DOCKER_SERVICE", "celery-worker")

# =============================================================================
# Configuration
# =============================================================================

DATASET_GROUPS = {
    "Facts": {
        "pos": {"label": "POS Sales", "has_date_partitions": True},
        "invoice_sales": {"label": "Invoice Sales", "has_date_partitions": True},
        "purchases": {"label": "Purchase Invoices", "has_date_partitions": True},
        "inventory_moves": {"label": "Inventory Moves", "has_date_partitions": True},
        "stock_quants": {"label": "Stock Quants", "has_date_partitions": True},
        "profit": {"label": "Profit (Cost + Aggregates)", "has_date_partitions": True},
    },
    "Dimensions": {
        "dimensions": {"label": "All Dimensions", "has_date_partitions": False},
        "dim_products": {"label": "Products Only", "has_date_partitions": False},
        "dim_categories": {"label": "Categories Only", "has_date_partitions": False},
        "dim_brands": {"label": "Brands Only", "has_date_partitions": False},
    },
    "Aggregates": {
        "sales_aggregates": {"label": "Sales Aggregates", "has_date_partitions": True},
        "profit_aggregates": {"label": "Profit Aggregates", "has_date_partitions": True},
    },
}

DATASET_TO_SCAN_KEY = {
    "pos": "pos",
    "invoice_sales": "invoice_sales",
    "purchases": "purchases",
    "inventory_moves": "inventory_moves",
    "stock_quants": "stock_quants",
    "profit": "profit",
}

# =============================================================================
# Materialized View Configuration
# =============================================================================

MV_TO_PARQUET_MAP = {
    "mv_sales_daily": {
        "path": etl_config.AGG_SALES_DAILY_PATH,
        "date_column": "date",
        "description": "Daily sales aggregates",
    },
    "mv_sales_by_product": {
        "path": etl_config.AGG_SALES_DAILY_BY_PRODUCT_PATH,
        "date_column": "date",
        "description": "Sales by product",
    },
    "mv_sales_by_principal": {
        "path": etl_config.AGG_SALES_DAILY_BY_PRINCIPAL_PATH,
        "date_column": "date",
        "description": "Sales by principal/brand",
    },
    "mv_profit_daily": {
        "path": etl_config.AGG_PROFIT_DAILY_PATH,
        "date_column": "date",
        "description": "Daily profit aggregates",
    },
    "mv_inventory_daily": {
        "path": etl_config.STAR_SCHEMA_PATH + "/fact_stock_on_hand_snapshot",
        "date_column": "date",
        "description": "Daily inventory snapshots",
    },
}


# =============================================================================
# Data Scanner
# =============================================================================

class DataScanner:
    """Scans data lake for missing or empty partitions."""

    def __init__(self, root_path: str = None):
        self.root_path = root_path or etl_config.DATA_LAKE_ROOT

    def scan_facts(
        self, dataset_key: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """Scan fact tables for missing data across date range."""
        if dataset_key in DATASET_TO_SCAN_KEY:
            return scan_dataset_partitions(dataset_key, start_date, end_date)
        return []

    def scan_dimensions(self, dim_type: str = "all") -> List[Dict]:
        """Scan dimension files for existence and freshness."""
        dims_map = {
            "dimensions": ["products", "categories", "brands", "cashiers", "taxes"],
            "dim_products": ["products"],
            "dim_categories": ["categories"],
            "dim_brands": ["brands"],
        }
        targets = dims_map.get(dim_type, ["products", "categories", "brands", "cashiers", "taxes"])
        return scan_dimension_files(targets)

    def scan_aggregates(
        self, agg_type: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """Scan aggregate tables for missing data."""
        path_map = {
            "sales_aggregates": etl_config.AGG_SALES_DAILY_PATH,
            "profit_aggregates": etl_config.AGG_PROFIT_DAILY_PATH,
        }
        base_path = path_map.get(agg_type)
        if not base_path:
            return []

        results = []
        current = start_date
        while current <= end_date:
            date_str = current.isoformat()
            partition_path = os.path.join(base_path, f"date={date_str}")

            status = "Missing"
            records = 0
            if os.path.exists(partition_path):
                parquet_files = glob.glob(os.path.join(partition_path, "*.parquet"))
                if parquet_files:
                    try:
                        import polars as pl
                        df = pl.read_parquet(parquet_files[0])
                        records = len(df)
                        status = "OK" if records > 0 else "Empty"
                    except Exception:
                        status = "Error"
                else:
                    status = "Empty"

            results.append({
                "date": date_str,
                "partition_path": partition_path,
                "status": status,
                "records": records,
            })
            current += timedelta(days=1)

        return results

    def find_missing_date_ranges(
        self, results: List[Dict]
    ) -> List[Tuple[date, date]]:
        """Collapse consecutive missing days into date ranges."""
        missing_days = []
        for row in results:
            if row.get("status") in ("Missing", "Empty"):
                try:
                    missing_days.append(date.fromisoformat(row["date"]))
                except (ValueError, TypeError):
                    continue

        if not missing_days:
            return []

        sorted_days = sorted(set(missing_days))
        ranges = []
        start = sorted_days[0]
        prev = sorted_days[0]

        for day in sorted_days[1:]:
            if day == prev + timedelta(days=1):
                prev = day
                continue
            ranges.append((start, prev))
            start = day
            prev = day
        ranges.append((start, prev))

        return ranges


# =============================================================================
# Materialized View Scanner
# =============================================================================

class MVScanner:
    """Scans for differences between parquet data and materialized views."""

    def __init__(self, db_path: str = None):
        self.data_lake = etl_config.DATA_LAKE_ROOT
        self.db_path = db_path or f"{self.data_lake}/cache/nkdash.duckdb"
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def _get_connection(self) -> Optional[duckdb.DuckDBPyConnection]:
        """Get or create DuckDB connection."""
        if not DUCKDB_AVAILABLE:
            return None
        if self.conn is None:
            try:
                self.conn = duckdb.connect(database=self.db_path, read_only=True)
            except Exception:
                return None
        return self.conn

    def _close_connection(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _get_parquet_dates(self, base_path: str, start_date: date, end_date: date) -> Set[date]:
        """Get set of dates with parquet files."""
        dates_found = set()
        
        if not os.path.exists(base_path):
            return dates_found

        current = start_date
        while current <= end_date:
            year, month, day = current.strftime("%Y"), current.strftime("%m"), current.strftime("%d")
            partition_path = os.path.join(base_path, f"year={year}/month={month}/day={day}")
            
            if os.path.exists(partition_path):
                parquet_files = list(Path(partition_path).glob("*.parquet"))
                if parquet_files:
                    dates_found.add(current)
            
            current += timedelta(days=1)

        return dates_found

    def _get_mv_dates(self, mv_name: str, start_date: date, end_date: date) -> Set[date]:
        """Get set of dates in materialized view."""
        dates_found = set()
        
        conn = self._get_connection()
        if not conn:
            return dates_found
        
        try:
            # Check if MV exists
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{mv_name}'
            """).fetchone()
            
            if result[0] == 0:
                return dates_found
            
            rows = conn.execute(f"""
                SELECT DISTINCT date 
                FROM {mv_name} 
                WHERE date >= '{start_date}' AND date <= '{end_date}'
            """).fetchall()
            
            for row in rows:
                if row[0]:
                    dates_found.add(row[0])
        except Exception:
            pass
                    
        return dates_found

    def _get_mv_metadata(self, mv_name: str) -> Optional[Dict]:
        """Get metadata about materialized view."""
        conn = self._get_connection()
        if not conn:
            return None
        
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{mv_name}'
            """).fetchone()
            
            if result[0] == 0:
                return {"exists": False}
            
            result = conn.execute(f"""
                SELECT COUNT(*), MIN(date), MAX(date) FROM {mv_name}
            """).fetchone()
            
            refresh_info = None
            try:
                refresh_result = conn.execute(f"""
                    SELECT last_refresh_date, max_data_date, refresh_type
                    FROM mv_refresh_metadata
                    WHERE view_name = '{mv_name}'
                """).fetchone()
                if refresh_result:
                    refresh_info = {
                        "last_refresh_date": str(refresh_result[0]) if refresh_result[0] else None,
                        "max_data_date": str(refresh_result[1]) if refresh_result[1] else None,
                        "refresh_type": refresh_result[2],
                    }
            except Exception:
                pass
            
            return {
                "exists": True,
                "row_count": result[0],
                "min_date": str(result[1]) if result[1] else None,
                "max_date": str(result[2]) if result[2] else None,
                "refresh_info": refresh_info,
            }
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def scan_mv_differences(
        self, start_date: date, end_date: date, progress_callback: Callable = None
    ) -> Dict[str, Dict]:
        """Scan all MVs and compare with their source parquet data."""
        results = {}
        
        for mv_name, config in MV_TO_PARQUET_MAP.items():
            if progress_callback:
                progress_callback(f"Checking {mv_name}...")
            
            parquet_path = config["path"]
            description = config["description"]
            
            parquet_dates = self._get_parquet_dates(parquet_path, start_date, end_date)
            mv_dates = self._get_mv_dates(mv_name, start_date, end_date)
            mv_metadata = self._get_mv_metadata(mv_name)
            
            missing_in_mv = parquet_dates - mv_dates
            missing_in_parquet = mv_dates - parquet_dates
            
            if not mv_metadata or not mv_metadata.get("exists"):
                status = "MISSING_MV"
            elif not missing_in_mv and not missing_in_parquet:
                status = "SYNCED"
            elif missing_in_mv and not missing_in_parquet:
                status = "STALE_MV"
            elif missing_in_parquet and not missing_in_mv:
                status = "ORPHANED_MV"
            else:
                status = "MIXED"
            
            results[mv_name] = {
                "description": description,
                "parquet_path": parquet_path,
                "status": status,
                "parquet_dates_count": len(parquet_dates),
                "mv_dates_count": len(mv_dates),
                "missing_in_mv_count": len(missing_in_mv),
                "missing_in_mv": sorted([d.isoformat() for d in missing_in_mv]),
                "missing_in_parquet": sorted([d.isoformat() for d in missing_in_parquet]),
                "mv_metadata": mv_metadata,
            }
        
        self._close_connection()
        return results


# =============================================================================
# Backfill Runner
# =============================================================================

class BackfillRunner:
    """Executes backfill operations for selected datasets."""

    def __init__(self, log_callback: Callable[[str], None] = None):
        self.log_callback = log_callback or print
        self._stop_requested = False

    def log(self, message: str):
        """Send log message to callback."""
        if self.log_callback:
            self.log_callback(message)

    def stop(self):
        """Request stop of current operation."""
        self._stop_requested = True
        self.log("Stop requested...")

    def reset_stop(self):
        """Reset stop flag."""
        self._stop_requested = False

    def refresh_materialized_views(self, views: Set[str]) -> Dict:
        """Refresh materialized views by loading data from parquet."""
        self.log(f"Refreshing materialized views: {sorted(views)}")
        
        if not DUCKDB_AVAILABLE:
            return {"success": 0, "failed": 1, "errors": ["duckdb not available"]}
        
        from services.duckdb_connector import DuckDBManager
        
        results = {"success": 0, "failed": 0, "errors": []}
        
        try:
            # MV loading removed - system now queries parquet files directly via DuckDB views
            results["success"] = len(views)
            self.log(f"  Skipping MV refresh - using DuckDB views over parquet")
            
        except Exception as e:
            results["failed"] = len(views)
            results["errors"].append(str(e))
            self.log(f"  Failed: {e}")
        
        return results

    def backfill_facts(
        self,
        dataset_key: str,
        start_date: date,
        end_date: date,
        refresh_dims: bool = False,
    ) -> Dict:
        """Backfill fact data for date range using force refresh scripts."""
        self.log(f"Backfilling {dataset_key} from {start_date} to {end_date}")

        if not FORCE_REFRESH_AVAILABLE:
            return {"success": 0, "failed": 1, "errors": ["Force refresh modules not available"]}

        results = {"success": 0, "failed": 0, "errors": []}

        # Map dataset keys to force refresh targets
        dataset_to_script = {
            "pos": ("pos", force_refresh_pos_data),
            "invoice_sales": ("invoice-sales", force_refresh_pos_data),
            "purchases": (None, force_refresh_purchase_data),
            "inventory_moves": ("inventory-moves", force_refresh_pos_data),
            "stock_quants": (None, force_refresh_stock_quants),
        }

        try:
            if dataset_key == "profit":
                # Profit uses run_profit_etl pattern - handle separately
                results = self._backfill_profit(start_date, end_date, refresh_dims)
            elif dataset_key in dataset_to_script:
                target, script_module = dataset_to_script[dataset_key]

                # Refresh dimensions if needed (for inventory/stock)
                if refresh_dims and dataset_key in ("inventory_moves", "stock_quants"):
                    self.log("  Refreshing dimensions first...")
                    try:
                        force_refresh_dimensions.main(["--targets", "products", "locations", "lots"])
                    except SystemExit:
                        pass  # argparse calls sys.exit()

                # Build args for force refresh script
                args = ["--start", start_date.isoformat()]
                if start_date != end_date:
                    args.extend(["--end", end_date.isoformat()])
                if target:
                    args.extend(["--targets", target])

                self.log(f"  Running: {script_module.__name__} {' '.join(args)}")

                try:
                    exit_code = script_module.main(args)
                    if exit_code == 0 or exit_code is None:
                        # Estimate days processed
                        days = (end_date - start_date).days + 1
                        results["success"] = days
                    else:
                        results["failed"] = 1
                        results["errors"].append(f"Script exited with code {exit_code}")
                except SystemExit as e:
                    # argparse calls sys.exit()
                    if e.code == 0 or e.code is None:
                        days = (end_date - start_date).days + 1
                        results["success"] = days
                    else:
                        results["failed"] = 1
                        results["errors"].append(f"Script exited with code {e.code}")
                except Exception as e:
                    results["failed"] = 1
                    results["errors"].append(str(e))
            else:
                results["errors"].append(f"Unknown dataset: {dataset_key}")

        except Exception as e:
            results["errors"].append(str(e))
            results["failed"] = 1

        return results

    def _backfill_profit(self, start_date: date, end_date: date, refresh_dims: bool) -> Dict:
        """Backfill profit data using run_profit_etl pattern."""
        results = {"success": 0, "failed": 0, "errors": []}

        try:
            from etl_tasks import (
                update_product_cost_events,
                update_product_cost_latest_daily,
                update_sales_lines_profit,
                update_profit_aggregates,
                refresh_dimensions_incremental,
            )

            current = start_date
            while current <= end_date and not self._stop_requested:
                date_str = current.isoformat()
                self.log(f"  Processing profit for {date_str}...")

                try:
                    if refresh_dims:
                        refresh_dimensions_incremental(["products", "locations", "lots"])

                    update_product_cost_events(date_str)
                    update_product_cost_latest_daily(date_str)
                    update_sales_lines_profit(date_str)
                    update_profit_aggregates(date_str)

                    results["success"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"{date_str}: {e}")
                current += timedelta(days=1)

        except ImportError as e:
            results["errors"].append(f"Could not import profit ETL tasks: {e}")
            results["failed"] = 1

        return results

    def backfill_dimensions(self, dim_type: str = "all") -> Dict:
        """Backfill dimension tables."""
        self.log(f"Backfilling dimensions: {dim_type}")

        from scripts.force_refresh_dimensions import main as refresh_dims_main

        targets_map = {
            "dim_products": ["products"],
            "dim_categories": ["categories"],
            "dim_brands": ["brands"],
        }

        try:
            targets = targets_map.get(dim_type)
            if targets:
                import sys
                old_argv = sys.argv
                sys.argv = ["force_refresh_dimensions.py", "--targets"] + targets
                try:
                    refresh_dims_main()
                finally:
                    sys.argv = old_argv
            else:
                refresh_dims_main()

            return {"success": 1, "failed": 0, "errors": []}
        except Exception as e:
            return {"success": 0, "failed": 1, "errors": [str(e)]}

    def backfill_aggregates(
        self, agg_type: str, start_date: date, end_date: date
    ) -> Dict:
        """Backfill aggregate tables for date range."""
        self.log(f"Backfilling {agg_type} from {start_date} to {end_date}")

        if agg_type == "sales_aggregates":
            from scripts.backfill_sales_aggregates import main as backfill_sales_main
            import sys

            old_argv = sys.argv
            sys.argv = [
                "backfill_sales_aggregates.py",
                "--start",
                start_date.isoformat(),
                "--end",
                end_date.isoformat(),
            ]
            try:
                backfill_sales_main()
                return {"success": 1, "failed": 0, "errors": []}
            except Exception as e:
                return {"success": 0, "failed": 1, "errors": [str(e)]}
            finally:
                sys.argv = old_argv

        elif agg_type == "profit_aggregates":
            from etl_tasks import update_profit_aggregates

            results = {"success": 0, "failed": 0, "errors": []}
            current = start_date

            while current <= end_date and not self._stop_requested:
                date_str = current.isoformat()
                try:
                    update_profit_aggregates(date_str)
                    results["success"] += 1
                    self.log(f"  {date_str}: OK")
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"{date_str}: {e}")
                    self.log(f"  {date_str}: FAILED - {e}")
                current += timedelta(days=1)

            return results

        return {"success": 0, "failed": 0, "errors": [f"Unknown aggregate type: {agg_type}"]}

    def _scan_parquet_availability(
        self,
        dataset_key: str,
        start_date: date,
        end_date: date,
    ) -> Dict:
        """Scan for missing parquet data and return dates that need fetching."""
        from etl import config as etl_config
        from pathlib import Path

        # Map dataset keys to parquet paths (using clean/fact paths from config)
        path_map = {
            "pos": etl_config.CLEAN_PATH,  # POS sales clean data
            "invoice_sales": etl_config.CLEAN_SALES_INVOICE_PATH,
            "purchases": etl_config.CLEAN_PURCHASES_PATH,
            "inventory_moves": etl_config.CLEAN_INVENTORY_MOVES_PATH,
            "stock_quants": etl_config.CLEAN_STOCK_QUANTS_PATH,
            "profit": etl_config.FACT_SALES_LINES_PROFIT_PATH,
        }

        base_path = path_map.get(dataset_key)
        if not base_path:
            return {"available": [], "missing": [], "error": f"Unknown dataset: {dataset_key}"}

        available_dates = []
        missing_dates = []

        current = start_date
        while current <= end_date:
            year, month, day = current.strftime("%Y"), current.strftime("%m"), current.strftime("%d")
            partition_path = Path(base_path) / f"year={year}" / f"month={month}" / f"day={day}"

            # Check if partition has parquet files with data
            if partition_path.exists():
                parquet_files = list(partition_path.glob("*.parquet"))
                if parquet_files:
                    # Verify file has data (not empty)
                    try:
                        import polars as pl
                        df = pl.read_parquet(parquet_files[0])
                        if len(df) > 0:
                            available_dates.append(current)
                        else:
                            missing_dates.append(current)
                    except:
                        missing_dates.append(current)
                else:
                    missing_dates.append(current)
            else:
                missing_dates.append(current)

            current += timedelta(days=1)

        return {
            "dataset": dataset_key,
            "available": available_dates,
            "missing": missing_dates,
            "available_count": len(available_dates),
            "missing_count": len(missing_dates),
        }

    def _auto_fetch_missing_data(
        self,
        dataset_key: str,
        missing_dates: List[date],
        refresh_dims: bool = False,
    ) -> Dict:
        """Fetch missing raw data from Odoo using force refresh scripts."""
        if not missing_dates:
            return {"success": 0, "failed": 0, "errors": [], "fetched": []}

        self.log(f"  Auto-fetching {len(missing_dates)} missing date(s) for {dataset_key}...")

        # Group consecutive dates into ranges for efficient fetching
        date_ranges = self._group_dates_into_ranges(missing_dates)

        results = {"success": 0, "failed": 0, "errors": [], "fetched": []}

        for range_start, range_end in date_ranges:
            self.log(f"    Fetching {range_start} to {range_end}...")

            # Use backfill_facts to fetch (this calls force refresh scripts)
            fetch_results = self.backfill_facts(
                dataset_key=dataset_key,
                start_date=range_start,
                end_date=range_end,
                refresh_dims=refresh_dims,
            )

            results["success"] += fetch_results.get("success", 0)
            results["failed"] += fetch_results.get("failed", 0)
            results["errors"].extend(fetch_results.get("errors", []))
            results["fetched"].append((range_start, range_end))

        return results

    def _group_dates_into_ranges(self, dates: List[date]) -> List[Tuple[date, date]]:
        """Group consecutive dates into (start, end) tuples for efficient batching."""
        if not dates:
            return []

        sorted_dates = sorted(dates)
        ranges = []
        start = sorted_dates[0]
        prev = sorted_dates[0]

        for d in sorted_dates[1:]:
            if d == prev + timedelta(days=1):
                prev = d
            else:
                ranges.append((start, prev))
                start = d
                prev = d
        ranges.append((start, prev))

        return ranges

    def refresh_materialized_views_cascading(
        self,
        views: Set[str],
        start_date: date = None,
        end_date: date = None,
        auto_fetch: bool = True,
        refresh_dims: bool = False,
    ) -> Dict:
        """
        Refresh materialized views with automatic raw data fetching.

        Flow:
        1. Scan parquet data availability for each view's source
        2. If missing data and auto_fetch=True, fetch from Odoo
        3. Build aggregates if needed
        4. Load into DuckDB MVs
        """
        self.log(f"Cascading MV refresh: {sorted(views)}")

        if not DUCKDB_AVAILABLE:
            return {"success": 0, "failed": 1, "errors": ["duckdb not available"], "fetched": []}

        results = {
            "success": 0,
            "failed": 0,
            "errors": [],
            "views_built": [],
            "fetched": [],
            "aggregates_built": [],
        }

        try:
            # Map views to their source datasets
            view_to_dataset = {
                "mv_sales_daily": "pos",
                "mv_sales_by_product": "pos",
                "mv_sales_by_principal": "pos",
                "mv_profit_daily": "profit",
                "mv_inventory_daily": "stock_quants",
            }

            # Step 1: Check and fetch missing raw data
            if auto_fetch and start_date and end_date:
                for view in views:
                    dataset_key = view_to_dataset.get(view)
                    if not dataset_key:
                        continue

                    self.log(f"  Checking data for {view} (source: {dataset_key})...")

                    # Scan availability
                    availability = self._scan_parquet_availability(
                        dataset_key, start_date, end_date
                    )

                    if availability["missing_count"] > 0:
                        missing_dates = availability["missing"]
                        self.log(f"    Missing {len(missing_dates)} date(s)")

                        # Auto-fetch missing data
                        fetch_results = self._auto_fetch_missing_data(
                            dataset_key, missing_dates, refresh_dims
                        )

                        results["fetched"].append({
                            "view": view,
                            "dataset": dataset_key,
                            "dates_fetched": fetch_results["fetched"],
                            "success": fetch_results["success"],
                            "failed": fetch_results["failed"],
                        })

                        if fetch_results["errors"]:
                            results["errors"].extend(fetch_results["errors"])

            # Step 2: Build aggregates if needed (for views that depend on them)
            aggregate_views = {"mv_sales_daily", "mv_sales_by_product", "mv_sales_by_principal"}
            profit_aggregate_views = {"mv_profit_daily"}

            if views & aggregate_views and start_date and end_date:
                self.log("  Building sales aggregates...")
                agg_results = self.backfill_aggregates("sales_aggregates", start_date, end_date)
                results["aggregates_built"].append({"type": "sales", **agg_results})

            if views & profit_aggregate_views and start_date and end_date:
                self.log("  Building profit aggregates...")
                agg_results = self.backfill_aggregates("profit_aggregates", start_date, end_date)
                results["aggregates_built"].append({"type": "profit", **agg_results})

            # Step 3: Signal dash-app to reload MVs from parquet.
            # If running in dash-app process (CELERY_WORKER_RUNNING != 1), reload directly.
            # If running in celery-worker, just set the Redis signal — dash-app will reload on next query.
            self.log("  Signalling MV reload...")

            in_celery = os.environ.get('CELERY_WORKER_RUNNING') == '1'

            if not in_celery:
                # Running in dash-app — MV loading removed, using DuckDB views
                results["views_built"] = list(views)
                results["success"] = len(views)
                self.log(f"  Skipping MV reload - using DuckDB views over parquet")
            else:
                # Running in celery-worker — parquet is written, signal dash-app
                results["views_built"] = list(views)
                results["success"] = len(views)
                self.log(f"  Parquet aggregates written. DuckDB views will query parquet directly.")

            # Cache clearing no longer needed - cache decorators removed from query functions
            # SQLite MVs are fast enough (0.004s - 0.005s) without cache

        except Exception as e:
            results["errors"].append(str(e))
            results["failed"] = len(views)
            self.log(f"  Cascading refresh error: {e}")

        return results

    def validate_profit(
        self,
        start_date: date,
        end_date: date,
    ) -> Dict:
        """Validate profit calculations for date range."""
        self.log(f"Validating profit from {start_date} to {end_date}")

        results = {
            "success": 0,
            "failed": 0,
            "errors": [],
            "validation_results": [],
        }

        try:
            import polars as pl
            from etl import config as etl_config

            # Check fact_sales_lines_profit exists and has data
            profit_path = etl_config.FACT_SALES_LINES_PROFIT_PATH

            current = start_date
            while current <= end_date and not self._stop_requested:
                date_str = current.isoformat()
                year, month, day = date_str.split("-")

                partition_path = Path(profit_path) / f"year={year}" / f"month={month}" / f"day={day}"

                try:
                    if not partition_path.exists():
                        results["validation_results"].append({
                            "date": date_str,
                            "status": "MISSING",
                            "records": 0,
                            "issues": ["No profit data partition"],
                        })
                        results["failed"] += 1
                    else:
                        # Load and validate
                        parquet_files = list(partition_path.glob("*.parquet"))
                        if not parquet_files:
                            results["validation_results"].append({
                                "date": date_str,
                                "status": "EMPTY",
                                "records": 0,
                                "issues": ["No parquet files"],
                            })
                            results["failed"] += 1
                        else:
                            df = pl.read_parquet(parquet_files[0])
                            record_count = len(df)

                            # Validate columns
                            required_cols = ["product_id", "revenue_tax_in", "cogs_tax_in", "gross_profit"]
                            missing_cols = [c for c in required_cols if c not in df.columns]

                            # Check for nulls in key columns
                            null_issues = []
                            if "gross_profit" in df.columns:
                                null_profit = df["gross_profit"].is_null().sum()
                                if null_profit > 0:
                                    null_issues.append(f"{null_profit} null gross_profit values")

                            # Check for negative COGS (should be positive for normal sales)
                            negative_cogs_issues = []
                            if "cogs_tax_in" in df.columns:
                                negative_cogs = (df["cogs_tax_in"] < 0).sum()
                                if negative_cogs > 0:
                                    negative_cogs_issues.append(f"{negative_cogs} negative COGS values")

                            issues = missing_cols + null_issues + negative_cogs_issues

                            status = "VALID" if not issues else "INVALID"

                            results["validation_results"].append({
                                "date": date_str,
                                "status": status,
                                "records": record_count,
                                "issues": issues,
                            })

                            if status == "VALID":
                                results["success"] += 1
                            else:
                                results["failed"] += 1

                            self.log(f"  {date_str}: {status} ({record_count} records, {len(issues)} issues)")

                except Exception as e:
                    results["errors"].append(f"{date_str}: {e}")
                    results["failed"] += 1

                current += timedelta(days=1)

        except ImportError as e:
            results["errors"].append(f"Could not import required modules: {e}")
            results["failed"] = 1

        return results


# =============================================================================
# GUI Application
# =============================================================================

class ETLDataManagerApp:
    """Tkinter GUI for ETL Data Manager."""

    def __init__(self, root):
        _ensure_tk()  # Lazy load tkinter
        self.root = root
        self.root.title("ETL Data Manager")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        self.scanner = DataScanner()
        self.runner = BackfillRunner(log_callback=self.log)

        self._scan_results: Dict[str, List[Dict]] = {}
        self._running = False

        self._build_ui()
        self._load_defaults()

    def _build_ui(self):
        """Build the user interface."""
        # Main container with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)  # Log area expands

        # Title
        title = ttk.Label(
            main_frame,
            text="ETL Data Manager",
            font=("Helvetica", 16, "bold"),
        )
        title.grid(row=0, column=0, sticky="w", pady=(0, 10))

        # Top section: Date range and datasets
        top_frame = ttk.LabelFrame(main_frame, text="Scan & Backfill Settings", padding="10")
        top_frame.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        top_frame.columnconfigure(1, weight=1)

        # Date range
        ttk.Label(top_frame, text="Start Date:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.start_date_entry = ttk.Entry(top_frame, width=12)
        self.start_date_entry.grid(row=0, column=1, sticky="w", padx=5, pady=5)

        ttk.Label(top_frame, text="End Date:").grid(row=0, column=2, sticky="w", padx=5, pady=5)
        self.end_date_entry = ttk.Entry(top_frame, width=12)
        self.end_date_entry.grid(row=0, column=3, sticky="w", padx=5, pady=5)

        ttk.Button(top_frame, text="Set Today", command=self._set_today).grid(
            row=0, column=4, padx=5, pady=5
        )
        ttk.Button(top_frame, text="Last 7 Days", command=self._set_last_7).grid(
            row=0, column=5, padx=5, pady=5
        )
        ttk.Button(top_frame, text="Last 30 Days", command=self._set_last_30).grid(
            row=0, column=6, padx=5, pady=5
        )

        # Dataset selection
        ttk.Label(top_frame, text="Datasets:").grid(row=1, column=0, sticky="nw", padx=5, pady=5)

        datasets_frame = ttk.Frame(top_frame)
        datasets_frame.grid(row=1, column=1, columnspan=6, sticky="ew", padx=5, pady=5)

        self.dataset_vars: Dict[str, tk.BooleanVar] = {}
        col = 0
        for group_name, datasets in DATASET_GROUPS.items():
            group_frame = ttk.LabelFrame(datasets_frame, text=group_name, padding="5")
            group_frame.grid(row=0, column=col, sticky="nw", padx=5, pady=5)

            for key, info in datasets.items():
                var = tk.BooleanVar(value=False)
                self.dataset_vars[key] = var
                cb = ttk.Checkbutton(group_frame, text=info["label"], variable=var)
                cb.pack(anchor="w")

            col += 1

        # Refresh dimensions checkbox
        self.refresh_dims_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            top_frame,
            text="Refresh dimensions first (for inventory/stock/profit datasets)",
            variable=self.refresh_dims_var,
        ).grid(row=2, column=1, columnspan=6, sticky="w", padx=5, pady=5)

        # Action buttons
        buttons_frame = ttk.Frame(top_frame)
        buttons_frame.grid(row=3, column=0, columnspan=7, sticky="ew", pady=(10, 0))

        ttk.Button(
            buttons_frame, text="Scan for Missing Data", command=self._scan, width=25
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            buttons_frame, text="Backfill Selected", command=self._backfill, width=25
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            buttons_frame, text="Stop", command=self._stop, width=15
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            buttons_frame, text="Clear Log", command=self._clear_log, width=15
        ).pack(side=tk.RIGHT, padx=5)

        # Separator
        ttk.Separator(top_frame, orient="horizontal").grid(
            row=4, column=0, columnspan=7, sticky="ew", pady=10
        )

        # MV Operations frame
        mv_frame = ttk.LabelFrame(top_frame, text="Materialized Views (Query Acceleration)", padding="5")
        mv_frame.grid(row=5, column=0, columnspan=7, sticky="ew", pady=(0, 5))

        ttk.Button(
            mv_frame, text="Scan MV vs Parquet", command=self._scan_mv_differences, width=18
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            mv_frame, text="Refresh All MVs", command=self._refresh_all_mvs, width=18
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            mv_frame, text="Refresh Selected MVs", command=self._refresh_selected_mvs, width=18
        ).pack(side=tk.LEFT, padx=5)
        ttk.Separator(mv_frame, orient="vertical").pack(side=tk.LEFT, padx=10, fill="y")
        ttk.Button(
            mv_frame, text="Validate Profit", command=self._validate_profit, width=18
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            mv_frame, text="Build Aggregates", command=self._build_aggregates, width=18
        ).pack(side=tk.LEFT, padx=5)

        # Results section
        results_frame = ttk.LabelFrame(main_frame, text="Scan Results", padding="10")
        results_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 10))
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)

        # Treeview for results
        columns = ("dataset", "date_range", "missing_days", "status")
        self.results_tree = ttk.Treeview(
            results_frame, columns=columns, show="headings", height=8
        )

        self.results_tree.heading("dataset", text="Dataset")
        self.results_tree.heading("date_range", text="Date Range / Details")
        self.results_tree.heading("missing_days", text="Missing")
        self.results_tree.heading("status", text="Status")

        self.results_tree.column("dataset", width=250)
        self.results_tree.column("date_range", width=250)
        self.results_tree.column("missing_days", width=80)
        self.results_tree.column("status", width=120)

        scrollbar = ttk.Scrollbar(results_frame, orient="vertical", command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=scrollbar.set)

        self.results_tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        # Log section
        log_frame = ttk.LabelFrame(main_frame, text="Operation Log", padding="10")
        log_frame.grid(row=3, column=0, sticky="nsew", pady=(0, 10))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, wrap=tk.WORD, height=10, state="disabled"
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")

        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=4, column=0, sticky="ew")

    def _load_defaults(self):
        """Load default date values."""
        today = date.today()
        self.end_date_entry.insert(0, today.isoformat())
        start = today - timedelta(days=7)
        self.start_date_entry.insert(0, start.isoformat())

    def _set_today(self):
        """Set both dates to today."""
        today = date.today().isoformat()
        self.start_date_entry.delete(0, tk.END)
        self.start_date_entry.insert(0, today)
        self.end_date_entry.delete(0, tk.END)
        self.end_date_entry.insert(0, today)

    def _set_last_7(self):
        """Set date range to last 7 days."""
        today = date.today()
        start = today - timedelta(days=7)
        self.start_date_entry.delete(0, tk.END)
        self.start_date_entry.insert(0, start.isoformat())
        self.end_date_entry.delete(0, tk.END)
        self.end_date_entry.insert(0, today.isoformat())

    def _set_last_30(self):
        """Set date range to last 30 days."""
        today = date.today()
        start = today - timedelta(days=30)
        self.start_date_entry.delete(0, tk.END)
        self.start_date_entry.insert(0, start.isoformat())
        self.end_date_entry.delete(0, tk.END)
        self.end_date_entry.insert(0, today.isoformat())

    def _get_date_range(self) -> Optional[Tuple[date, date]]:
        """Parse and validate date range from entries."""
        try:
            start = date.fromisoformat(self.start_date_entry.get().strip())
            end = date.fromisoformat(self.end_date_entry.get().strip())

            if end < start:
                start, end = end, start

            # Limit to 90 days for safety
            if (end - start).days > 90:
                messagebox.showwarning(
                    "Date Range Too Large",
                    "Date range limited to 90 days for safety. Please use a smaller range.",
                )
                return None

            return start, end
        except ValueError:
            messagebox.showerror(
                "Invalid Date",
                "Please enter valid dates in YYYY-MM-DD format.",
            )
            return None

    def _get_selected_datasets(self) -> List[str]:
        """Get list of selected dataset keys."""
        return [key for key, var in self.dataset_vars.items() if var.get()]

    def log(self, message: str):
        """Add message to log."""
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _clear_log(self):
        """Clear the log area."""
        self.log_text.configure(state="normal")
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state="disabled")

    def _scan(self):
        """Scan for missing data."""
        if self._running:
            messagebox.showwarning("Busy", "Another operation is already running.")
            return

        date_range = self._get_date_range()
        if not date_range:
            return

        start_date, end_date = date_range
        selected = self._get_selected_datasets()

        if not selected:
            # If no datasets selected, scan all fact datasets
            selected = list(DATASET_GROUPS["Facts"].keys())
            self.log("No datasets selected - scanning all facts by default")

        self._running = True
        self.status_var.set("Scanning...")
        self._clear_results()

        def do_scan():
            try:
                self.log(f"Scanning from {start_date} to {end_date}")
                self._scan_results = {}

                for dataset_key in selected:
                    if dataset_key in DATASET_GROUPS["Facts"]:
                        self.log(f"Scanning {dataset_key}...")
                        results = self.scanner.scan_facts(dataset_key, start_date, end_date)
                        self._scan_results[dataset_key] = results

                        missing_ranges = self.scanner.find_missing_date_ranges(results)
                        missing_count = sum(
                            1 for r in results if r.get("status") in ("Missing", "Empty")
                        )

                        range_str = f"{start_date} to {end_date}"
                        if missing_ranges:
                            range_str = ", ".join(
                                f"{s} to {e}" for s, e in missing_ranges
                            )

                        status = f"OK" if missing_count == 0 else f"{missing_count} missing"

                        self.root.after(0, lambda k=dataset_key, rs=range_str, mc=missing_count, st=status: self._add_result(k, rs, mc, st))

                    elif dataset_key in DATASET_GROUPS["Dimensions"]:
                        self.log(f"Scanning dimensions ({dataset_key})...")
                        results = self.scanner.scan_dimensions(dataset_key)
                        self._scan_results[dataset_key] = results

                        missing_count = sum(1 for r in results if r.get("status") != "OK")
                        status = f"OK" if missing_count == 0 else f"{missing_count} missing"

                        self.root.after(0, lambda k=dataset_key, rs="N/A", mc=missing_count, st=status: self._add_result(k, rs, mc, st))

                    elif dataset_key in DATASET_GROUPS["Aggregates"]:
                        self.log(f"Scanning {dataset_key}...")
                        results = self.scanner.scan_aggregates(dataset_key, start_date, end_date)
                        self._scan_results[dataset_key] = results

                        missing_ranges = self.scanner.find_missing_date_ranges(results)
                        missing_count = sum(
                            1 for r in results if r.get("status") in ("Missing", "Empty")
                        )

                        range_str = f"{start_date} to {end_date}"
                        if missing_ranges:
                            range_str = ", ".join(
                                f"{s} to {e}" for s, e in missing_ranges
                            )

                        status = f"OK" if missing_count == 0 else f"{missing_count} missing"

                        self.root.after(0, lambda k=dataset_key, rs=range_str, mc=missing_count, st=status: self._add_result(k, rs, mc, st))

                self.log("Scan complete")
                self.root.after(0, lambda: self.status_var.set("Scan complete"))

            except Exception as e:
                self.log(f"Scan error: {e}")
                self.root.after(0, lambda: self.status_var.set(f"Error: {e}"))

            finally:
                self._running = False

        threading.Thread(target=do_scan, daemon=True).start()

    def _add_result(self, dataset: str, date_range: str, missing_days: int, status: str):
        """Add a result row to the treeview."""
        label = self._get_dataset_label(dataset)
        self.results_tree.insert(
            "", tk.END, values=(label, date_range, str(missing_days), status)
        )

    def _get_dataset_label(self, key: str) -> str:
        """Get human-readable label for dataset key."""
        for group in DATASET_GROUPS.values():
            if key in group:
                return group[key]["label"]
        return key

    def _clear_results(self):
        """Clear results treeview."""
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)

    def _backfill(self):
        """Run backfill for selected datasets."""
        if self._running:
            messagebox.showwarning("Busy", "Another operation is already running.")
            return

        date_range = self._get_date_range()
        if not date_range:
            return

        start_date, end_date = date_range
        selected = self._get_selected_datasets()

        if not selected:
            messagebox.showwarning("No Selection", "Please select at least one dataset to backfill.")
            return

        # Confirm with user
        dataset_labels = [self._get_dataset_label(k) for k in selected]
        confirm_msg = (
            f"Backfill the following datasets from {start_date} to {end_date}?\n\n"
            + "\n".join(f"  - {label}" for label in dataset_labels)
        )

        if not messagebox.askyesno("Confirm Backfill", confirm_msg):
            return

        self._running = True
        self.status_var.set("Backfilling...")
        self.runner.reset_stop()

        refresh_dims = self.refresh_dims_var.get()

        def do_backfill():
            try:
                total_success = 0
                total_failed = 0

                for dataset_key in selected:
                    if self.runner._stop_requested:
                        self.log("Backfill stopped by user")
                        break

                    self.log(f"\nBackfilling {dataset_key}...")

                    if dataset_key in DATASET_GROUPS["Facts"]:
                        results = self.runner.backfill_facts(
                            dataset_key, start_date, end_date, refresh_dims
                        )
                    elif dataset_key in DATASET_GROUPS["Dimensions"]:
                        results = self.runner.backfill_dimensions(dataset_key)
                    elif dataset_key in DATASET_GROUPS["Aggregates"]:
                        results = self.runner.backfill_aggregates(
                            dataset_key, start_date, end_date
                        )
                    else:
                        self.log(f"Unknown dataset type: {dataset_key}")
                        continue

                    total_success += results.get("success", 0)
                    total_failed += results.get("failed", 0)

                    for error in results.get("errors", []):
                        self.log(f"  Error: {error}")

                self.log(f"\nBackfill complete: {total_success} success, {total_failed} failed")
                self.root.after(0, lambda: self.status_var.set("Backfill complete"))

                if total_failed == 0:
                    self.root.after(
                        0, lambda: messagebox.showinfo("Success", "Backfill completed successfully!")
                    )
                else:
                    self.root.after(
                        0,
                        lambda: messagebox.showwarning(
                            "Completed with Errors",
                            f"Backfill completed with {total_failed} error(s). Check the log for details.",
                        ),
                    )

            except Exception as e:
                self.log(f"Backfill error: {e}")
                self.root.after(0, lambda: self.status_var.set(f"Error: {e}"))
                self.root.after(
                    0, lambda: messagebox.showerror("Error", f"Backfill failed: {e}")
                )

            finally:
                self._running = False

        threading.Thread(target=do_backfill, daemon=True).start()

    def _scan_mv_differences(self):
        """Scan for differences between parquet data and materialized views."""
        if self._running:
            messagebox.showwarning("Busy", "Another operation is already running.")
            return

        if not DUCKDB_AVAILABLE:
            messagebox.showerror(
                "DuckDB Not Available",
                "DuckDB is not installed. Cannot scan materialized views."
            )
            return

        date_range = self._get_date_range()
        if not date_range:
            return

        start_date, end_date = date_range
        self._running = True
        self.status_var.set("Scanning MV differences...")
        self._clear_results()

        def do_mv_scan():
            try:
                self.log(f"Scanning MV differences from {start_date} to {end_date}")
                
                scanner = MVScanner()
                results = scanner.scan_mv_differences(
                    start_date, end_date, 
                    progress_callback=lambda msg: self.log(f"  {msg}")
                )
                
                # Display results
                for mv_name, result in results.items():
                    status = result["status"]
                    description = result["description"]
                    parquet_count = result["parquet_dates_count"]
                    mv_count = result["mv_dates_count"]
                    missing_count = result["missing_in_mv_count"]
                    
                    # Build details string
                    if status == "SYNCED":
                        details = f"Parquet: {parquet_count}, MV: {mv_count}"
                    elif status == "MISSING_MV":
                        details = f"MV does not exist (parquet has {parquet_count} dates)"
                    elif status == "STALE_MV":
                        details = f"MV missing {missing_count} dates (parquet: {parquet_count})"
                    else:
                        details = f"Parquet: {parquet_count}, MV: {mv_count}"
                    
                    status_symbol = {
                        "SYNCED": "✓",
                        "STALE_MV": "⚠ STALE",
                        "MISSING_MV": "✗ MISSING",
                        "ORPHANED_MV": "? ORPHAN",
                        "MIXED": "~ MIXED",
                    }.get(status, status)
                    
                    self.root.after(0, lambda n=mv_name, d=details, m=missing_count, s=status_symbol: 
                        self._add_result(n, d, str(m), s))
                
                # Summary
                synced = sum(1 for r in results.values() if r["status"] == "SYNCED")
                stale = sum(1 for r in results.values() if r["status"] == "STALE_MV")
                missing = sum(1 for r in results.values() if r["status"] == "MISSING_MV")
                
                self.log(f"\nMV Scan complete:")
                self.log(f"  Synced: {synced}, Stale: {stale}, Missing: {missing}")
                
                self.root.after(0, lambda: self.status_var.set(f"MV scan: {synced} synced, {stale} stale, {missing} missing"))
                
            except Exception as e:
                self.log(f"MV scan error: {e}")
                self.root.after(0, lambda: self.status_var.set(f"Error: {e}"))
            finally:
                self._running = False

        threading.Thread(target=do_mv_scan, daemon=True).start()

    def _refresh_all_mvs(self):
        """Refresh all materialized views."""
        self._refresh_mvs(set(MV_TO_PARQUET_MAP.keys()), "all")

    def _refresh_selected_mvs(self):
        """Refresh selected materialized views (opens dialog)."""
        # Create a simple dialog to select MVs
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Materialized Views to Refresh")
        dialog.geometry("400x300")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text="Select views to refresh:").pack(pady=10)
        
        mv_vars = {}
        for mv_name, config in MV_TO_PARQUET_MAP.items():
            var = tk.BooleanVar(value=False)
            mv_vars[mv_name] = var
            ttk.Checkbutton(
                dialog, 
                text=f"{mv_name} - {config['description']}", 
                variable=var
            ).pack(anchor="w", padx=20)
        
        def on_refresh():
            selected = {k for k, v in mv_vars.items() if v.get()}
            dialog.destroy()
            if selected:
                self._refresh_mvs(selected, "selected")
            else:
                messagebox.showinfo("No Selection", "No views selected.")
        
        def on_cancel():
            dialog.destroy()
        
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="Refresh", command=on_refresh).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=on_cancel).pack(side=tk.LEFT, padx=5)

    def _refresh_mvs(self, views: Set[str], mode: str):
        """Refresh the specified materialized views with optional auto-fetch."""
        if self._running:
            messagebox.showwarning("Busy", "Another operation is already running.")
            return

        if not DUCKDB_AVAILABLE:
            messagebox.showerror(
                "DuckDB Not Available",
                "DuckDB is not installed. Cannot refresh materialized views."
            )
            return

        # Get date range for cascading refresh
        date_range = self._get_date_range()
        if not date_range:
            # Fall back to simple refresh without auto-fetch if no dates
            start_date, end_date = None, None
        else:
            start_date, end_date = date_range

        # Confirm with summary
        view_list = "\n".join(f"  - {v}" for v in sorted(views))
        auto_fetch_str = "\n(Auto-fetch from Odoo enabled if data missing)" if start_date else ""

        if not messagebox.askyesno(
            "Confirm MV Refresh",
            f"Refresh {mode} materialized views?\n\n{view_list}{auto_fetch_str}"
        ):
            return

        self._running = True
        self.status_var.set("Refreshing MVs (checking data availability)...")
        self.runner.reset_stop()

        def do_refresh():
            try:
                # Check if Docker ETL is enabled
                if DOCKER_ETL_ENABLED and DOCKER_RUNNER_AVAILABLE and start_date and end_date:
                    self.log("[Docker Mode] Running MV refresh in container...")
                    views_csv = ",".join(sorted(views))
                    cli_args = [
                        "python", "scripts/etl_data_manager_cli.py",
                        "refresh-mvs-cascading",
                        "--views", views_csv,
                        "--start", start_date.isoformat(),
                        "--end", end_date.isoformat(),
                        "--auto-fetch",
                    ]
                    exit_code = self._run_docker_cli(cli_args)
                    if exit_code != 0:
                        self.log(f"Docker CLI exited with code {exit_code}")
                        self.root.after(0, lambda: self.status_var.set(f"Docker error: exit {exit_code}"))
                        return
                    success = 5  # All 5 MVs in default set
                    failed = 0
                else:
                    # Use cascading refresh with auto-fetch (local mode)
                    if start_date and end_date:
                        results = self.runner.refresh_materialized_views_cascading(
                            views, start_date, end_date, auto_fetch=True, refresh_dims=False
                        )

                        # Log what was fetched
                        fetched = results.get("fetched", [])
                        if fetched:
                            total_fetched = sum(f.get("success", 0) for f in fetched)
                            self.log(f"\nAuto-fetched {total_fetched} day(s) from Odoo:")
                            for fetch_info in fetched:
                                view = fetch_info.get("view", "unknown")
                                dates = fetch_info.get("dates_fetched", [])
                                for range_start, range_end in dates:
                                    self.log(f"  {view}: {range_start} to {range_end}")

                        # Log aggregates built
                        aggregates = results.get("aggregates_built", [])
                        if aggregates:
                            self.log(f"\nBuilt {len(aggregates)} aggregate type(s)")
                    else:
                        # Simple refresh without auto-fetch
                        results = self.runner.refresh_materialized_views(views)

                    success = results.get("success", 0)
                    failed = results.get("failed", 0)
                    errors = results.get("errors", [])

                    if failed:
                        for error in errors:
                            self.log(f"  Error: {error}")

                self.log(f"\nMV Refresh complete: {success} view(s) built")

                if failed == 0:
                    self.root.after(
                        0,
                        lambda: messagebox.showinfo(
                            "Success",
                            f"Successfully refreshed {success} materialized view(s)!"
                        )
                    )
                else:
                    self.root.after(
                        0,
                        lambda: messagebox.showwarning(
                            "Completed with Errors",
                            f"Refreshed {success} view(s) with {failed} error(s).\n"
                            f"Check logs for details."
                        )
                    )

                self.root.after(0, lambda: self.status_var.set("MV refresh complete"))

            except Exception as e:
                self.log(f"MV refresh error: {e}")
                self.root.after(0, lambda: self.status_var.set(f"Error: {e}"))
            finally:
                self._running = False

        threading.Thread(target=do_refresh, daemon=True).start()

    def _validate_profit(self):
        """Run profit validation."""
        if self._running:
            messagebox.showwarning("Busy", "Another operation is already running.")
            return

        date_range = self._get_date_range()
        if not date_range:
            return

        start_date, end_date = date_range

        if not messagebox.askyesno(
            "Confirm Validation",
            f"Validate profit calculations from {start_date} to {end_date}?"
        ):
            return

        self._running = True
        self.status_var.set("Validating profit...")
        self._clear_results()

        def do_validate():
            try:
                # Check if Docker ETL is enabled
                if DOCKER_ETL_ENABLED and DOCKER_RUNNER_AVAILABLE:
                    self.log("[Docker Mode] Running profit validation in container...")
                    cli_args = [
                        "python", "scripts/etl_data_manager_cli.py",
                        "validate-profit",
                        "--start", start_date.isoformat(),
                        "--end", end_date.isoformat(),
                    ]
                    exit_code = self._run_docker_cli(cli_args)
                    if exit_code != 0:
                        self.log(f"Docker CLI exited with code {exit_code}")
                        self.root.after(0, lambda: self.status_var.set(f"Docker error: exit {exit_code}"))
                        return
                    success = 0
                    failed = 0
                else:
                    results = self.runner.validate_profit(start_date, end_date)

                    # Display results
                    for vr in results.get("validation_results", []):
                        status = vr["status"]
                        date_str = vr["date"]
                        records = vr["records"]
                        issues = vr["issues"]

                        issues_str = "; ".join(issues) if issues else "OK"

                        self.root.after(0, lambda ds=date_str, st=status, rec=records, iss=issues_str:
                            self._add_result(f"Profit {ds}", iss, str(rec), st))

                    success = results.get("success", 0)
                    failed = results.get("failed", 0)

                self.log(f"\nProfit validation complete: {success} valid, {failed} invalid")

                if failed == 0:
                    self.root.after(
                        0,
                        lambda: messagebox.showinfo("Validation Passed",
                            f"All {success} days passed profit validation!")
                    )
                else:
                    self.root.after(
                        0,
                        lambda: messagebox.showwarning(
                            "Validation Issues",
                            f"{failed} of {success + failed} days have validation issues."
                        )
                    )

                self.root.after(0, lambda: self.status_var.set("Profit validation complete"))

            except Exception as e:
                self.log(f"Validation error: {e}")
                self.root.after(0, lambda: self.status_var.set(f"Error: {e}"))
            finally:
                self._running = False

        threading.Thread(target=do_validate, daemon=True).start()

    def _build_aggregates(self):
        """Build aggregate tables for selected date range."""
        if self._running:
            messagebox.showwarning("Busy", "Another operation is already running.")
            return

        date_range = self._get_date_range()
        if not date_range:
            return

        start_date, end_date = date_range

        # Ask which aggregates to build
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Aggregates to Build")
        dialog.geometry("350x200")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Select aggregates to build:").pack(pady=10)

        sales_var = tk.BooleanVar(value=True)
        profit_var = tk.BooleanVar(value=True)

        ttk.Checkbutton(dialog, text="Sales Aggregates", variable=sales_var).pack(anchor="w", padx=20)
        ttk.Checkbutton(dialog, text="Profit Aggregates", variable=profit_var).pack(anchor="w", padx=20)

        def on_build():
            dialog.destroy()

            selected_aggs = []
            if sales_var.get():
                selected_aggs.append("sales_aggregates")
            if profit_var.get():
                selected_aggs.append("profit_aggregates")

            if not selected_aggs:
                messagebox.showinfo("No Selection", "No aggregates selected.")
                return

            self._running = True
            self.status_var.set("Building aggregates...")
            self._clear_results()

            def do_build():
                try:
                    total_success = 0
                    total_failed = 0

                    # Check if Docker ETL is enabled
                    if DOCKER_ETL_ENABLED and DOCKER_RUNNER_AVAILABLE:
                        self.log("[Docker Mode] Building aggregates in container...")
                        types_csv = ",".join(selected_aggs)
                        cli_args = [
                            "python", "scripts/etl_data_manager_cli.py",
                            "build-aggregates",
                            "--types", types_csv,
                            "--start", start_date.isoformat(),
                            "--end", end_date.isoformat(),
                        ]
                        exit_code = self._run_docker_cli(cli_args)
                        if exit_code != 0:
                            self.log(f"Docker CLI exited with code {exit_code}")
                            self.root.after(0, lambda: self.status_var.set(f"Docker error: exit {exit_code}"))
                            return
                        # Assume success for now - CLI output is streamed
                        total_success = len(selected_aggs)
                        total_failed = 0
                    else:
                        for agg_type in selected_aggs:
                            self.log(f"\nBuilding {agg_type}...")
                            results = self.runner.backfill_aggregates(
                                agg_type, start_date, end_date
                            )
                            total_success += results.get("success", 0)
                            total_failed += results.get("failed", 0)

                            for error in results.get("errors", []):
                                self.log(f"  Error: {error}")

                    self.log(f"\nAggregate build complete: {total_success} success, {total_failed} failed")
                    self.root.after(0, lambda: self.status_var.set("Aggregate build complete"))

                    if total_failed == 0:
                        self.root.after(
                            0,
                            lambda: messagebox.showinfo("Success",
                                f"Successfully built {total_success} aggregate day(s)!")
                        )
                    else:
                        self.root.after(
                            0,
                            lambda: messagebox.showwarning(
                                "Completed with Errors",
                                f"Built {total_success} day(s) with {total_failed} error(s)."
                            )
                        )

                except Exception as e:
                    self.log(f"Build error: {e}")
                    self.root.after(0, lambda: self.status_var.set(f"Error: {e}"))
                finally:
                    self._running = False

            threading.Thread(target=do_build, daemon=True).start()

        def on_cancel():
            dialog.destroy()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="Build", command=on_build).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=on_cancel).pack(side=tk.LEFT, padx=5)

    def _run_docker_cli(self, cli_args: list[str]) -> int:
        """Run CLI command in Docker container with streaming output.
        
        Args:
            cli_args: CLI arguments to pass to etl_data_manager_cli.py
            
        Returns:
            Exit code from the command
        """
        from services.docker_compose_runner import run_compose_exec_with_output
        
        def log_line(line: str) -> None:
            self.root.after(0, lambda ln=line: self.log(ln))
        
        return run_compose_exec_with_output(
            service=DOCKER_ETL_SERVICE,
            args=cli_args,
            cwd=str(project_root),
            line_callback=log_line,
        )

    def _parse_json_result(self, output_lines: list[str]) -> dict:
        """Parse RESULT_JSON_START...RESULT_JSON_END from output.
        
        Args:
            output_lines: Lines from CLI output
            
        Returns:
            Parsed JSON result dictionary
        """
        import json
        in_json = False
        json_lines = []
        for line in output_lines:
            if "RESULT_JSON_START" in line:
                in_json = True
                continue
            if "RESULT_JSON_END" in line:
                in_json = False
                continue
            if in_json:
                json_lines.append(line)
        
        if json_lines:
            try:
                return json.loads("\n".join(json_lines))
            except json.JSONDecodeError:
                pass
        return {"success": 0, "failed": 0, "errors": []}

    def _stop(self):
        """Stop current operation."""
        if self._running:
            self.runner.stop()
            self.status_var.set("Stopping...")


def get_mv_scanner():
    """Get MVScanner instance for use in other modules."""
    return MVScanner()

def get_data_scanner():
    """Get DataScanner instance for use in other modules."""
    return DataScanner()

def get_backfill_runner(log_callback=None):
    """Get BackfillRunner instance for use in other modules."""
    return BackfillRunner(log_callback=log_callback)

# Export key functions for direct import
__all__ = [
    'DataScanner', 'MVScanner', 'BackfillRunner',
    'get_mv_scanner', 'get_data_scanner', 'get_backfill_runner',
    'MV_TO_PARQUET_MAP', 'DATASET_GROUPS'
]


def main():
    """Main entry point."""
    _ensure_tk()  # Lazy load tkinter before GUI
    root = tk.Tk()
    app = ETLDataManagerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
