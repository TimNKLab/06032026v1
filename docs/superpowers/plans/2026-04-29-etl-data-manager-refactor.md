# ETL Data Manager Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor ETL Data Manager to use existing force refresh scripts, add aggregation functions, build MVs, and validate profit.

**Architecture:** Replace direct etl_tasks calls with force refresh script imports, add aggregation runner using update_sales_aggregates and update_profit_aggregates, add MV builder, add profit validator.

**Tech Stack:** Python, Tkinter, Celery tasks, DuckDB, Polars

---

## File Structure

| File | Responsibility |
|------|---------------|
| `scripts/etl_data_manager.py` | Main GUI - will be refactored to use new runner classes |
| `scripts/force_refresh_pos_data.py` | POS/Invoice/Inventory moves refresh - import main() |
| `scripts/force_refresh_dimensions.py` | Dimension refresh - import main() |
| `scripts/force_refresh_stock_quants.py` | Stock quants refresh - import main() |
| `scripts/force_refresh_purchase_data.py` | Purchase data refresh - import main() |
| `scripts/backfill_sales_aggregates.py` | Sales aggregates backfill - reference for aggregation |
| `scripts/run_profit_etl.py` | Profit ETL - reference for profit validation |

---

## Task 1: Create Modular Refresh Runner

**Files:**
- Modify: `scripts/etl_data_manager.py:390-550` (BackfillRunner class)

**Goal:** Refactor BackfillRunner to use force refresh scripts instead of direct etl_tasks calls.

- [ ] **Step 1: Add imports for force refresh scripts**

Add these imports at the top of BackfillRunner section:

```python
# Import force refresh modules
import sys
from pathlib import Path

# Add scripts directory to path for imports
scripts_dir = Path(__file__).parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

# Import force refresh main functions
try:
    from force_refresh_pos_data import main as refresh_pos_main
    from force_refresh_dimensions import main as refresh_dims_main
    from force_refresh_stock_quants import main as refresh_quants_main
    from force_refresh_purchase_data import main as refresh_purchase_main
    FORCE_REFRESH_AVAILABLE = True
except ImportError as e:
    FORCE_REFRESH_AVAILABLE = False
    print(f"Warning: Could not import force refresh modules: {e}")
```

- [ ] **Step 2: Refactor backfill_facts to use force refresh**

Replace the entire `backfill_facts` method (lines ~441-550):

```python
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
        "pos": ("pos", refresh_pos_main),
        "invoice_sales": ("invoice-sales", refresh_pos_main),
        "purchases": (None, refresh_purchase_main),  # Uses its own script
        "inventory_moves": ("inventory-moves", refresh_pos_main),
        "stock_quants": (None, refresh_quants_main),  # Uses its own script
    }
    
    try:
        if dataset_key == "profit":
            # Profit uses run_profit_etl - handle separately
            results = self._backfill_profit(start_date, end_date, refresh_dims)
        elif dataset_key in dataset_to_script:
            target, script_main = dataset_to_script[dataset_key]
            
            # Refresh dimensions if needed (for inventory/stock)
            if refresh_dims and dataset_key in ("inventory_moves", "stock_quants"):
                self.log("  Refreshing dimensions first...")
                try:
                    refresh_dims_main(["--targets", "products", "locations", "lots"])
                except SystemExit:
                    pass  # argparse calls sys.exit()
            
            # Build args for force refresh script
            args = ["--start", start_date.isoformat()]
            if start_date != end_date:
                args.extend(["--end", end_date.isoformat()])
            if target:
                args.extend(["--targets", target])
            
            self.log(f"  Running: {script_main.__module__} {' '.join(args)}")
            
            try:
                exit_code = script_main(args)
                if exit_code == 0:
                    results["success"] = 1
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
```

---

## Task 2: Add Aggregation Runner

**Files:**
- Modify: `scripts/etl_data_manager.py:550-620` (after backfill_facts)

**Goal:** Add methods to build sales and profit aggregates.

- [ ] **Step 3: Add backfill_aggregates method**

Insert after `_backfill_profit`:

```python
def backfill_aggregates(
    self,
    agg_type: str,
    start_date: date,
    end_date: date,
) -> Dict:
    """Build aggregate tables for date range."""
    self.log(f"Building {agg_type} from {start_date} to {end_date}")
    
    results = {"success": 0, "failed": 0, "errors": []}
    
    try:
        if agg_type == "sales_aggregates":
            from etl_tasks import update_sales_aggregates
            
            current = start_date
            while current <= end_date and not self._stop_requested:
                date_str = current.isoformat()
                self.log(f"  Building sales aggregates for {date_str}...")
                
                try:
                    update_sales_aggregates(date_str)
                    results["success"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"{date_str}: {e}")
                
                current += timedelta(days=1)
                
        elif agg_type == "profit_aggregates":
            from etl_tasks import update_profit_aggregates
            
            current = start_date
            while current <= end_date and not self._stop_requested:
                date_str = current.isoformat()
                self.log(f"  Building profit aggregates for {date_str}...")
                
                try:
                    update_profit_aggregates(date_str)
                    results["success"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"{date_str}: {e}")
                
                current += timedelta(days=1)
        else:
            results["errors"].append(f"Unknown aggregate type: {agg_type}")
            
    except ImportError as e:
        results["errors"].append(f"Could not import aggregation tasks: {e}")
        results["failed"] = 1
    
    return results
```

---

## Task 3: Add Profit Validator

**Files:**
- Modify: `scripts/etl_data_manager.py:620-700` (after backfill_aggregates)

**Goal:** Add profit validation functionality.

- [ ] **Step 4: Add profit validation method**

```python
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
```

---

## Task 4: Add Cascading MV Builder with Auto-Fetch

**Files:**
- Modify: `scripts/etl_data_manager.py:700-850` (after validate_profit)

**Goal:** Add MV building that auto-fetches raw data from Odoo if missing.

- [ ] **Step 5: Add parquet availability scanner**

```python
def _scan_parquet_availability(
    self,
    dataset_key: str,
    start_date: date,
    end_date: date,
) -> Dict:
    """Scan for missing parquet data and return dates that need fetching."""
    from etl import config as etl_config
    from pathlib import Path
    
    # Map dataset keys to parquet paths
    path_map = {
        "pos": etl_config.FACT_SALES_PATH,
        "invoice_sales": etl_config.FACT_INVOICE_SALES_PATH,
        "purchases": etl_config.FACT_PURCHASES_PATH,
        "inventory_moves": etl_config.FACT_INVENTORY_MOVES_PATH,
        "stock_quants": etl_config.FACT_STOCK_QUANTS_PATH,
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
```

- [ ] **Step 6: Add auto-fetch for missing data**

```python
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
```

- [ ] **Step 7: Add cascading MV refresh method**

```python
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
            "mv_sales_daily": "pos",  # Uses agg_sales_daily which comes from pos/invoice
            "mv_sales_daily_by_product": "pos",
            "mv_sales_daily_by_principal": "pos",
            "mv_profit_daily": "profit",
            "mv_profit_daily_by_product": "profit",
            "mv_purchases_daily": "purchases",
            "mv_inventory_moves": "inventory_moves",
            "mv_stock_quants": "stock_quants",
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
        aggregate_views = {"mv_sales_daily", "mv_sales_daily_by_product", "mv_sales_daily_by_principal"}
        profit_aggregate_views = {"mv_profit_daily", "mv_profit_daily_by_product"}
        
        if views & aggregate_views and start_date and end_date:
            self.log("  Building sales aggregates...")
            agg_results = self.backfill_aggregates("sales_aggregates", start_date, end_date)
            results["aggregates_built"].append({"type": "sales", **agg_results})
        
        if views & profit_aggregate_views and start_date and end_date:
            self.log("  Building profit aggregates...")
            agg_results = self.backfill_aggregates("profit_aggregates", start_date, end_date)
            results["aggregates_built"].append({"type": "profit", **agg_results})
        
        # Step 3: Load into DuckDB MVs
        self.log("  Loading data into DuckDB materialized views...")
        
        from services.duckdb_connector import DuckDBManager
        manager = DuckDBManager()
        
        try:
            manager.ensure_materialized_views(views)
            results["views_built"] = list(views)
            results["success"] = len(views)
            self.log(f"  Successfully built {len(views)} view(s)")
        except Exception as e:
            results["failed"] = len(views)
            results["errors"].append(f"DuckDB error: {e}")
            self.log(f"  Failed to build views: {e}")
        
    except Exception as e:
        results["errors"].append(str(e))
        results["failed"] = len(views)
        self.log(f"  Cascading refresh error: {e}")
    
    return results
```

---

## Task 5: Update GUI Button Handlers

**Files:**
- Modify: `scripts/etl_data_manager.py:1060-1150` (_backfill method)

**Goal:** Update the backfill handler to use new runner methods properly.

- [ ] **Step 6: Update _backfill to handle all dataset types**

Replace the `_backfill` method (around line 1060):

```python
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
```

---

## Task 6: Add Profit Validation Button

**Files:**
- Modify: `scripts/etl_data_manager.py:760-780` (MV operations frame)

**Goal:** Add profit validation button to GUI.

- [ ] **Step 7: Add Validate Profit button**

Add after the MV buttons in the mv_frame:

```python
# Add separator
        ttk.Separator(mv_frame, orient="vertical").pack(side=tk.LEFT, padx=10, fill="y")
        
        # Add validation buttons
        ttk.Button(
            mv_frame, text="Validate Profit", command=self._validate_profit, width=18
        ).pack(side=tk.LEFT, padx=5)
```

Then add the handler method after `_refresh_mvs` (around line 1270):

```python
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

**Files:**
- Modify: `scripts/etl_data_manager.py:550-600` (existing backfill_dimensions)

**Goal:** Update backfill_dimensions to use force_refresh_dimensions.

- [ ] **Step 8: Refactor backfill_dimensions**

Replace `backfill_dimensions` method:

```python
def backfill_dimensions(self, dim_type: str = "all") -> Dict:
    """Refresh dimensions using force_refresh_dimensions script."""
    self.log(f"Refreshing dimensions ({dim_type})...")
    
    if not FORCE_REFRESH_AVAILABLE:
        return {"success": 0, "failed": 1, "errors": ["Force refresh not available"]}
    
    results = {"success": 0, "failed": 0, "errors": []}
    
    try:
        # Map dim_type to targets
        target_map = {
            "dimensions": [],  # All
            "dim_products": ["--targets", "products"],
            "dim_categories": ["--targets", "categories"],
            "dim_brands": ["--targets", "brands"],
        }
        
        args = target_map.get(dim_type, [])
        
        self.log(f"  Running: force_refresh_dimensions {' '.join(args)}")
        
        try:
            exit_code = refresh_dims_main(args)
            if exit_code == 0:
                results["success"] = 1
            else:
                results["failed"] = 1
                results["errors"].append(f"Script exited with code {exit_code}")
        except SystemExit as e:
            if e.code == 0 or e.code is None:
                results["success"] = 1
            else:
                results["failed"] = 1
                results["errors"].append(f"Script exited with code {e.code}")
        except Exception as e:
            results["failed"] = 1
            results["errors"].append(str(e))
            
    except Exception as e:
        results["errors"].append(str(e))
        results["failed"] = 1
    
    return results
```

---

## Task 8: Add Build Aggregates Button

**Files:**
- Modify: `scripts/etl_data_manager.py:760-800` (MV operations frame)

**Goal:** Add button to build aggregates.

- [ ] **Step 9: Add Build Aggregates button**

Add in mv_frame after Validate Profit button:

```python
ttk.Button(
    mv_frame, text="Build Aggregates", command=self._build_aggregates, width=18
).pack(side=tk.LEFT, padx=5)
```

Then add handler method after `_validate_profit`:

```python
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
```

---

## Testing Checklist

- [ ] Run GUI: `python scripts/etl_data_manager.py`
- [ ] Test POS backfill with 1-day range
- [ ] Test Invoice Sales backfill with 1-day range
- [ ] Test Purchases backfill with 1-day range
- [ ] Test Inventory Moves backfill with dimensions refresh
- [ ] Test Stock Quants backfill
- [ ] Test Dimensions refresh
- [ ] Test Sales Aggregates build
- [ ] Test Profit Aggregates build
- [ ] Test MV scan differences
- [ ] Test MV refresh all
- [ ] Test Profit validation
- [ ] Test Stop button functionality

---

## Summary

This refactor:
1. **Uses force refresh scripts** instead of direct etl_tasks calls for better maintainability
2. **Adds aggregation builder** for sales and profit aggregates
3. **Adds cascading MV builder** - scans parquet → fetches missing data from Odoo → builds aggregates → loads MVs
4. **Adds profit validator** to check profit data quality
5. **Keeps existing GUI structure** but improves backend reliability

### Cascading MV Refresh Flow
```
Click "Refresh All MVs" → 
  Check parquet availability for each view →
    Missing data? → Fetch from Odoo using force refresh scripts →
    Present? → Skip fetch →
  Build aggregates if needed →
  Load into DuckDB MVs →
  Show summary (fetched X days, built Y views)
```

All changes are backward-compatible with existing GUI layouts.
