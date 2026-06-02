**Subtask 1.3 Reflection: ETL Load Extraction**

**Completed:** 2026-06-02  
**Workstream:** NK_20260602_migration_solo_render_0a1b  
**Scope:** Extract star-schema writers and raw save functions from `etl_tasks.py` into `etl/load/` package.

**What Was Done**

Two new modules created under `etl/load/`:

1. `etl/load/raw.py` — All raw data persistence functions.
  - `save_raw_data` (POS)
  - `_save_raw_account_move_lines` (Helper for invoices)
  - `save_raw_sales_invoice_lines`
  - `save_raw_purchase_invoice_lines`
  - `save_raw_inventory_moves`
  - `save_raw_stock_quants`
2. `etl/load/star_schema.py` — All final fact table writers.
  - `update_fact_inventory_moves`
  - `update_fact_sales_pos`
  - `update_fact_invoice_sales`
  - `update_fact_purchases`
  - `update_fact_stock_on_hand_snapshot`

**Troubleshoot Findings**

**Finding A:** `_update_fact_sales_pos` **had hidden transform dependency**

The function performed a last-minute timezone conversion if `date` was missing but `order_date` was present:
`if 'date' not in df.columns and 'order_date' in df.columns: df = df.with_columns(to_local_datetime('order_date').alias('date'))`

**Fix applied:** I imported `to_local_datetime` from `etl.transform._utils`. This is a permissible dependency as `load` naturally depends on `transform` (the data must be cleaned before it is loaded).

**Finding B:** `save_raw_data` **used a hardcoded raw_schema**

The original code defined a large schema for POS raw data inside the function. I preserved this exactly to ensure that raw data remains consistent with legacy runs, but marked it as a candidate for moving to `etl/core/schema.py` in a future optimization phase.

**Finding C: No launder/cleanup of an app.task decorator**

Similar to the transform phase, the `load` functions in `etl_tasks.py` were all decorated with `@app.task`. In the new `etl/load/` modules, these are now plain Python functions. This completely removes the Celery dependency from the persistence layer.

**Dependency Verification**

**ImportLoad FilesStatus**`celery`❌ Not present✅ Clean`redis`❌ Not present✅ Clean`dash` / `flask` / `gunicorn`❌ Not present✅ Clean`odoorpc`❌ Not present✅ Clean`services.*`❌ Not present✅ Clean`polars`✅ Present (expected)✅ OK`etl.config`✅ Present (paths only)✅ OK`etl.io_parquet`✅ Present (atomic write)✅ OK`etl.transform._utils`✅ Present (timezone)✅ OK

**Risk & Mitigation**

**RiskStatusMitigation**`etl_tasks.py` still contains duplicate inline load functions🔧 PendingRemoved during Subtask 1.4 (wiring) when Celery task wrappers are refactored to import from `etl.load.*`Incorrect partition path creation if `target_date` format changes🔍 Low risk`target_date` is strictly ISO (YYYY-MM-DD) throughout the pipeline. Behavior identical to original.

**Next Subtask**

**1.4** — Create `etl/tasks.py` as the plain Python function registry. This will be the "glue" that maps string task names (used by the scheduler) to the actual functions in `extract/`, `transform/`, `load/`, and `core/`.

**Files Referenced**

- `etl_tasks.py` (source of extraction, lines 380-630 and 1030-1150)
- `etl/load/raw.py` (new)
- `etl/load/star_schema.py` (new)
- `etl/transform/_utils.py` (imported)
- `etl/config.py` (path constants)

*End of Reflection 1.3*
