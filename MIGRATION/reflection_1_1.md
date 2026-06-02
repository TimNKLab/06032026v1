# Subtask 1.1 Reflection: ETL Core Extraction

**Completed:** 2026-06-02
**Workstream:** NK_20260602_migration_solo_render_0a1b
**Scope:** Extract pure business logic from `etl_tasks.py` lines ~800-1600 into `etl/core/` package.

---

## What Was Done

Three new modules created under `etl/core/`:

1. **`schema.py`** — I/O primitives (`has_parquet_files`, `read_parquet_or_empty`, `partition_path`, `write_partitioned`) and shared Polars schema definitions.
2. **`cost_engine.py`** — Tax multiplier Polars expression (`tax_multiplier_expr`) and beginning-cost validation (`validate_beginning_costs`).
3. **`profit_calculator.py`** — All profit ETL functions:
   - `build_product_cost_events`
   - `latest_cost_by_product`
   - `build_cost_snapshot_from_events`
   - `build_product_cost_latest_daily`
   - `_unified_costs` *(new — replaces DuckDB view)*
   - `build_sales_lines_profit` *(rewritten — replaces DuckDB connection with pure Polars)*
   - `build_profit_aggregates`
   - `build_sales_aggregates`

## Troubleshoot Findings

### Finding A: `build_sales_lines_profit` had hidden dashboard-service dependency
**Original code in `etl_tasks.py`:**
```python
from services.duckdb_connector import get_duckdb_connection  # ← Dashboard service!
conn = get_duckdb_connection()
unified_costs_query = f"SELECT ... FROM fact_product_costs_unified ..."
unified_costs_df = pl.from_pandas(conn.execute(unified_costs_query).fetchdf())
```

This violates the ETL/Dashboard separation boundary. ETL core was reading from a DuckDB **view** (`fact_product_costs_unified`) that is defined inside `services/duckdb_connector.py` — a dashboard module.

**Fix applied:** Created `_unified_costs(target_date)` in `etl/core/profit_calculator.py` that:
1. Reads `fact_product_cost_latest_daily/**/*.parquet` directly
2. Reads `fact_product_legacy_costs/**/*.parquet` directly
3. Concatenates both sources with `priority` column
4. Replicates `ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY priority ASC, date DESC)` using Polars `group_by(...).agg(pl.all().first())` after sorting

**Risk:** Polars `group_by` + `first()` is semantically equivalent to `ROW_NUMBER() = 1` only when the DataFrame is pre-sorted by the exact same keys. Verified that the sort key is `['product_id', struct('priority', 'date')]` and group is on `product_id` — correct.

### Finding B: `_tax_multiplier_expr` defined twice in `etl_tasks.py`
Lines 299 and 2093 both define the same function with parameter names `tax_col` vs `tax_id_col`. This is a code-smell indicating the god file had organically grown duplication.

**Fix:** Single canonical definition now lives in `etl/core/cost_engine.py`.

### Finding C: `_build_sales_aggregates` bundled with profit functions
In the original `etl_tasks.py`, `_build_sales_aggregates` (which rolls up POS + invoice sales into `agg_sales_daily`) was placed immediately after `_build_profit_aggregates` even though it is conceptually a **sales** function, not profit.

**Decision:** Moved into `etl/core/profit_calculator.py` for now because it shares the same ETL pipeline phase (daily aggregation at 02:12). Future refactoring (Phase 1.2-1.3) may extract it to `etl/core/sales_aggregates.py` when the full `etl/` package structure is stabilised.

## Dependency Verification

| Import | New Core Files | Status |
|---|---|---|
| `celery` | ❌ Not present | ✅ Clean |
| `redis` | ❌ Not present | ✅ Clean |
| `dash` / `flask` / `gunicorn` | ❌ Not present | ✅ Clean |
| `odoorpc` | ❌ Not present | ✅ Clean |
| `services.duckdb_connector` | ❌ Removed from profit calc | ✅ Clean |
| `polars` | ✅ Present (expected) | ✅ OK |
| `etl.config` | ✅ Present (paths only) | ✅ OK |
| `etl.io_parquet` | ✅ Present (atomic write) | ✅ OK |

## Risk & Mitigation

| Risk | Status | Mitigation |
|---|---|---|
| `_unified_costs` Polars logic deviates from DuckDB view output | 🔍 Needs parity test in Subtask 4.1 | Compare `build_sales_lines_profit` output (new) vs old implementation on 3 historical dates |
| `etl_tasks.py` still contains old inline copies of extracted functions | 🔧 Cleanup pending | Will be removed during Subtask 1.4 (create `etl/tasks.py`) when Celery wrappers are refactored |
| Schema constants duplicated between `etl/core/schema.py` and inline schemas in `etl_tasks.py` | 🔧 Cleanup pending | Inline schemas in `etl_tasks.py` should import from `etl.core.schema` during wiring (Subtask 1.4) |

## Next Subtask
**1.2** — Extract Polars cleaning functions from `etl_tasks.py` (lines ~500-800, `clean_pos_data`, `clean_sales_invoice_lines`, etc.) into `etl/transform/*.py`.

## Files Referenced
- `etl_tasks.py` (original god file, lines 1251-1700 analysed)
- `etl/core/schema.py` (new)
- `etl/core/cost_engine.py` (new)
- `etl/core/profit_calculator.py` (new)
- `etl/config.py` (path constants)
- `services/duckdb_connector.py` (old dependency, now severed)

---

*End of Reflection 1.1*
