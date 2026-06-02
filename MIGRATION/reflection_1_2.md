# Subtask 1.2 Reflection: ETL Transform Extraction

**Completed:** 2026-06-02
**Workstream:** NK_20260602_migration_solo_render_0a1b
**Scope:** Extract Polars cleaning functions from `etl_tasks.py` into `etl/transform/` package.

---

## What Was Done

Five new modules created under `etl/transform/`:

1. **`etl/transform/__init__.py`** — Package-level docstring establishing zero-framework rule.
2. **`etl/transform/_utils.py`** — `to_local_datetime` Polars expression utility.
3. **`etl/transform/pos.py`** — `clean_pos_data` (raw POS → cast types, compute revenue, write clean Parquet).
4. **`etl/transform/invoices.py`** — `clean_sales_invoice_lines` + `clean_purchase_invoice_lines` (discount-calculation logic preserved exactly).
5. **`etl/transform/inventory.py`** — `clean_stock_quants` + `clean_inventory_moves` (dimension parquet joins preserved exactly).

All functions were extracted from `etl_tasks.py` with **identical logic** — same Polars expressions, same column renames, same null-fill defaults, same dimension join patterns.

---

## Troubleshoot Findings

### Finding A: `to_local_datetime` had hidden Celery dependency
**Original code in `etl_tasks.py` (line 287):**
```python
def to_local_datetime(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        ...
        .dt.convert_time_zone(app.conf.timezone)   # ← Celery app object!
        ...
    )
```

The timezone conversion reached into the global Celery app instance (`app.conf.timezone`). This meant the transform layer could not run without Celery being initialised — a framework dependency in what should be pure Polars logic.

**Fix applied:** Replaced with environment-variable lookup:
```python
LOCAL_TZ = os.environ.get('TZ', 'Asia/Jakarta')

def to_local_datetime(col_name: str) -> pl.Expr:
    ...
    .dt.convert_time_zone(LOCAL_TZ)
    ...
```

This matches the timezone already configured in `etl_tasks.py` (`Asia/Jakarta`) and makes the utility self-contained.

### Finding B: `clean_inventory_moves` reads dimension parquet files directly
This is not a bug — it is correct architecture — but it is worth noting for future load-phase design.

`clean_inventory_moves` joins against:
- `dim_products.parquet`
- `dim_locations.parquet`
- `dim_uoms.parquet`
- `dim_partners.parquet`

These files are produced by `refresh_dimensions_incremental` (dimension refresh task). The transform function safely falls back to empty lazy DataFrames when the files are missing, so the pipeline does not crash on first run before dimensions exist.

**Implication for future:** When we build `scheduler/main.py` (Subtask 1.5), the dependency ordering must ensure dimension refresh runs before inventory-move cleaning on the same day.

### Finding C: No de-duplication or simplification was applied
The cleaning functions contain a lot of repetitive boilerplate:
- Each function checks `os.path.isfile(raw_file_path)` individually.
- Each function splits `target_date` into year/month/day individually.
- Each function calls `os.makedirs(..., exist_ok=True)` individually.

**Decision:** I deliberately did **not** refactor or abstract these patterns in this subtask. The goal is extraction (code movement) not optimisation. Refactoring the logic risks introducing subtle behaviour changes. Abstraction can happen later in a dedicated optimisation phase once parity tests pass.

---

## Dependency Verification

| Import | Transform Files | Status |
|---|---|---|
| `celery` | ❌ Not present | ✅ Clean |
| `redis` | ❌ Not present | ✅ Clean |
| `dash` / `flask` / `gunicorn` | ❌ Not present | ✅ Clean |
| `odoorpc` | ❌ Not present | ✅ Clean |
| `services.*` | ❌ Not present | ✅ Clean |
| `polars` | ✅ Present (expected) | ✅ OK |
| `etl.config` | ✅ Present (paths only) | ✅ OK |
| `etl.io_parquet` | ✅ Present (atomic write) | ✅ OK |

---

## Risk & Mitigation

| Risk | Status | Mitigation |
|---|---|---|
| `etl_tasks.py` still contains duplicate inline cleaning functions | 🔧 Pending | Removed during Subtask 1.4 (wiring) when Celery task wrappers are refactored to import from `etl.transform.*` |
| `to_local_datetime` timezone default might differ from Celery beat TZ config | 🔍 Low risk | Both use `Asia/Jakarta`. Verified: `etl_tasks.py` sets `app.conf.timezone = os.environ.get('TZ', 'Asia/Jakarta')`. Behaviour identical. |
| `clean_inventory_moves` dimension fallbacks (empty lazy frames) might change output schema subtly | 🔍 Low risk | Empty frames use explicit schemas identical to the parquet schemas. Verified against `etl_tasks.py` inline definitions. |

---

## Next Subtask
**1.3** — Extract star-schema writers (`_update_fact_*`) and raw save functions (`save_raw_*`, `_save_raw_account_move_lines`) from `etl_tasks.py` into `etl/load/*.py`.

## Files Referenced
- `etl_tasks.py` (source of extraction, lines 287-1060)
- `etl/transform/_utils.py` (new)
- `etl/transform/pos.py` (new)
- `etl/transform/invoices.py` (new)
- `etl/transform/inventory.py` (new)
- `etl/config.py` (path constants)

---

*End of Reflection 1.2*
