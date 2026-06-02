# NKDash Restructuring Plan
## From Coupled Monolith → Decoupled UI + Data Platform

**Prepared:** 2026-06-02
**Current Version Analyzed:** Commit `17859de` (main)
**Scope:** Separate dashboard consumption from ETL production while preserving the data lake contract.

---

## 1. Goal Statement

Draw a hard boundary:

> **Dashboard (`nkdash-ui`)** is a read-only consumer of the data lake.  
> **Pipelines (`nkdash-pipelines`)** is the sole producer of the data lake.

They must not share code, imports, or runtime processes. They communicate only through parquet/SQLite files on a shared volume (or S3 in cloud).

---

## 2. Current vs. Target Architecture

### Current (Monolith)
```
nkdash/
├── app.py                     ← Dashboard + ETL health check + DuckDB init
├── etl_tasks.py               ← 2,200-line god file: Celery + Polars + business logic
├── services/
│   ├── duckdb_connector.py    ← Used by BOTH ETL workers AND dashboard queries
│   ├── etl_ops.py             ← ETL orchestration
│   ├── sales_metrics.py       ← Dashboard queries (imports duckdb_connector)
│   └── ...
├── pages/
│   ├── operational.py         ← ETL admin UI inside BI dashboard (1,378 lines)
│   ├── sales.py               ← BI page (imports sales_metrics)
│   └── ...
├── etl/
│   ├── extract/               ← Odoo extractors
│   ├── pipelines/             ← Celery pipeline definitions
│   └── ...
├── scripts/                   ← Mix of setup, backfill, diagnostics
├── check_*.py                 ← One-off operational scripts in root
└── docker-compose.yml         ← 5 services in one file
```

### Target (Separated)
```
nkdash-pipelines/              ← NEW REPO / sub-project
├── celery_app.py              ← Celery app + beat schedule ONLY
├── etl/
│   ├── extract/               ← Odoo RPC extractors (moved from nkdash/etl/)
│   ├── transform/             ← Polars cleaning (moved from etl_tasks.py)
│   ├── load/                  ← Parquet writers + star-schema builders
│   └── pipelines/             ← Orchestration signatures (daily.py, ranges.py, health.py)
├── tasks/
│   ├── extract_tasks.py       ← @app.task wrappers for extractors
│   ├── transform_tasks.py     ← @app.task wrappers for cleaners
│   └── load_tasks.py          ← @app.task wrappers for star-schema + aggregate writers
├── core/
│   ├── profit_calculator.py   ← Pure Polars profit logic (no Celery)
│   ├── cost_engine.py         ← Beginning costs + cost event logic
│   └── schema.py              ← Parquet schemas, constants
├── orchestration/
│   ├── daily_scheduler.py     ← Beat schedule definitions
│   ├── backfill.py            ← Date-range backfill runner
│   └── health.py              ← Catch-up + diagnostics
├── ops_cli/
│   ├── etl_manager.py         ← Headless CLI for force-refresh, backfill
│   └── monitor.py             ← Health checks, partition scanning
├── config/
│   └── settings.py            ← Pydantic settings (env vars, paths)
├── docker/
│   ├── Dockerfile.worker      ← Celery worker image
│   ├── Dockerfile.beat        ← Celery beat image
│   └── docker-compose.yml     ← Redis + Worker + Beat + CLI service
├── requirements.txt           ← No dash, no gunicorn, no mantine
└── tests/
    ├── unit/                  ← Polars transformation tests
    └── integration/           ← End-to-end pipeline tests (parquet output)

nkdash-ui/                     ← NEW REPO / sub-project (current nkdash slimmed)
├── app.py                     ← Dash app ONLY
├── config/
│   └── settings.py            ← Data lake path, Redis URL (for cache only), DuckDB disabled
├── data_access/
│   ├── connection.py          ← SQLite connection manager ONLY
│   ├── mv_reader.py           ← Materialized view / aggregate parquet reader
│   └── cache_client.py        ← Redis cache (for dashboard query caching)
├── metrics/
│   ├── sales.py               ← Query pre-aggregated sales (was services/sales_metrics.py)
│   ├── profit.py              ← Query pre-aggregated profit
│   ├── inventory.py           ← Query inventory snapshots
│   └── overview.py            ← Query overview KPIs
├── charts/
│   ├── sales_charts.py        ← Plotly figure builders (moved from services/)
│   ├── profit_charts.py
│   └── inventory_charts.py
├── pages/
│   ├── home.py                ← Overview dashboard
│   ├── sales.py               ← Sales page (1,100 lines, slimmed)
│   ├── sales_drilldown.py
│   ├── inventory.py
│   ├── customer.py
│   └── operational.py         ← DELETED (replaced by external ops tool)
├── components/
│   ├── layout.py              ← Header, nav, Mantine provider
│   ├── loading_modal.py
│   └── filters.py             ← Date pickers, preset buttons
├── assets/
│   └── custom.css
├── docker/
│   ├── Dockerfile             ← Gunicorn image ONLY
│   └── docker-compose.yml     ← Dash-app + Nginx (optional) + shared volume mount
├── requirements.txt           ← No odoorpc, no celery, no polars (or minimal polars for parquet reads)
└── tests/
    └── ui/                    ← Dash callback tests, snapshot tests

nkdash-admin/                  ← NEW LIGHTWEIGHT TOOL (optional Phase 2)
├── app.py                     ← Streamlit or Flask internal admin UI
├── ops/
│   ├── partition_scanner.py   ← Scan raw/clean/fact, show grid
│   ├── job_trigger.py         ← Enqueue Celery tasks via Redis (calls nkdash-pipelines tasks)
│   └── validator.py           ← Run parity tests
└── docker-compose.yml         ← Internal port, VPN-only access
```

---

## 3. The Data Contract (The Only Link Between Them)

Both projects agree on **file paths and schemas**. Nothing else is shared.

### Parquet Data Lake (shared volume / S3 bucket)
```
/data-lake/
├── raw/
│   ├── pos_order_lines/year=YYYY/month=MM/day=DD/*.parquet
│   ├── account_move_out_invoice_lines/...
│   ├── account_move_in_invoice_lines/...
│   ├── inventory_moves/...
│   └── stock_quants/...
├── clean/
│   └── (same hierarchy)
└── star-schema/
    ├── fact_sales/year=YYYY/month=MM/day=DD/*.parquet
    ├── fact_invoice_sales/...
    ├── fact_purchases/...
    ├── fact_inventory_moves/...
    ├── fact_stock_on_hand_snapshot/...
    ├── fact_product_cost_events/...
    ├── fact_product_cost_latest_daily/...
    ├── fact_sales_lines_profit/...
    ├── agg_profit_daily/...
    ├── agg_profit_daily_by_product/...
    ├── agg_sales_daily/...
    ├── agg_sales_daily_by_product/...
    ├── agg_sales_daily_by_principal/...
    ├── dim_products.parquet
    ├── dim_categories.parquet
    ├── dim_brands.parquet
    └── dim_taxes.parquet
```

### SQLite Dashboard Cache (optional, produced by pipelines or dashboard)
```
/data-lake/cache/nkdash.duckdb         ← Pipelines may continue to use DuckDB for ETL
/data-lake/cache/nkdash_metrics.db     ← Dashboard SQLite MVs (if kept)
```

**Critical rule:** `nkdash-ui` never writes to `/data-lake/star-schema/`. It is read-only.

---

## 4. Detailed Migration Steps

### Phase 0: Pre-Migration Hardening (1–2 days)
**Goal:** Make the split safe. Freeze schema changes during migration.

1. **Pin all dependency versions** in current `requirements.txt` (already mostly pinned, but add `polars==x.y.z`, `duckdb==1.2.0`).
2. **Add a parity test suite** that compares dashboard query output before/after refactor.
   - Create `tests/parity/` with one test per dashboard page.
   - Run against current code, save expected outputs as JSON fixtures.
3. **Document current Docker volume mounts**:
   - `D:\data-lake:/data-lake` (Windows host path — note this for cross-platform issues)
   - `D:\logs:/app/logs`
4. **Branch:** Create `main` → `restructure-prep` branch. All migration work happens here.

### Phase 1: Extract `nkdash-pipelines` (3–5 days)
**Goal:** Move all ETL code out of the dashboard repo into a standalone project.

#### Step 1.1: Scaffold the new project structure
Create the directory tree shown in Target Architecture. Initialize as a new Git repo or a subdirectory if using a monorepo.

#### Step 1.2: Migrate `etl/` directory
- Copy `etl/extract/*`, `etl/pipelines/*`, `etl/cache.py`, `etl/config.py`, `etl/dimension_cache.py`, `etl/io_parquet.py`, `etl/metadata.py`, `etl/odoo_helpers.py`, `etl/odoo_pool.py` → `nkdash-pipelines/etl/`
- Refactor `etl_tasks.py`:
  - **Extract** Celery app + config → `celery_app.py`
  - **Extract** Polars transformation functions (clean_pos_data, clean_sales_invoice_lines, etc.) → `etl/transform/`
  - **Extract** star-schema writers → `etl/load/`
  - **Extract** profit calculation pure functions → `core/profit_calculator.py`
  - **Keep only** `@app.task` wrappers in `tasks/`

#### Step 1.3: Migrate operational scripts
- Move all `check_*.py`, `refresh_*.py`, `rebuild_*.py` from root → `nkdash-pipelines/scripts/ops/`
- Move `scripts/backfill_sales_aggregates.py`, `scripts/benchmark_*.py`, `scripts/etl_data_manager*.py` → `nkdash-pipelines/ops_cli/`
- Consolidate duplicate scripts (e.g., `refresh_may_26_mv.py`, `refresh_may_2_27_mv.py`, `refresh_may_2_27_mv_v2.py` → one parameterized script).

#### Step 1.4: Docker separation
- Build `Dockerfile.worker` and `Dockerfile.beat` based on current `Dockerfile` but **remove Dash dependencies**.
- New `docker-compose.yml`:
  - `redis`
  - `celery-worker` (mounts `D:\data-lake:/data-lake`)
  - `celery-beat`
  - `etl-manager-cli` (for manual ops)
- **Remove** `dash-app` from this compose.

#### Step 1.5: Requirements cleanup
```
# nkdash-pipelines/requirements.txt
odoorpc==0.10.1
celery[redis]
redis
polars==1.x.y
duckdb==1.2.0
pyarrow
pydantic
python-dotenv
psutil
pandas
# NO dash, NO gunicorn, NO dash-mantine-components, NO dash-ag-grid
```

#### Step 1.6: Testing
- Run full backfill for 3 days.
- Verify parquet outputs match pre-migration fixtures.

### Phase 2: Slim `nkdash-ui` to Read-Only (2–3 days)
**Goal:** Remove all ETL imports and production capabilities from the dashboard.

#### Step 2.1: Delete ETL code from UI repo
- **Delete** `etl_tasks.py`
- **Delete** `etl/` directory (entirely)
- **Delete** `services/etl_ops.py`
- **Delete** `services/duckdb_connector.py` → Replace with `data_access/connection.py` using SQLite + Polars parquet reads only.
- **Delete** `pages/operational.py` (or move to `nkdash-admin/`)
- **Delete** root-level `check_*.py`, `refresh_*.py`, `rebuild_*.py`, `test_*.py` (those belong to pipelines)

#### Step 2.2: Create `data_access/` layer
```python
# nkdash-ui/data_access/connection.py
import os
import sqlite3
import polars as pl
from typing import Optional

DATA_LAKE = os.environ.get('DATA_LAKE_ROOT', '/data-lake')

def get_sqlite_connection() -> sqlite3.Connection:
    db_path = f"{DATA_LAKE}/cache/nkdash_metrics.db"
    return sqlite3.connect(db_path, check_same_thread=False)

def read_parquet_aggregate(path_pattern: str, columns: list[str]) -> pl.DataFrame:
    """Read pre-computed aggregate parquet with safe fallback."""
    full_path = f"{DATA_LAKE}/star-schema/{path_pattern}"
    try:
        return pl.read_parquet(full_path)
    except Exception:
        return pl.DataFrame({c: [] for c in columns})
```

#### Step 2.3: Rewrite `services/sales_metrics.py`
Current version imports `duckdb_connector`. New version:
```python
# nkdash-ui/metrics/sales.py
from datetime import date
import polars as pl
from data_access.connection import read_parquet_aggregate

def get_revenue_comparison(start_date: date, end_date: date) -> dict:
    df = read_parquet_aggregate(
        "agg_sales_daily/**/*.parquet",
        columns=["date", "revenue", "transactions", "items_sold", "lines"]
    )
    # Polars aggregation logic (no DuckDB, no Odoo)
    ...
```

#### Step 2.4: Docker cleanup
- New `docker-compose.yml`:
  - `dash-app` only
  - Mounts `D:\data-lake:/data-lake:ro` (read-only mount if OS supports; otherwise honor via convention)
  - No Redis dependency (unless used for dashboard query cache only)
- `Dockerfile` removes `odoorpc`, `celery`, `duckdb` (unless DuckDB is kept for parquet reads — but prefer Polars).

#### Step 2.5: Requirements cleanup
```
# nkdash-ui/requirements.txt
dash==2.14.2
dash-mantine-components==2.4.0
dash-ag-grid==31.2.0
gunicorn==23.0.0
plotly
polars          # For fast parquet reads only
pandas          # For Plotly compatibility
Flask-Caching==2.1.0
redis           # Only if keeping dashboard cache
python-dotenv
# NO odoorpc, NO celery, NO duckdb, NO pydantic (unless needed elsewhere)
```

### Phase 3: Build `nkdash-admin` (Optional, 2 days)
**Goal:** Replace `pages/operational.py` with a dedicated internal ops tool.

Options:
1. **Streamlit app** (`streamlit`) — Fastest for internal tools. 200 lines vs 1,378.
2. **Flask admin** — If you want to keep it Pythonic but lighter than Dash.
3. **Prefect/Dagster UI** — If you switch orchestrators, you get this for free.

If keeping it lightweight Streamlit:
```python
# nkdash-admin/app.py
import streamlit as st
from ops.partition_scanner import scan_dataset_partitions
from ops.job_trigger import enqueue_refresh

st.title("NKDash ETL Operations")

dataset = st.selectbox("Dataset", ["pos", "invoice_sales", ...])
start_date = st.date_input("Start")
end_date = st.date_input("End")

if st.button("Scan Partitions"):
    rows = scan_dataset_partitions(dataset, start_date, end_date)
    st.dataframe(rows)

if st.button("Trigger Backfill"):
    job_id = enqueue_refresh(dataset, start_date, end_date)
    st.success(f"Queued job {job_id}")
```

This connects to Redis and enqueues tasks in `nkdash-pipelines` — it does not import them.

### Phase 4: Orchestrator Upgrade (1–2 weeks, future sprint)
**Goal:** Replace Celery Beat with a proper DAG orchestrator.

Current Celery Beat schedule is brittle:
```python
# Hard-coded offsets (2:00, 2:05, 2:10, ...)
# If one pipeline fails, downstream pipelines still run and may process stale data.
```

**Dagster** or **Prefect** model:
```python
# nkdash-pipelines/orchestration/daily_dag.py (Prefect example)
from prefect import flow, task
from tasks.extract_tasks import extract_pos_order_lines
from tasks.transform_tasks import clean_pos_data
from tasks.load_tasks import update_star_schema

@flow(name="daily_etl")
def daily_etl_flow(target_date: str):
    raw = extract_pos_order_lines(target_date)
    clean = clean_pos_data(raw, target_date)
    update_star_schema(clean, target_date)
    # Downstream profit flow waits for upstream:
    daily_profit_flow(target_date)
```

Benefits:
- **Observability:** See which step failed, retry single tasks.
- **Backfill UI:** Built-in date-range reprocessing.
- **No custom polling code:** The `operational.py` polling loop (`etl-ops-bulk-poll` every 2s) disappears.

### Phase 5: Cloud-Ready Volume Abstraction (future)
**Goal:** Remove Windows host path dependency (`D:\data-lake`).

Current:
```yaml
volumes:
  - D:\data-lake:/data-lake
```

Target:
```yaml
volumes:
  - data-lake:/data-lake  # Named Docker volume
  # OR S3:
  # environment:
  #   - DATA_LAKE_PROTOCOL=s3
  #   - AWS_ACCESS_KEY_ID=...
```

In `nkdash-pipelines`, use `fsspec` or DuckDB's S3 support. In `nkdash-ui`, Polars reads S3 directly:
```python
pl.read_parquet("s3://nkdash-lake/star-schema/agg_sales_daily/**/*.parquet")
```

---

## 5. File-by-File Migration Map

| Current Path | Destination in Pipelines | Destination in UI | Action |
|--------------|---------------------------|-------------------|--------|
| `app.py` | — | `app.py` (slimmed) | Remove health check ETL logic |
| `etl_tasks.py` | Split to `celery_app.py`, `tasks/*.py`, `core/*.py` | — | **Delete** from UI |
| `etl/extract/*.py` | `etl/extract/*.py` | — | Move |
| `etl/pipelines/*.py` | `etl/pipelines/*.py` | — | Move |
| `etl/config.py` | `config/settings.py` | — | Refactor to Pydantic |
| `services/duckdb_connector.py` | `etl/duckdb_etl.py` (ETL-only views) | `data_access/connection.py` (SQLite) | Split |
| `services/etl_ops.py` | `ops_cli/etl_manager.py` | — | Move |
| `services/sales_metrics.py` | — | `metrics/sales.py` | Rewrite: no DuckDB import |
| `services/profit_metrics.py` | — | `metrics/profit.py` | Rewrite: no DuckDB import |
| `services/inventory_metrics.py` | — | `metrics/inventory.py` | Rewrite: no DuckDB import |
| `services/sales_charts.py` | — | `charts/sales_charts.py` | Move + adjust imports |
| `services/overview_metrics.py` | — | `metrics/overview.py` | Rewrite: no DuckDB import |
| `pages/operational.py` | — | **DELETE** | Replaced by `nkdash-admin` |
| `pages/sales.py` | — | `pages/sales.py` | Keep, but change imports |
| `pages/inventory.py` | — | `pages/inventory.py` | Keep, but change imports |
| `scripts/etl_data_manager*.py` | `ops_cli/` | — | Move |
| `scripts/backfill*.py` | `orchestration/backfill.py` | — | Move + consolidate |
| `check_*.py` (root) | `scripts/ops/` | — | Move |
| `refresh_*.py` (root) | `scripts/ops/` | — | Consolidate |
| `tests/test_profit_etl.py` | `tests/unit/` | — | Move |
| `tests/test_sales_parity.py` | — | `tests/parity/` | Move |
| `.github/workflows/main.yml` | `.github/workflows/pipelines.yml` | `.github/workflows/ui.yml` | Split CI |

---

## 6. Dependency Boundary Rules

After migration, enforce these with linting (e.g., `import-linter` or custom pre-commit hooks):

### `nkdash-ui` Forbidden Imports
```python
# These must never appear in nkdash-ui:
odoorpc          # No ERP connection
celery           # No task queue
from etl_tasks   # No ETL module
from services.etl_ops  # No orchestration
duckdb           # Prefer polars for reads (or SQLite for MVs)
```

### `nkdash-pipelines` Forbidden Imports
```python
# These must never appear in nkdash-pipelines:
dash             # No UI framework
dash_mantine_components
dash_ag_grid
gunicorn         # No web server
plotly           # No charting (unless generating static exports)
from pages       # No dashboard pages
```

---

## 7. Operational Continuity During Migration

You cannot stop the business while refactoring. Here's the parallel-run strategy:

| Day | Action | Risk |
|-----|--------|------|
| 1–2 | Set up `nkdash-pipelines` in parallel, run ETL to a **separate** `data-lake-v2/` | Zero risk |
| 3–4 | Compare `data-lake/` vs `data-lake-v2/` with parity tests | Zero risk |
| 5 | Switch Celery Beat to write to `data-lake/` from new pipelines repo (same path) | Low: old repo still has code as backup |
| 6 | Point `nkdash-ui` (new slimmed repo) at `data-lake/` | Medium: rollback to old `app.py` if needed |
| 7 | Delete ETL code from original repo once stable | Low |

**Safety rule:** Keep the original `nkdash` repo intact until `nkdash-ui` + `nkdash-pipelines` have run in production for **one full ETL cycle** (24h + scheduled beat tasks).

---

## 8. Immediate Quick Wins (Do These Today)

Even before the full migration, these reduce coupling immediately:

### 8.1 Delete `pages/operational.py` from the dashboard
- Move it to a standalone `tools/etl_admin.py` script.
- Run via CLI: `python tools/etl_admin.py --scan pos --from 2026-05-01 --to 2026-05-31`
- Benefit: Removes 1,378 lines of ETL UI from the web process.

### 8.2 Remove `force_refresh_day` imports from `app.py`
The `app.py` health check currently does:
```python
from services.duckdb_connector import get_duckdb_connection, ensure_duckdb_view_groups
conn.execute("SELECT 1 FROM agg_sales_daily LIMIT 1")
```
Change to:
```python
# Simple file-existence health check (no DuckDB connection needed)
import os
def health_check():
    latest_agg = f"{DATA_LAKE}/star-schema/agg_sales_daily/..."
    return jsonify({'status': 'healthy' if os.path.exists(latest_agg) else 'stale'})
```

### 8.3 Consolidate root scripts
Merge `refresh_may_26_mv.py` + `refresh_may_2_27_mv.py` + `refresh_may_2_27_mv_v2.py` into one:
```bash
python scripts/refresh_mv.py --date 2026-05-26 --dataset mv_name
```

### 8.4 Add `__init__.py` boundaries
In `services/__init__.py`, explicitly expose only dashboard-safe modules:
```python
# nkdash/services/__init__.py
from .sales_metrics import get_revenue_comparison, get_top_products
from .profit_metrics import query_profit_summary
# DO NOT export etl_ops, duckdb_connector
```

---

## 9. Success Metrics

Define done for this restructuring:

| Metric | Target | How to Measure |
|--------|--------|----------------|
| ETL code in UI repo | 0 files | `find nkdash-ui -name "*.py" | xargs grep -l "celery\\|odoorpc\\|etl_tasks"` → empty |
| Dashboard code in Pipelines repo | 0 files | `find nkdash-pipelines -name "*.py" | xargs grep -l "dash\\|gunicorn\\|plotly"` → empty |
| `pages/operational.py` exists | False | File deleted |
| `docker-compose up` in UI repo | Starts in <5s | Timer |
| `docker-compose up` in Pipelines repo | Starts workers + beat | Visual check |
| Parity tests pass | 100% | `pytest tests/parity/` |
| Dashboard query latency | <2s (unchanged) | Browser/Perf timer |
| ETL runtime | <30min (unchanged) | Logs |

---

## 10. Appendix: Why Not a Monorepo?

You could keep both in one Git repo (`nkdash/` with `packages/ui/` and `packages/pipelines/`). This is valid if:
- You have a small team (< 5 people)
- You want atomic commits across both
- You use a monorepo tool (Nx, Pants, Bazel, or even just `uv` workspaces)

However, I recommend **two repos** because:
1. **Different deploy cadences:** Dashboard deploys daily (UI tweaks). Pipelines deploy weekly (careful validation).
2. **Different access patterns:** BI analysts commit to UI. Data engineers commit to pipelines. Separate repos = separate CODEOWNERS + CI + review rules.
3. **Different scaling:** UI may move to Vercel/Netlify static hosting (if later switching to React frontend). Pipelines stay on Docker/VM.

If you prefer monorepo, use **directory boundaries** and enforce the dependency rules above with CI linting.

---

*End of Plan*
