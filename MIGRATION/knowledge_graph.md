# NKDash Migration Knowledge Graph
**Workstream:** NK_20260602_migration_solo_render_0a1b
**Updated:** 2026-06-03 (Phase 3 Tasks 3.10–3.11 Complete — Docker Solo Mode Ready)

---

## Entity Types

### 🏛️ Architecture Patterns
| ID | Name | Status | Description |
|---|---|---|---|
| P1 | Celery Distributed Queue | ⛔ DEPRECATED | 5-service Docker Compose, Redis broker, task routing complexity |
| P2 | Render Solo Mode (original) | ⛔ REVISED | Assumed Render paid tier with shared disk — blocked by cost |
| P3 | **Local-First Solo Mode** | ✅ ACTIVE | 1 container, supervisord, 3 processes, bind-mounted data lake |
| P4 | Oracle Cloud Free Tier Deploy | 📋 PLANNED | Future Phase C: same container on Always Free ARM VM |

### 📦 Components
| ID | Name | Type | Current Path | Target Path | Status |
|---|---|---|---|---|---|
| C1 | ETL God File | Module | `etl_tasks.py` (2,213 lines) | `etl/` package | ✅ EXTRACTED — core/transform/load/tasks done |
| C2 | DuckDB Connector | Service | `services/duckdb_connector.py` | `services/duckdb_connector.py` (707 lines) | ✅ REWRITTEN — Pure in-memory, no singleton |
| C3 | ETL Admin UI | Page | ~~`pages/operational.py`~~ | `admin/pages/{1-4}_*.py` | ✅ DELETED from Dash — rewritten as 4 Streamlit pages |
| C4 | Sales Metrics | Service | `services/sales_metrics.py` | `services/sales_metrics.py` | ✅ CLEAN — pure DuckDB thin wrappers |
| C5 | Profit Metrics | Service | `services/profit_metrics.py` | `services/profit_metrics.py` | ✅ CLEAN — pure DuckDB thin wrappers |
| C6 | Inventory Metrics | Service | `services/inventory_metrics.py` | `services/inventory_metrics.py` | ✅ OPTIMIZED — zero Polars, zero .apply(), single DuckDB conn per function |
| C7 | Dashboard App | App | `app.py` | `app.py` | ✅ SLIMMED — no ETL imports, no operational page |
| C8 | Odoo Extractors | Module | `etl/extract/*.py` | `etl/extract/*.py` | ✅ KEEP |
| C9 | ETL Pipelines | Module | `etl/pipelines/*.py` | `scheduler/main.py` | ✅ REPLACED by scheduler |
| C10 | Streamlit Admin | App | `admin/app.py` | `admin/app.py` | ✅ CREATED (Phase 2) |
| C11 | Python Scheduler | Daemon | `scheduler/main.py` | `scheduler/main.py` | ✅ CREATED (Phase 1.5) |
| C12 | Executive Page | Page | `pages/executive.py` | `pages/executive.py` | ✅ CREATED (3.8) — 4 KPI cards + Revenue/Margin sparkline |
| C13 | ETL Core — Schema | Module | `etl/core/schema.py` | `etl/core/schema.py` | ✅ CREATED (1.1) |
| C14 | ETL Core — Cost Engine | Module | `etl/core/cost_engine.py` | `etl/core/cost_engine.py` | ✅ CREATED (1.1) |
| C15 | ETL Core — Profit Calculator | Module | `etl/core/profit_calculator.py` | `etl/core/profit_calculator.py` | ✅ CREATED (1.1) |
| C16 | ETL Transform — Utils | Module | `etl/transform/_utils.py` | `etl/transform/_utils.py` | ✅ CREATED (1.2) |
| C17 | ETL Transform — POS | Module | `etl/transform/pos.py` | `etl/transform/pos.py` | ✅ CREATED (1.2) |
| C18 | ETL Transform — Invoices | Module | `etl/transform/invoices.py` | `etl/transform/invoices.py` | ✅ CREATED (1.2) |
| C19 | ETL Transform — Inventory | Module | `etl/transform/inventory.py` | `etl/transform/inventory.py` | ✅ CREATED (1.2) |
| C20 | ETL Load — Raw | Module | `etl/load/raw.py` | `etl/load/raw.py` | ✅ CREATED (1.3) |
| C21 | ETL Load — Star Schema | Module | `etl/load/star_schema.py` | `etl/load/star_schema.py` | ✅ CREATED (1.3) |
| C22 | ETL Ops (Scanner) | Service | `services/etl_ops.py` | `services/etl_ops.py` | ✅ STRIPPED — scan-only, zero Celery dependency |
| C23 | POS Data Extractor | Service | ~~`services/pos_data.py`~~ | (removed) | ✅ DELETED — Odoo RPC, zero callers |
| C24 | Docker Compose Runner | Service | ~~`services/docker_compose_runner.py`~~ | `admin/legacy_ops/` | ✅ ARCHIVED — legacy 5-service utility |

### 🔧 Technologies
| ID | Name | Role | Status |
|---|---|---|---|
| T1 | Celery | Distributed task queue | ⛔ REMOVED from pages/services |
| T2 | Redis | Message broker | ⛔ REMOVED (Flask-Caching fallback only) |
| T3 | SQLite MV (hybrid) | Dashboard query cache | ⛔ REMOVED — replaced by DuckDB in-memory |
| T4 | DuckDB (disk-backed) | ETL + query (buggy) | ✅ FIXED — pure in-memory only |
| T5 | DuckDB (in-memory) | Dashboard query only | ✅ ACTIVE |
| T6 | Python `schedule` | Cron replacement | ✅ ACTIVE (scheduler/main.py) |
| T7 | Streamlit | Admin UI framework | ✅ ACTIVE (admin/app.py) |
| T8 | supervisord | Process manager | ✅ CONFIGURED (3.10) |
| T9 | nginx | Reverse proxy | ✅ NOT NEEDED (Solo Mode exposes ports directly) |
| T10 | Polars | ETL transformation | ✅ KEEP (ETL only — removed from inventory_metrics) |
| T11 | Plotly Dash | BI dashboard | ✅ KEEP |
| T12 | Parquet | Data lake format | ✅ KEEP |

### 📁 Data Lake Paths
| ID | Path | Writer | Reader | Format |
|---|---|---|---|---|
| D1 | `/data-lake/raw/` | Scheduler | Admin (read-only) | Parquet |
| D2 | `/data-lake/clean/` | Scheduler | Admin (read-only) | Parquet |
| D3 | `/data-lake/star-schema/` | Scheduler | Dashboard + Admin | Parquet |
| D4 | `/data-lake/admin/etl_queue.sqlite` | Admin (Streamlit) | Scheduler | SQLite |
| D5 | `/data-lake/admin/etl_state.json` | Scheduler | Admin + Dashboard | JSON |
| D6 | `/data-lake/admin/logs/` | Scheduler | Admin | Text |

### 🐛 Known Bugs
| ID | Description | Root Cause | Fix Strategy | Status |
|---|---|---|---|---|
| B1 | ~~`duckdb_connector.py` double `get_readonly_connection()`~~ | Two method definitions; second overrides first with disk-backed | Rewrite to pure in-memory | ✅ FIXED (3.4) |
| B2 | MV refresh stuck at Feb 2026 | `etl_tasks.py` monolith: profit pipeline fails, MV refresh runs before profit completes | Modular ETL with explicit ordering; scheduler orchestrates | ✅ MITIGATED — scheduler replaces Celery ordering |
| B3 | ~~`pages/operational.py` 1,378 lines~~ | ETL admin embedded inside BI dashboard | Move to `admin/` as Streamlit; remove from Dash nav | ✅ FIXED (3.2) — deleted, replaced by 4 Streamlit pages |
| B4 | Windows host path `D:\data-lake` | Local dev artifact hardcoded in `docker-compose.yml` | Parameterize via `.env` or named volume | ✅ FIXED (3.10) |
| B5 | **Memory leak — callers bypassing `_query_conn()`** | 4 locations called `get_duckdb_connection()` directly without `.close()`: `pages/inventory.py` (2x), `services/inventory_metrics.py` (2x), `app.py` health check (1x) | Wrap all calls in `_query_conn()` context manager | ✅ FIXED (3.4 audit) |
| B6 | **SIGKILL — inventory page OOM** | `inventory_metrics.py` opened 3× DuckDB (4GB each) + 2× Polars in single callback = ~12-16GB peak; 7 `.apply(axis=1)` row-by-row | Consolidated to single `_query_conn()` per function; replaced all Polars with DuckDB SQL; vectorized `.apply()` with numpy; reduced DuckDB limit to 2GB/conn | ✅ FIXED (3.7+ optimization) |

### 🔗 Relationships
```
[User: C-Level] --uses--> [C7: Dashboard App] --reads--> [D3: star-schema]
[User: Operational] --uses--> [C7: Dashboard App] --reads--> [D3: star-schema]
[User: Maintainer] --uses--> [C10: Streamlit Admin] --reads--> [D4, D5, D6]
[User: Maintainer] --uses--> [C10: Streamlit Admin] --writes--> [D4: etl_queue.sqlite]

[C11: Python Scheduler] --polls--> [D4: etl_queue.sqlite]
[C11: Python Scheduler] --writes--> [D1, D2, D3: data lake]
[C11: Python Scheduler] --writes--> [D5: etl_state.json]
[C11: Python Scheduler] --writes--> [D6: logs]

[C1: ETL God File] --refactored-to--> [C8: Odoo Extractors] + [etl/transform/] + [etl/load/] + [etl/core/]
[C2: DuckDB Connector] --rewritten--> [T5: DuckDB in-memory] (Dashboard side only)
[C3: ETL Admin UI] --deleted-from--> [C7: Dashboard App] --replaced-by--> [C10: Streamlit Admin]

[T1: Celery] + [T2: Redis] --replaced-by--> [T6: Python schedule]
[T3: SQLite MV] --replaced-by--> [T5: DuckDB in-memory] + [D3: Parquet star-schema]
[T4: DuckDB disk-backed] --replaced-by--> [T5: DuckDB in-memory only]

[C22: ETL Ops Scanner] --decoupled-from--> [C1: ETL God File] --now-only-scans--> [D3: Parquet files]
```

### 📊 Migration State Matrix

| Phase | Subtask | Status | Notes |
|---|---|---|---|
| 0 | 0.1 Render topology research | ✅ DONE | Found blockers: disk sharing, cron limitations |
| 0 | 0.2 Bootstrap constraint revision | ✅ DONE | Revised to Oracle Cloud Free Tier |
| 1 | 1.1 Extract `etl/core/` | ✅ DONE | profit_calculator, cost_engine, schema |
| 1 | 1.2 Extract `etl/transform/` | ✅ DONE | pos, invoices, inventory |
| 1 | 1.3 Extract `etl/load/` | ✅ DONE | raw, star_schema |
| 1 | 1.4 Create `etl/tasks.py` | ✅ DONE | Task registry, plain functions |
| 1 | 1.5 Create `scheduler/main.py` | ✅ DONE | Python schedule daemon |
| 1 | 1.6 Local dry-run test | ✅ DONE | |
| 2 | 2.1 Streamlit skeleton | ✅ DONE | admin/app.py + core.py + theme.py |
| 2 | 2.2 Scanner page | ✅ DONE | admin/pages/1_scanner.py |
| 2 | 2.3 Trigger page | ✅ DONE | admin/pages/2_trigger.py |
| 2 | 2.4 Logs page | ✅ DONE | admin/pages/3_logs.py |
| 2 | 2.5 Health page | ✅ DONE | admin/pages/4_health.py |
| 2 | 2.6 Queue protocol test | ✅ DONE | test_scheduler_queue.py |
| 3 | 3.1 Remove operational from NAV_LINKS | ✅ DONE | Removed from app.py navigation |
| 3 | 3.2 Delete `pages/operational.py` | ✅ DONE | Deleted + replaced by 4 Streamlit admin pages |
| 3 | 3.3 Audit & remove forbidden imports | ✅ DONE | pages/ clean; etl_ops.py stripped; pos_data.py + docker_compose_runner.py removed |
| 3 | 3.4 Rewrite `duckdb_connector.py` (in-memory) | ✅ DONE | 1,677→707 lines; no singleton; pure :memory:; _query_conn context manager |
| 3 | 3.5 Verify `sales_metrics.py` | ✅ DONE | Already clean — pure DuckDB wrappers, zero SQLite/MV |
| 3 | 3.6 Verify `profit_metrics.py` | ✅ DONE | Already clean — pure DuckDB wrappers, zero SQLite/MV |
| 3 | 3.7 Verify `inventory_metrics.py` | ✅ DONE | Zero Polars, zero .apply(), single DuckDB conn per function; SIGKILL-safe |
| 3 | 3.7a Archive `sqlite_manager.py` | ✅ DONE | Zero callers; archived to admin/legacy_ops/; 6 parity tests archived |
| 3 | 3.8 Add executive page | ✅ DONE | 4 KPI cards (Revenue, GP, COGS, Txns) + Revenue/Margin dual-axis sparkline |
| 3 | 3.9 Data freshness badge | ✅ DONE | Badge on home/sales/inventory; reads etl_state.json; auto-refresh 60s |
| 3 | 3.10 Docker compose rewrite | ✅ DONE | 5→1 service; supervisord; bind mount; Bug B4 fixed |
| 3 | 3.11 Dockerfile rewrite | ✅ DONE | python:3.11; supervisord entrypoint; 3 programs |
| 4 | 4.1 Parity tests | 📋 QUEUED | Depends on 3.5–3.7 |
| 4 | 4.2–4.5 Stress & benchmark | 📋 QUEUED | Depends on 4.1 |
| 5 | 5.1–5.6 Documentation | 📋 QUEUED | Depends on 4.5 |

### 🧠 Key Decision Chains

```
Bootstrap Constraint ($0)
    └─→ Render Paid Tier ❌ ( violates $0 )
    └─→ Fly.io Free Tier ❌ ( removed in 2024 )
    └─→ Oracle Cloud Free Tier ✅ ( Always Free, never expires )
        └─→ Single VM + Block Volume
            └─→ Single Docker Container
                └─→ supervisord (3 processes)
                    ├─→ gunicorn (Dash, public)
                    ├─→ streamlit (Admin, /admin)
                    └─→ python-schedule (ETL, background)
                        └─→ File-based data lake (Parquet)
                            ├─→ ETL writes via Polars
                            └─→ Dashboard reads via DuckDB in-memory
                                └─→ No SQLite hybrid ✅
                                    └─→ No Celery/Redis ✅
                                        └─→ No external database service ✅
                                            └─→ $0 monthly cost ✅
```

---

*This knowledge graph is updated after each subtask completion.*
