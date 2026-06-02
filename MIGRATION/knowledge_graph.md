# NKDash Migration Knowledge Graph
**Workstream:** NK_20260602_migration_solo_render_0a1b
**Updated:** 2026-06-02 (Subtask 0.1 + Troubleshoot 0.2)

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
| C1 | ETL God File | Module | `etl_tasks.py` (2,213 lines) | `etl/` package | 🔧 IN PROGRESS — `etl/core/`, `etl/transform/`, `etl/load/` extracted |
| C2 | DuckDB Connector | Service | `services/duckdb_connector.py` (1,677 lines) | `services/duckdb_connector.py` (~200 lines) | 🔧 TO REWRITE |
| C3 | ETL Admin UI | Page | `pages/operational.py` (1,378 lines) | `admin/pages/operational.py` | 🔧 TO MOVE |
| C4 | Sales Metrics | Service | `services/sales_metrics.py` | `services/sales_metrics.py` | 🔧 TO REWRITE |
| C5 | Profit Metrics | Service | `services/profit_metrics.py` | `services/profit_metrics.py` | 🔧 TO REWRITE |
| C6 | Inventory Metrics | Service | `services/inventory_metrics.py` | `services/inventory_metrics.py` | 🔧 TO REWRITE |
| C7 | Dashboard App | App | `app.py` | `app.py` | 🔧 TO SLIM |
| C8 | Odoo Extractors | Module | `etl/extract/*.py` | `etl/extract/*.py` | ✅ KEEP |
| C9 | ETL Pipelines | Module | `etl/pipelines/*.py` | `scheduler/main.py` | 🔧 TO REWRITE |
| C10 | Streamlit Admin | App | (new) | `admin/app.py` | 📋 TO CREATE |
| C11 | Python Scheduler | Daemon | (new) | `scheduler.py` | 📋 TO CREATE |
| C12 | Executive Page | Page | (new) | `pages/executive.py` | 📋 TO CREATE |
| C13 | ETL Core — Schema | Module | (new) | `etl/core/schema.py` | ✅ CREATED (1.1) |
| C14 | ETL Core — Cost Engine | Module | (new) | `etl/core/cost_engine.py` | ✅ CREATED (1.1) |
| C15 | ETL Core — Profit Calculator | Module | (new) | `etl/core/profit_calculator.py` | ✅ CREATED (1.1) |
| C16 | ETL Transform — Utils | Module | (new) | `etl/transform/_utils.py` | ✅ CREATED (1.2) |
| C17 | ETL Transform — POS | Module | (new) | `etl/transform/pos.py` | ✅ CREATED (1.2) |
| C18 | ETL Transform — Invoices | Module | (new) | `etl/transform/invoices.py` | ✅ CREATED (1.2) |
| C19 | ETL Transform — Inventory | Module | (new) | `etl/transform/inventory.py` | ✅ CREATED (1.2) |
| C20 | ETL Load — Raw | Module | (new) | `etl/load/raw.py` | ✅ CREATED (1.3) |
| C21 | ETL Load — Star Schema | Module | (new) | `etl/load/star_schema.py` | ✅ CREATED (1.3) |

### 🔧 Technologies
| ID | Name | Role | Status |
|---|---|---|---|
| T1 | Celery | Distributed task queue | ⛔ REMOVE |
| T2 | Redis | Message broker | ⛔ REMOVE |
| T3 | SQLite MV (hybrid) | Dashboard query cache | ⛔ REMOVE |
| T4 | DuckDB (disk-backed) | ETL + query (current buggy) | 🔧 FIX |
| T5 | DuckDB (in-memory) | Dashboard query only | ✅ TARGET |
| T6 | Python `schedule` | Cron replacement | 📋 ADD |
| T7 | Streamlit | Admin UI framework | 📋 ADD |
| T8 | supervisord | Process manager | 📋 ADD |
| T9 | nginx | Reverse proxy | 📋 ADD |
| T10 | Polars | ETL transformation | ✅ KEEP |
| T11 | Plotly Dash | BI dashboard | ✅ KEEP |
| T12 | Parquet | Data lake format | ✅ KEEP |

### 📁 Data Lake Paths
| ID | Path | Writer | Reader | Format |
|---|---|---|---|---|
| D1 | `/data-lake/raw/` | Scheduler | Admin (read-only) | Parquet |
| D2 | `/data-lake/clean/` | Scheduler | Admin (read-only) | Parquet |
| D3 | `/data-lake/star-schema/` | Scheduler | Dashboard + Admin | Parquet |
| D4 | `/data-lake/admin/etl_queue.duckdb` | Admin (Streamlit) | Scheduler | DuckDB |
| D5 | `/data-lake/admin/etl_state.json` | Scheduler | Admin + Dashboard | JSON |
| D6 | `/data-lake/admin/logs/` | Scheduler | Admin | Text |

### 🐛 Known Bugs
| ID | Description | Root Cause | Fix Strategy | Status |
|---|---|---|---|---|
| B1 | `duckdb_connector.py` double `get_readonly_connection()` | Two method definitions at lines 42-52 and 54-61; second overrides first with disk-backed connection | Delete second definition; enforce `:memory:` only for Dashboard | 🔧 PENDING |
| B2 | MV refresh stuck at Feb 2026 | `etl_tasks.py` monolith: profit pipeline fails, MV refresh runs before profit completes, incorrect view names in `_reload_mvs_background` | Modular ETL with explicit dependency ordering; correct view names | 🔧 PENDING |
| B3 | `pages/operational.py` 1,378 lines | ETL admin embedded inside BI dashboard | Move to `admin/` as Streamlit; remove from Dash nav | 🔧 PENDING |
| B4 | Windows host path `D:\data-lake` | Local dev artifact hardcoded in `docker-compose.yml` | Parameterize via `.env` or named volume | 🔧 PENDING |

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
[C3: ETL Admin UI] --moved-from--> [C7: Dashboard App] --to--> [C10: Streamlit Admin]

[T1: Celery] + [T2: Redis] --replaced-by--> [T6: Python schedule]
[T3: SQLite MV] --replaced-by--> [T5: DuckDB in-memory] + [D3: Parquet star-schema]
[T4: DuckDB disk-backed] --restricted-to--> [C11: Scheduler] (ETL transformations only)

[B1: Double get_readonly_connection] --causes--> [B2: MV refresh stuck]
[B2: MV refresh stuck] --mitigated-by--> [C1: ETL modularization]
```

### 📊 Migration State Matrix

| Phase | Subtask | Status | Blockers |
|---|---|---|---|
| 0 | 0.1 Render topology research | ✅ DONE | Render disk sharing |
| 0 | 0.2 Bootstrap constraint revision | ✅ DONE | None — revised to Oracle Free Tier |
| 1 | 1.1 Extract `etl/core/` | ✅ DONE | None |
| 1 | 1.2 Extract `etl/transform/` | ✅ DONE | None |
| 1 | 1.3 Extract `etl/load/` | ✅ DONE | None |
| 1 | 1.4 Create `etl/tasks.py` | ✅ DONE | None |
| 1 | 1.5 Create `scheduler/main.py` | ✅ DONE | None |
| 1 | 1.6 Local dry-run test | ✅ DONE | None |
| 2 | 2.1 Streamlit skeleton | 🔧 NEXT | None (parallelizable) |
| 2 | 2.2-2.5 Admin pages | 📋 QUEUED | Depends on 2.1 |
| 2 | 2.6 Queue protocol test | 📋 QUEUED | Depends on 1.5 + 2.5 |
| 3 | 3.1 Remove operational from nav | 📋 QUEUED | None (parallelizable) |
| 3 | 3.2-3.3 Delete/move imports | 📋 QUEUED | Depends on 2.1 |
| 3 | 3.4 Rewrite DuckDB connector | 📋 QUEUED | Depends on 1.1 |
| 3 | 3.5-3.7 Rewrite metric services | 📋 QUEUED | Depends on 3.4 |
| 3 | 3.8 Add executive page | 📋 QUEUED | None (parallelizable) |
| 3 | 3.9-3.11 Docker/compose rewrite | 📋 QUEUED | Depends on 1.5 + 2.1 |
| 4 | 4.1 Parity tests | 📋 QUEUED | Depends on 3.4-3.7 |
| 4 | 4.2-4.5 Stress & benchmark | 📋 QUEUED | Depends on 4.1 |
| 5 | 5.1-5.6 Documentation | 📋 QUEUED | Depends on 4.5 |

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
