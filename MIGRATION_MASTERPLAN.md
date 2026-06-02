# NKDash Migration Masterplan
## Solo Mode: 1 Container, 3 Processes, No Celery, Pure DuckDB, Bootstrap-First

**Prepared:** 2026-06-02 (Subtask 0.1 → 0.2 revised)
**Platform:** Local Docker (now) → Oracle Cloud Free Tier (future)
**Execution Model:** supervisord (gunicorn + streamlit + python-schedule)
**Data Lake:** Local Parquet on bind-mounted volume, queried via DuckDB in-memory
**Budget:** $0 (bootstrap phase)

---

## 1. Architecture Decision (Revised After Troubleshoot 0.2)

### Original Assumption
Multi-service Render deployment with shared persistent disk.

### Blockers Found
1. **Render Persistent Disk cannot be shared across services.** [Render Community](https://community.render.com/t/how-do-i-share-a-disk-between-services/1478)
2. **Render Cron Jobs cannot mount disks at all.** [Render Docs](https://render.com/articles/how-render-handles-scheduled-tasks)
3. **Fly.io removed permanent free tier in 2024.** [Fly.io Pricing 2026](https://costbench.com/software/developer-tools/flyio/) — only $5 trial credit.
4. **Render Free tier:** no persistent disk, web service sleeps after 15 min inactivity.

### Bootstrap-First Decision
> **Develop and validate locally in a single Docker container.** When ready to deploy, use **Oracle Cloud Free Tier** (Always Free: 2 VMs + 200GB storage, never expires). No paid services during bootstrap.

**Why Oracle Cloud Free Tier fits NKLab:**
- **Always Free, never expires** — not a trial [Oracle Cloud Docs](https://cloudpricecheck.com/free-tier/oracle)
- **ARM VM: 4 OCPU + 24GB RAM** — more than enough for Dash + Streamlit + DuckDB
- **200GB block storage** — sufficient for years of retail Parquet data
- **Docker-native** — deploy the exact same container built locally
- **$0 forever** — ideal for bootstrapping until revenue justifies upgrade

### Why We Don't Need an External Database Service
DuckDB is **embedded**. The "database" is the Parquet files in `/data-lake`. There is no server to host, no connection string to manage, no free tier to hunt for. This eliminates a major cost and complexity category entirely.

---

## 2. Current vs. Target Architecture

### Current (Monolith, 5 Services, Fragile, Celery-Heavy)
```
nkdash/
├── app.py                     ← Dashboard + ETL health check + DuckDB init
├── etl_tasks.py               ← 2,200-line god file: Celery + Polars + business logic
├── services/
│   ├── duckdb_connector.py    ← 1,677 lines, DOUBLE get_readonly_connection() bug
│   ├── etl_ops.py             ← ETL orchestration
│   ├── sales_metrics.py       ← Dashboard queries (imports duckdb_connector)
│   └── ...
├── pages/
│   ├── operational.py         ← 1,378-line ETL admin INSIDE dashboard (WRONG)
│   ├── sales.py               ← BI page
│   └── ...
├── etl/
│   ├── extract/               ← Odoo extractors
│   ├── pipelines/             ← Celery pipeline definitions
│   └── ...
├── scripts/                   ← Mix of setup, backfill, diagnostics
├── check_*.py                 ← 20+ one-off scripts polluting root
└── docker-compose.yml         ← 5 services (redis, celery-worker, celery-beat, etl-cli, dash-app)
```

### Target (Solo Mode, 1 Container, 3 Processes, Zero Cost)
```
nkdash/                        ← ONE repo, modular code separation, single container
├── app.py                     ← Dash app ONLY (public-facing BI)
├── scheduler.py               ← Python schedule background ETL loop
├── admin/
│   ├── app.py                 ← Streamlit admin UI (maintainer-only)
│   ├── pages/
│   │   └── operational.py     ← Moved & rewritten from pages/operational.py
│   └── components/
│       └── status_badge.py    ← ETL health indicator for embedding in dashboard
├── etl/                       ← Modular ETL package (was 2,200-line god file)
│   ├── extract/               ← Odoo RPC extractors (unchanged logic)
│   ├── transform/             ← Polars cleaning logic (extracted from etl_tasks.py)
│   ├── load/                  ← Parquet writers + star-schema builders
│   ├── core/                  ← Pure business logic (zero framework dependencies)
│   │   ├── profit_calculator.py   ← Tax-adjusted cost + margin logic
│   │   ├── cost_engine.py         ← Beginning costs + cost events
│   │   └── schema.py              ← Parquet schemas, path constants
│   └── tasks.py               ← Python schedule task registry (plain functions)
├── services/
│   ├── duckdb_connector.py    ← REWRITTEN: ~200 lines, in-memory DuckDB ONLY
│   ├── sales_metrics.py       ← DuckDB in-memory → Parquet aggregates
│   ├── profit_metrics.py      ← DuckDB in-memory → Parquet aggregates
│   ├── inventory_metrics.py   ← DuckDB in-memory → Parquet aggregates
│   └── overview_metrics.py    ← DuckDB in-memory → Parquet aggregates
├── pages/
│   ├── home.py                ← Overview dashboard
│   ├── sales.py               ← Sales performance
│   ├── sales_drilldown.py     ← Deep-drill sales
│   ├── inventory.py           ← Inventory KPIs
│   ├── customer.py            ← Customer analytics
│   └── executive.py           ← NEW: C-level one-page summary
│   └── operational.py         ← DELETED (moved to admin/)
├── config/
│   └── settings.py            ← Pydantic settings (env vars, data lake path)
├── docker/
│   ├── Dockerfile             ← Single image: Python + supervisord + nginx
│   ├── supervisord.conf       ← 3 processes: gunicorn, streamlit, scheduler
│   └── nginx.conf             ← Reverse proxy: / → :8050, /admin → :8501
├── docker-compose.yml         ← 1 service + bind mount ./data-lake:/data-lake
├── requirements.txt           ← Removed: celery, redis. Added: streamlit, schedule
└── tests/
    ├── parity/                ← Output comparison: old vs new (must match)
    ├── unit/                  ← ETL transform tests (Polars assertions)
    └── ui/                    ← Dash callback tests
```

**Runtime (inside one container, local or Oracle Cloud):**
```
┌─────────────────────────────────────────────────────────┐
│              supervisord (PID 1)                        │
├─────────────────┬─────────────────┬─────────────────────┤
│    gunicorn     │    streamlit    │    python schedule  │
│    Dash BI      │    Admin UI     │    ETL runner       │
│    :8050        │    :8501        │    (no port)        │
│    PUBLIC       │    /admin       │    BACKGROUND       │
│  C-Level + Ops  │  You (Maintainer)│  Automated daily    │
└─────────────────┴─────────────────┴─────────────────────┘
│                  /data-lake (bind mount)                │
│     raw/  clean/  star-schema/  admin/logs/           │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Data Contract (The Only Shared Surface)

All three processes agree on this directory tree. No process imports code from another.

```
/data-lake/                          # Bind-mounted volume (local) or Block Volume (Oracle)
├── raw/                             # ETL writes; Dashboard/Admin read-only
│   ├── pos_order_lines/year=YYYY/month=MM/day=DD/*.parquet
│   ├── account_move_out_invoice_lines/...
│   ├── account_move_in_invoice_lines/...
│   ├── inventory_moves/...
│   └── stock_quants/...
├── clean/                           # ETL writes; Dashboard/Admin read-only
│   └── (same hierarchy)
├── star-schema/                     # ETL writes; Dashboard/Admin read-only
│   ├── fact_sales/...
│   ├── fact_invoice_sales/...
│   ├── fact_purchases/...
│   ├── fact_inventory_moves/...
│   ├── fact_stock_on_hand_snapshot/...
│   ├── fact_product_cost_events/...
│   ├── fact_product_cost_latest_daily/...
│   ├── fact_sales_lines_profit/...
│   ├── agg_profit_daily/...
│   ├── agg_profit_daily_by_product/...
│   ├── agg_sales_daily/...
│   ├── agg_sales_daily_by_product/...
│   ├── agg_sales_daily_by_principal/...
│   ├── dim_products.parquet
│   ├── dim_categories.parquet
│   ├── dim_brands.parquet
│   └── dim_taxes.parquet
└── admin/                           # Internal operational data
    ├── etl_queue.sqlite             # Streamlit writes job request; scheduler polls & executes
    ├── etl_state.json               # Last run timestamps, success/failure status
    └── logs/
        ├── etl_YYYYMMDD.log         # Per-day ETL logs
        └── scheduler.log            # Scheduler daemon log
```

**I/O Rules:**
- `scheduler` process: **WRITE** to `raw/`, `clean/`, `star-schema/`, `admin/logs/`, `admin/etl_state.json`.
- `gunicorn` process: **READ-ONLY** from `star-schema/` via DuckDB `:memory:` views.
- `streamlit` process: **READ-ONLY** from `star-schema/` and `admin/logs/`. **WRITE** only to `admin/etl_queue.sqlite`.

---

## 4. Technology Stack Replacement

| Current (Overkill) | Target (Bootstrap-Fit) | Reason |
|---|---|---|
| Celery + Redis + Beat | Python `schedule` library | Single-process, no broker, no network, zero config |
| 5 Docker services | 1 Docker container + supervisord | Local-first, no cloud distributed systems needed |
| Celery task queue | SQLite queue file (`admin/etl_queue.sqlite`) | File-based IPC, survives container restart |
| SQLite MV hybrid | DuckDB in-memory over Parquet | User mandate: "tidak ada hibrida yang memusingkan" |
| `pages/operational.py` (1,378 lines inside Dash) | Streamlit Admin (`admin/pages/operational.py`) | ETL ops should not be in BI dashboard |
| `etl_tasks.py` (god file, 2,213 lines) | Modular `etl/` package | Maintainability, testability |
| Render deployment | Local Docker → Oracle Cloud Free Tier | $0 bootstrap, no sleep/timeout restrictions |

---

## 5. Deployment Pathway

### Phase A: Local Development (Now — $0)
```bash
# Build and run on your laptop/PC
docker-compose up --build

# Access:
# Dashboard  → http://localhost:8050
# Admin UI   → http://localhost/admin (via nginx reverse proxy)
# Direct admin (dev) → http://localhost:8501

# Data lake persists in ./data-lake (bind mount)
```

### Phase B: Validation (Before Any Cloud Deploy)
```bash
# Run full ETL backfill locally
python -m scheduler.main --backfill 2025-02-01 2026-06-01

# Verify parity
pytest tests/parity/

# Benchmark queries
curl http://localhost:8050/health
```

### Phase C: Oracle Cloud Free Tier (Future — $0)
```
1. Create Oracle Cloud account (credit card for verification only, no charge)
2. Launch Always Free ARM VM: 2 OCPU + 4GB RAM + 50GB block volume
3. Install Docker on VM
4. scp docker-compose.yml + Dockerfile + repo to VM
5. docker-compose up -d
6. Configure security list: open port 8050 (or 80 via nginx)
7. Data lake lives on block volume mounted to /data-lake
```

**Oracle Cloud Free Tier Allocation for NKDash:**
| Resource | Free Limit | NKDash Usage | Status |
|---|---|---|---|
| ARM Compute | 4 OCPU + 24GB RAM | 2 OCPU + 4GB RAM | ✅ Within limit |
| Block Storage | 200GB total | 50GB (2+ years retail data) | ✅ Within limit |
| Egress | 10TB/month | <1GB (dashboard HTML+JSON) | ✅ Within limit |

### Phase D: Paid Upgrade (When Revenue Justifies)
When business needs exceed free tier (e.g., multiple users, faster ETL, redundancy):
- **Render Starter** ($7/month): simplest migration from local Docker, but still 1 service
- **Oracle Cloud Pay-as-you-go**: scale same VM to paid tier, no re-architecture
- **Hetzner Cloud** (~€4/month): cheap VPS alternative with persistent disk

---

## 6. Migration Phases & Subtasks

### Phase 0: Foundation & Audit ✅
- **0.1** Research Render topology — **BLOCKED by free tier constraints**
- **0.2** Revise for bootstrap: local-first + Oracle Cloud Free Tier — **DONE**

### Phase 1: ETL Engine Modularization (Code-Only, No Deploy Change)
**Goal:** `etl_tasks.py` 2,213 lines → modular `etl/` package. Remove Celery. Keep running locally with existing docker-compose.

- **1.1** Create `etl/core/` — Extract pure business logic from `etl_tasks.py`
  - `profit_calculator.py`: `_build_product_cost_events`, `_latest_cost_by_product`, `_build_cost_snapshot_from_events`, `_build_sales_lines_profit`, `_build_profit_aggregates`
  - `cost_engine.py`: `_tax_multiplier_expr`, `_validate_beginning_costs`, `load_beginning_costs_from_csv`
  - `schema.py`: Path constants (`FACT_SALES_PATH`, etc.), Parquet schema definitions
- **1.2** Create `etl/transform/` — Extract Polars cleaning functions
  - `pos.py`: `clean_pos_data` logic
  - `invoices.py`: `clean_sales_invoice_lines`, `clean_purchase_invoice_lines`
  - `inventory.py`: `clean_inventory_moves`, `clean_stock_quants`
- **1.3** Create `etl/load/` — Extract star-schema writers
  - `fact_writer.py`: `_update_fact_sales_pos`, `_update_fact_invoice_sales`, etc.
  - `dimension_writer.py`: `refresh_dimensions_incremental` logic
- **1.4** Create `etl/tasks.py` — Plain Python function registry (NO `@app.task`)
  - Maps string task names to functions from `etl/extract/`, `etl/transform/`, `etl/load/`
- **1.5** Create `scheduler/main.py` — Python `schedule` loop
  - Daily at 02:00 WIB: run `daily_etl_pipeline(target_date=today)`
  - Poll `admin/etl_queue.sqlite` every 30s for manual job requests
- **1.6** Test locally: `python -m scheduler.main --dry-run 2026-06-01`

### Phase 2: Admin UI (Streamlit, Local)
**Goal:** Replace `pages/operational.py` with lightweight Streamlit app.

- **2.1** Create `admin/app.py` — Streamlit skeleton with sidebar navigation
- **2.2** Create `admin/pages/operational.py` — Port partition scanner from Dash AgGrid to Streamlit `st.dataframe`
- **2.3** Create `admin/pages/trigger.py` — Manual ETL trigger UI (writes to `admin/etl_queue.sqlite`)
- **2.4** Create `admin/pages/logs.py` — Log viewer (tail `admin/logs/etl_*.log`)
- **2.5** Create `admin/pages/health.py` — ETL status dashboard (reads `admin/etl_state.json`)
- **2.6** Test locally: `streamlit run admin/app.py`

### Phase 3: Dashboard Decoupling (Critical)
**Goal:** Dashboard becomes pure read-only consumer. No ETL imports.

- **3.1** Remove `pages/operational.py` from `app.py` `NAV_LINKS`
- **3.2** Delete `pages/operational.py` (already moved to `admin/`)
- **3.3** Audit & remove all `import odoorpc`, `from etl_tasks import`, `from services.etl_ops import` from `pages/` and `services/`
- **3.4** Rewrite `services/duckdb_connector.py` — Fix double `get_readonly_connection()` bug
  - Single definition: always returns `duckdb.connect(database=':memory:')`
  - Creates views via `CREATE VIEW ... AS SELECT * FROM read_parquet('/data-lake/...')`
  - No disk-backed connection for dashboard queries
- **3.5** Rewrite `services/sales_metrics.py` — Remove all SQLite MV imports, use DuckDB in-memory only
- **3.6** Rewrite `services/profit_metrics.py` — Same as above
- **3.7** Rewrite `services/inventory_metrics.py` — Same as above
- **3.8** Add `pages/executive.py` — C-level one-page summary (4 KPI cards + trend sparkline)
- **3.9** Add data freshness badge to `pages/home.py`, `pages/sales.py`, `pages/inventory.py`
- **3.10** Update `docker-compose.yml` — Single service with supervisord
- **3.11** Update `Dockerfile` — Install supervisord + nginx, copy `supervisord.conf`

### Phase 4: Validation & Hardening
**Goal:** Numbers identical, performance improved, local container stable.

- **4.1** Parity test suite: compare query output from old code vs new code for 3 sample dates
- **4.2** Local stress test: run ETL for 30 days backfill, verify container memory stays <2GB
- **4.3** Query latency benchmark: each dashboard page load <2s on local Docker
- **4.4** Admin queue test: trigger manual backfill from Streamlit, verify scheduler picks up and completes
- **4.5** Failure recovery test: kill scheduler mid-ETL, restart container, verify no data corruption

### Phase 5: Documentation & Cleanup
- **5.1** Update `docs/ARCHITECTURE.md` — Solo Mode architecture diagram
- **5.2** Update `SSOT.md` — Mark M10 complete, add Phase A/B/C/D deployment notes
- **5.3** Update `docs/runbook.md` — Local operations: `docker-compose up`, tail logs, manual trigger
- **5.4** Archive root-level `check_*.py` scripts to `admin/legacy_ops/` with README explaining they are deprecated
- **5.5** Update `README.md` — Bootstrap-first setup: "Clone → docker-compose up → open localhost:8050"
- **5.6** Add `docs/ORACLE_CLOUD_DEPLOY.md` — Future deploy guide (Phase C)

---

## 7. File Migration Map (Phase 1-3)

| Current Path | Target Path | Action | Lines (approx) |
|---|---|---|---|
| `etl_tasks.py` (2,213 lines) | `etl/core/*.py`, `etl/transform/*.py`, `etl/load/*.py`, `etl/tasks.py` | Split | 2,200 → 5 files |
| `etl/extract/*.py` | `etl/extract/*.py` | Keep logic, remove Celery context | ~500 |
| `etl/pipelines/daily.py` | `scheduler/main.py` | Rewrite for `schedule` library | ~180 |
| `services/etl_ops.py` | `admin/etl_ops.py` | Move + adapt for Streamlit | ~280 |
| `pages/operational.py` (1,378 lines) | `admin/pages/operational.py` | Move + convert to Streamlit | ~1,400 → ~250 |
| `services/duckdb_connector.py` (1,677 lines) | `services/duckdb_connector.py` | Rewrite: in-memory only | ~200 |
| `services/sales_metrics.py` | `services/sales_metrics.py` | Remove SQLite MV, DuckDB in-memory | ~130 |
| `services/profit_metrics.py` | `services/profit_metrics.py` | Remove SQLite MV, DuckDB in-memory | ~80 |
| `services/inventory_metrics.py` | `services/inventory_metrics.py` | Remove SQLite MV, DuckDB in-memory | ~TBD |
| `check_may_26_data.py` (root) | `admin/legacy_ops/README.md` + archive | Deprecate | 1 → doc |
| `refresh_may_*.py` (root, 4 files) | `admin/legacy_ops/` | Consolidate to 1 parameterized script | 4 → 1 |
| `Dockerfile` | `docker/Dockerfile` | Rewrite: supervisord + nginx base | ~30 |
| `docker-compose.yml` | `docker-compose.yml` | Rewrite: 1 service, bind mount | ~40 |
| `requirements.txt` | `requirements.txt` | Remove celery, redis. Add streamlit, schedule | ~20 |

---

## 8. Risk & Mitigation (Bootstrap Context)

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **Local disk fills up** (Parquet growth) | Medium | High | Monitor `df -h` on data-lake mount; old raw data can be archived to compressed tarball |
| **Oracle Cloud ARM out of capacity** | Medium | Medium | Fallback to AMD Micro (1/8 OCPU + 1GB RAM) — tight but workable with memory optimization |
| **ETL run exceeds 1GB RAM** (AMD fallback) | Medium | High | ETL writes direct to Parquet via Polars (streaming), no in-memory accumulation; schedule runs at night |
| **DuckDB file lock** (if ETL uses disk DuckDB) | Low | High | **Hard rule:** ETL writes Parquet via Polars only. Dashboard uses `:memory:` DuckDB only. Zero overlap. |
| **Streamlit admin exposed publicly** | Low | High | Nginx reverse proxy at `/admin` + basic auth. On Oracle Cloud, security list restricts port 8501. |
| **Single container = single point of failure** | Low | Medium | For bootstrap, acceptable. Auto-restart via Docker `--restart unless-stopped`. |
| **Container crash corrupts Parquet** | Very Low | High | Parquet is immutable (daily partitions). Corruption limited to 1 day. `admin/etl_state.json` tracks success. |

---

## 9. Success Criteria

| # | Criteria | How to Verify | Phase |
|---|---|---|---|
| 1 | `pages/operational.py` deleted from Dash nav and repo | `ls pages/operational.py` → "No such file" | 3 |
| 2 | Zero `import celery`, `import redis`, `import odoorpc` in `pages/` or `services/` | `grep -r "celery\|odoorpc\|redis" pages/ services/ --include="*.py"` → empty | 3 |
| 3 | Dashboard loads in <2s per page | Browser DevTools Network tab, local Docker | 4 |
| 4 | ETL backfill 1 day completes in <30 min | `time python -m scheduler.main --run 2026-06-01` | 4 |
| 5 | Streamlit admin can trigger manual ETL and view logs | Click test at `http://localhost:8501` | 2 |
| 6 | Parity tests pass (old output == new output ±0.5%) | `pytest tests/parity/ -v` | 4 |
| 7 | Container starts with `docker-compose up --build` and all 3 processes healthy | `docker ps` + `docker logs nkdash-app` | 3 |
| 8 | Zero cost in bootstrap phase | Credit card statement = $0 | A |

---

## 10. Appendix: Why Not Separate Repos?

Earlier plan suggested splitting to `nkdash-ui/` and `nkdash-pipelines/`. **Revised decision: keep one repo** for bootstrap phase because:

1. **Solo maintainer** — managing two repos = twice the git operations, twice the CI setup, twice the mental context switch.
2. **One container** — code separation is at module level (`etl/` vs `pages/` vs `admin/`), not repo level. Deploy unit is still one container.
3. **Atomic commits** — ETL schema change + dashboard query change can be committed together, tested together.
4. **Simple `docker-compose up`** — one `git clone`, one `docker-compose up`.

**Future split:** When team grows to 2+ developers (one frontend, one data engineer), then split to separate repos with shared data contract (Parquet schemas). For now, module-level separation is sufficient.

---

## 11. Next Subtask

**Subtask 1.1** — Create `etl/core/profit_calculator.py`, `etl/core/cost_engine.py`, `etl/core/schema.py` by extracting pure functions from `etl_tasks.py` lines 800-1600.

---

*End of Masterplan — Phase 0 (Revised)*
