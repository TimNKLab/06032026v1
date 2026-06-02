# NKDash — Single Source of Truth (SSOT)

## Document Version
- **Version:** 3.0
- **Last Validated:** 2026-02-21 (post-refactor)
- **Next Review:** 2026-03-07
- **Change Log:** See `docs/ssot_changelog.md`

## 🚨 Critical Blockers
1. **Stock.quant dependency** - Inventory KPIs require daily stock snapshots (marked optional but critical)
2. **Ownership gaps** - Oversight team roles use placeholders (TEAM-001, etc.)

## Purpose
Canonical coordination document for NKDash repository. Links to authoritative docs, tracks milestones, and provides project oversight.

## Current Phase
**Migration & Decoupling (M10)** — Separate ETL engine from BI Dashboard codebase; replace Celery/Redis with Python `schedule`; deploy as Render Solo Mode. Dashboard menarik data dengan DuckDB in-memory; Admin pakai Streamlit.

## Quick Links (Authoritative Docs)
- **Architecture:** `docs/ARCHITECTURE.md` - Data lake + DuckDB architecture
- **Runbook:** `docs/runbook.md` - Operational procedures + troubleshooting  
- **Decisions:** `docs/decisions.md` - Append-only decision log
- **Inventory Spec:** `docs/inventory_spec.md` - Inventory KPIs + implementation plan
- **Performance Policy:** `docs/performance_policy.md` - Chart building + query optimization
- **Technical Docs:** `DOCUMENTATION.md`, `RELIABILITY.md`

## Milestone Status

| Milestone | Status | Completion Date |
|-----------|--------|-----------------|
| M0 — MVP dashboard | Validated | 2025-01-15 |
| M1 — Data lake + ETL decoupling | Validated | 2025-01-20 |
| M2 — Daily ETL pipelines | Validated | 2025-01-25 |
| M3 — Reliability (catch-up + health) | Validated | 2025-02-01 |
| M4 — Operational tooling | Validated | 2025-02-05 |
| M5 — Cloud + monitoring | Planned | TBD |
| M6 — Inventory KPIs | Done (code present) | 2025-02-10 |
| M7 — UI/UX enhancement | In Progress | 2026-02-21 |
| M8 — Sales aggregates optimization | Validated | 2026-04-08 |
| M9 — Sales aggregates backfill (Feb 2025–Feb 2026) | Validated | 2026-04-08 |
| M10 — Render Solo Mode Migration | In Progress | 2026-06-02 |

## Validation Standard (Option C)
- **Correctness:** ≤0.5% revenue variance vs Odoo (3-date sampling)
- **Freshness:** ETL metadata within 1 day of today
- **Performance:** ETL < 30min, dashboard queries < 2s
- **Evidence:** All validations recorded in decision log

## Oversight Team
**Acting owners** (real names needed):
- **Project Owner:** [NAME - TBD]
- **Technical Lead:** [NAME - TBD] 
- **Data/ETL Owner:** [NAME - TBD]
- **Dashboard Owner:** [NAME - TBD]
- **Ops/Release Owner:** [NAME - TBD]

**Current process:** Weekly 30min sync, acting owner = person implementing changes

## Active Workstreams
- **NK_20260602_migration_solo_render_0a1b** - Architecture migration to Render Solo Mode: single service, supervisord, pure DuckDB, no Celery (Phase 0 completed, Subtask 0.1 done)
- **NK_20260126_design_enhancement_4a7c** - UI/UX enhancement (DMC framework)
- **NK_20260408_ux_responsiveness_a1b2** - Dashboard UX responsiveness improvement (modal loading, explicit triggers, navigation cancellation)
- **NK_20260121_adjustments_8d9b** - Inventory adjustments handling (in progress)
- **NK_20260408_sales_aggregates_optimization_9d2e** - Sales aggregates ETL implementation for performance (validated, includes materialized views)
- **NK_20260408_historical_backfill_7e3f** - Historical sales aggregates backfill Feb 2025–Feb 2026 (validated, 1,203 files created)
- **NK_20260514_mv_refresh_stuck_0001** - MV refresh stuck issue (in progress)
- **NK_20260527_sell_through_migration_8f3a** - SQLite MV migration for sell-through query (validated)
- **NK_20260527_duckdb_cleanup_9f4b** - DuckDB cleanup for user-facing queries (validated)

## Team Log

### 2026-06-02: NK_20260602_migration_solo_render_0a1b - Repository Tree-shaking & Doc Consolidation
- **Tests Cleanup**: Archived obsolete tests (SQLite manager, old Docker compose runner, and deprecated DuckDB view tests) to `tests/archive/`.
- **Script Cleanup**: Moved root-level `check_*.py` and `refresh_*.py` scripts to `admin/legacy_ops/`.
- **Doc Consolidation**: 
  - Created `docs/DATA_LOGIC_GUIDE.md` as the unified source for business logic (merging Profit ETL, Inventory Spec, and Performance Policy).
  - Archived fragmented/outdated docs to `docs/archive/`.
- **Metadata Pruning**: Removed tool-specific metadata folders (`.kiro`, `.roo`, `.windsurf`).
- **Dependency Update**: Updated `requirements.txt` to fix `dash-extensions` conflicts and add `schedule` and `duckdb`.
- **Verification**: Verified `scripts/dry_run_pipeline.py` still passes after cleanup.


### 2026-06-02: NK_20260602_migration_solo_render_0a1b - Subtask 1.4 Complete (ETL Task Registry)
- **Scope:** Create `etl/tasks.py` as the plain Python function registry.
- **Implementation:** 
  - Created `TASK_REGISTRY` dictionary mapping string identifiers to extracted functions from `etl/core/`, `etl/transform/`, `etl/load/`, and `etl/extract/`.
  - Implemented `get_task(name)` helper for dynamic task retrieval.
  - Zero framework dependencies (no Celery decorators).
- **Refactoring items:**
  - Extracted `refresh_dimensions_incremental` from `etl_tasks.py` to `etl/extract/dimensions.py`.
  - Moved `load_beginning_costs_from_csv` to `etl/core/cost_engine.py`.
- **Verification:** Registry covers all atomic operations previously handled by Celery tasks in `etl_tasks.py`. Orchestration pipelines (e.g., `daily_etl_pipeline`) are deferred to `scheduler/main.py`.
- **Next subtask:** 1.5 — Create `scheduler/main.py` (Python `schedule` loop).

### 2026-06-02: NK_20260602_migration_solo_render_0a1b - Subtask 1.3 Complete (ETL Load Extraction)
- **Scope:** Extract star-schema writers and raw save functions from `etl_tasks.py` into `etl/load/` package.
- **Files created:**
  - `etl/load/raw.py` — `save_raw_data`, `save_raw_sales_invoice_lines`, `save_raw_purchase_invoice_lines`, `save_raw_inventory_moves`, `save_raw_stock_quants`, and helper `_save_raw_account_move_lines`.
  - `etl/load/star_schema.py` — `update_fact_inventory_moves`, `update_fact_sales_pos`, `update_fact_invoice_sales`, `update_fact_purchases`, `update_fact_stock_on_hand_snapshot`.
- **Key changes:** Removed `@app.task` decorators; logic now pure Polars writing to filesystem. `update_fact_sales_pos` now uses `etl.transform._utils.to_local_datetime` for timezone handling.
- **Dependency verification:** Clean (stdlib + polars + etl.config + etl.io_parquet + etl.transform._utils).
- **Next subtask:** 1.4 — Create `etl/tasks.py` as the plain Python function registry.

### 2026-06-02: NK_20260602_migration_solo_render_0a1b - Subtask 1.2 Complete (ETL Transform Extraction)
- **Scope:** Extract Polars cleaning functions from `etl_tasks.py` into `etl/transform/` package.
- **Files created:**
  - `etl/transform/__init__.py` — Package docstring explaining zero-framework rule.
  - `etl/transform/_utils.py` — `to_local_datetime` Polars expression. **Critical decoupling:** removed `app.conf.timezone` dependency (Celery app object); replaced with `os.environ.get('TZ', 'Asia/Jakarta')`.
  - `etl/transform/pos.py` — `clean_pos_data` (raw POS → cast types → compute revenue → clean Parquet).
  - `etl/transform/invoices.py` — `clean_sales_invoice_lines` + `clean_purchase_invoice_lines` (discount logic preserved exactly).
  - `etl/transform/inventory.py` — `clean_stock_quants` + `clean_inventory_moves` (dimension parquet joins preserved exactly).
- **Logic preservation:** All Polars expressions, column renames, filter conditions, null-fill defaults, and dimension join logic copied verbatim from `etl_tasks.py`. No behaviour changes.
- **Dependency verification:**
  - Imports: stdlib, `polars`, `etl.config`, `etl.io_parquet`, `etl.transform._utils`.
  - Forbidden imports (celery, redis, dash, flask, gunicorn, odoorpc, services.*): **absent**.
- **Syntax validation:** All 4 files parse successfully (`ast.parse`).
- **Note:** Original functions in `etl_tasks.py` intentionally **not deleted yet** — will be removed during Subtask 1.4 (wiring) to avoid breaking existing Celery task references until scheduler migration is complete.
- **Next subtask:** 1.3 — Extract `etl/load/` (star-schema writers + raw save functions) from `etl_tasks.py`.

### 2026-06-02: NK_20260602_migration_solo_render_0a1b - Subtask 1.1 Complete (ETL Core Extraction)
- **Scope:** Extract pure business logic from `etl_tasks.py` (lines 800-1600) into `etl/core/` package.
- **Files created:**
  - `etl/core/schema.py` — I/O helpers + reusable Polars schemas.
  - `etl/core/cost_engine.py` — Tax multiplier, cost validation.
  - `etl/core/profit_calculator.py` — Profit ETL functions, `_unified_costs` replaces DuckDB view dependency.
- **Key rewrite:** `build_sales_lines_profit` severed from `services.duckdb_connector`; `_unified_costs()` reads Parquet directly via Polars.
- **Dependency:** Clean (stdlib + polars + etl.config + etl.io_parquet only).
- **Next subtask:** 1.2 — Extract cleaning functions.

### 2026-06-02: NK_20260602_migration_solo_render_0a1b - Phase 0 Complete (Subtask 0.1 + Troubleshoot 0.2)
- **Subtask 0.1:** Render topology research completed. Found blockers: Render disk cannot be shared, Render Cron cannot mount disk, Fly.io removed free tier. Codebase finding: `duckdb_connector.py` double `get_readonly_connection()` definition causes file-lock conflicts.
- **Troubleshoot 0.2:** Bootstrap constraint clarification. User priority: $0 cost, local-first development, deployable container. Replaced Render paid tier with Oracle Cloud Free Tier (Always Free: 2 VMs + 200GB storage, never expires).
- **Architecture decision:** Solo Mode — 1 container, 3 processes (gunicorn + streamlit + python-schedule), supervisord, pure DuckDB in-memory for queries, Parquet data lake on local bind mount.
- **Documents updated:** `MIGRATION_MASTERPLAN.md`, `MIGRATION/reflection_0_1.md`, `docs/decisions.md`, `docs/ssot_changelog.md`.
- **Masterplan phases:** 5 phases defined (0→5), subtasks detailed, file migration map created, risk matrix updated for bootstrap context.

### 2026-05-27: NK_20260527_duckdb_cleanup_9f4b - DuckDB cleanup for user-facing queries
- **Changes:**
  - Migrated `query_inventory_summary()` to SQLite MVs + Polars (stock status calculations in Python)
  - Migrated `_query_location_ledger_deltas()` to Polars parquet reads (inventory movement deltas)
  - Migrated `get_inventory_costs()` to Polars parquet reads (product cost data)
  - Removed unused `get_stock_levels()` function (complex, not called by dashboard)
  - Removed DuckDB imports from inventory_metrics.py
  - Updated app.py `/api/mv-diagnostics` to check SQLite MVs
  - Updated app.py `/health` to check SQLite MVs
  - Removed `_precreate_views()` background thread from app.py
- **Validation:**
  - Parity test created: tests/test_inventory_cleanup_parity.py (7/7 passed)
  - Verified no DuckDB imports in inventory_metrics.py
  - Verified app.py endpoints use SQLite
  - Verified remaining DuckDB usage only in duckdb_connector.py (ETL operations)
- **Notes:**
  - Completes DuckDB cleanup for user-facing query operations
  - DuckDB now used ONLY for ETL operations (extraction, parquet creation)
  - SQLite used for all user-facing MVs and dashboard queries
  - Polars used for parquet reads during query operations
  - Architecture aligned with user preference: DuckDB=ETL, SQLite=queries

### 2026-05-27: NK_20260527_sell_through_migration_8f3a - SQLite MV migration for sell-through
- **Changes:**
  - Migrated `_query_sell_through()` from DuckDB to SQLite MVs + Polars parquet reads
  - Removed DuckDB dependencies (ensure_duckdb_view_groups, DuckDBManager, get_duckdb_connection)
  - Implemented movement_type classification logic in Python/Pandas (incoming, production_in, adjustment, production_out, transfer)
  - Used SQLite MVs: mv_inventory_daily (stock snapshots), mv_sales_by_product (sales aggregates)
  - Used Polars for parquet reads: fact_inventory_moves (movement data), dim_products (dimensions)
  - Used Pandas merge for cross-domain joins
- **Validation:**
  - Parity test created: tests/test_sell_through_parity.py (5/5 passed)
  - Verified SQLiteManager usage, Polars parquet reads, movement classification preservation
  - Verified sell-through ratio calculation preserved
- **Notes:**
  - Completes Phase 4 (Inventory Domain Migration) deferred item
  - Follows established migration pattern: SQLite MVs for aggregates, Polars for parquet reads, Pandas for joins
  - No new SQLite MV refresh logic needed (uses existing mv_inventory_daily and mv_sales_by_product)

### 2026-05-14: NK_20260514_mv_refresh_stuck_0001 - MV refresh stuck and view name fix
- **Changes:**
  - Added MV refresh status polling in `pages/operational.py` (task state tracking, 2-second polling)
  - Fixed MV reload view names in `services/duckdb_connector.py` (`_reload_mvs_background`)
  - View names corrected: `agg_sales_by_product` → `agg_sales_daily_by_product`, `agg_sales_by_principal` → `agg_sales_daily_by_principal`
- **Validation:** 
  - MV refresh button now shows real-time status (QUEUED/WAITING/RUNNING/SUCCESS/FAILURE)
  - View names match actual aggregate table names in DuckDB
- **Notes:** 
  - Root cause: `_reload_mvs_background()` used incorrect view names (missing `_daily` suffix)
  - Fix ensures MV reload uses correct source views: `agg_sales_daily_by_product`, `agg_sales_daily_by_principal`

## Key Data Sources
- **Transactional:** pos.order, account.move (sales/purchases), stock.move.line, stock.quant
- **Aggregates:** sales aggregates (daily, by-product, by-principal) - 401 days backfilled Feb 2025–Feb 2026
- **Dimensions:** product, category, brand, tax, partner
- **Derived:** Cost events, profit aggregates, inventory snapshots

## Next Steps
1. Continue M10 migration: refactor `etl_tasks.py` god file → modular `etl/` package (Subtask 1.1)
2. Build Streamlit Admin skeleton (Subtask 2.1)
3. Remove `pages/operational.py` from Dash nav_links (Subtask 3.1)
4. Assign real names to oversight roles
5. Make stock.quant snapshots mandatory for inventory KPIs
6. Complete M7 UI/UX enhancement

## Migration Artifacts
- `MIGRATION_MASTERPLAN.md` — Full migration roadmap
- `MIGRATION/reflection_0_1.md` — Subtask 0.1 research & troubleshooting findings
