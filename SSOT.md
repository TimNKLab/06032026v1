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
**Stabilization & Dataset Expansion** - Ensure reliable daily ETL pipelines while expanding dashboard capabilities.

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
- **NK_20260126_design_enhancement_4a7c** - UI/UX enhancement (DMC framework)
- **NK_20260408_ux_responsiveness_a1b2** - Dashboard UX responsiveness improvement (modal loading, explicit triggers, navigation cancellation)
- **NK_20260121_adjustments_8d9b** - Inventory adjustments handling (in progress)
- **NK_20260408_sales_aggregates_optimization_9d2e** - Sales aggregates ETL implementation for performance (validated, includes materialized views)
- **NK_20260408_historical_backfill_7e3f** - Historical sales aggregates backfill Feb 2025–Feb 2026 (validated, 1,203 files created)
- **NK_20260514_mv_refresh_stuck_0001** - MV refresh stuck issue (in progress)
- **NK_20260527_sell_through_migration_8f3a** - SQLite MV migration for sell-through query (validated)
- **NK_20260527_duckdb_cleanup_9f4b** - DuckDB cleanup for user-facing queries (validated)

## Team Log

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
1. Assign real names to oversight roles
2. Make stock.quant snapshots mandatory for inventory KPIs
3. Complete M7 UI/UX enhancement
4. Plan M5 cloud deployment strategy
