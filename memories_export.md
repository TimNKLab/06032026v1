# Workspace Memories Export

This file contains all memories for the TimNKLab/nkdash workspace.

---

## Global Rules

### Universal AI Team Rulebook (Windsurf-Friendly, Project-Agnostic)

A reusable operational rule set for AI-assisted software teams. No required repo structure. No mandated file paths. Enforces invariants, not folders.

#### Rule 0 — Quality Over Speed
Choose the correct design, not the quickest patch.
- Prefer simple, readable architectures over cleverness.
- Avoid indirection (wrappers/shims/adapters) unless it clearly reduces complexity or risk.
- Make changes that improve the codebase's clarity and maintainability.

#### Rule 1 — Project Conventions and SSOT
Every project must define **one canonical "Single Source of Truth" (SSOT)** for planning and coordination (repo docs, ticketing system, ADR folder, etc.).

SSOT must cover (directly or by links):
- Current goals / phase
- Decisions (and decision history)
- Open questions
- Work tracking (TODOs/issues)
- How to run build/tests (or CI reference)

**Invariant:** planning + coordination live in SSOT, not scattered across chats.

**Precedence Order When Sources Conflict:**
1) Explicit user instruction (most recent, specific)
2) SSOT current phase / decision records
3) Code + tests (observed behavior)
4) Older logs / comments / historical notes

#### Rule 2 — Workstream Identity and Traceability
Track work by **workstream**, not by "conversation."

**Workstream ID:** Use a globally unique ID. Prefer, in order:
- PR number: `NK_PR1234_<slug>`
- Ticket ID: `NK_JIRA-123_<slug>`
- Otherwise: `NK_<YYYYMMDD>_<slug>_<rand4>`

**Traceability Tagging:** When modifying code, add a short trace tag in nearby comments only when it adds real value (non-obvious changes, tricky behavior, or surprising constraints):
```
// NK_PR1234: why this change exists (one sentence)
```

#### Rule 3 — Capability Declaration (Reality Check)
At the start of a workstream, explicitly state what you can and cannot do in this environment.

**Invariant:** Never claim you validated something you could not actually run or observe.

#### Rule 4 — Before Starting Work
Before implementing:
1) Read SSOT (current phase, constraints, conventions)
2) Check recent decisions + open questions
3) Identify baseline expectations (tests, golden files, snapshots, contracts)
4) If possible: ensure the project builds and tests pass before changes
5) Create or update the workstream log (location per SSOT)

#### Rule 5 — Behavioral Regression Protection
If the project defines behavioral baselines (golden files, snapshots, deterministic logs, API contract tests):
1) Run baseline checks (if possible) → must pass
2) Make changes
3) Re-run baseline checks
4) If output changes: treat as intentional change vs regression decision; document rationale and update baselines only with explicit user approval

**Invariant:** Baselines are contracts. Don't silently rewrite contracts.

#### Rule 6 — Compatibility and Breaking Changes
**Internal Refactors (no external consumers):** Prefer clean breaks and fix call sites directly. Avoid long-lived adapter layers. Document the refactoring scope and impact.

**External Contracts (public API, SDK, CLI, data formats, plugins):** Breaking changes require a migration plan (deprecation period or version bump, clear migration notes, compatibility layer allowed only if it reduces user harm and has a removal plan).

**Invariant:** Don't "paper over" design flaws with permanent compatibility hacks. But don't break consumers casually — always plan for migration and document the path forward.

#### Rule 7 — No Dead Code (With Explicit Exceptions)
Remove unused functions/modules, commented-out blocks, "kept for reference" code. Allowed exceptions must be explicit and justified (required reference implementation for a spec, regulated/audit constraints, migration bridge with a defined removal trigger).

#### Rule 8 — Modular Refactoring Standards
When splitting or reorganizing code:
- Each module owns its state; expose intentional APIs.
- Keep encapsulation strong (private by default).
- Avoid import graphs that are hard to reason about.
- Prefer smaller files/modules (human-readable; use project norms).
- Organize by responsibility (domain boundaries), not convenience.

#### Rule 9 — Questions and Decision Records (Don't Guess on Big Choices)
If requirements conflict, ambiguity affects architecture or user-facing behavior, a change could break contracts, or something feels "off," record it as a question in SSOT and seek clarification.

---

## User-Provided Memories

### Profit ETL Performance Optimizations Implemented
**Memory ID:** dc7bd031-6bc1-44da-abb9-5862bb196b14  
**Date:** Feb 6, 2026

#### Completed Optimizations

**1. Hive Partition Pruning Enabled (Highest ROI)**
- Updated all profit-related DuckDB views to use `hive_partitioning=1`
- Views affected: fact_product_cost_events, fact_product_cost_latest_daily, fact_sales_lines_profit, agg_profit_daily, agg_profit_daily_by_product
- Expected benefit: Faster file pruning for 30-day date range queries

**2. Caching Layer Added**
- Created services/profit_metrics.py with cached query functions
- Created services/profit_charts.py with cached chart builders
- Uses @cache.memoize() with 600s TTL (default)
- Functions cached: query_profit_trends, query_profit_by_product, query_profit_summary, and all chart builders

**3. Optimized Query Functions**
- Default to aggregate tables (agg_profit_daily*) for daily totals
- Optional drill-down to agg_profit_daily_by_product for product breakdown
- Line-level fact table (fact_sales_lines_profit) only for detailed analysis
- All queries include timing logs for performance monitoring

**4. Performance Monitoring Script**
- Created scripts/monitor_profit_performance.py
- Monitors file counts, partition distribution, and query performance
- Provides recommendations for compaction when needed
- Usage: python scripts/monitor_profit_performance.py --days 30 --verbose

#### Performance Characteristics
- Primary use case: Daily totals → drill-down to by-product (30-day range)
- Expected history: 30 days initially
- Query patterns optimized for this use case
- Partition pruning critical for date-range performance

#### Files Modified/Created
- services/duckdb_connector.py: Added hive_partitioning=1 to all profit views
- services/profit_metrics.py: New file with cached profit query functions
- services/profit_charts.py: New file with cached profit chart builders
- scripts/monitor_profit_performance.py: New performance monitoring script

---

### POS Refactor Implementation Approach Comparison
**Memory ID:** 586feb15-6d14-4077-bcfe-8e95dc00e745

**Backward-compatible vs zero-rebuild:**
- **Backward-compatible:** Keeps legacy parquet files, introduces v2 datasets, preserves reproducibility, allows dual-run validation, and avoids downtime.
- **Zero-rebuild:** Simplifies code, reduces storage, and avoids dual path maintenance, but loses historical reproducibility and carries higher risk during cutover.

**Decision:** Choose based on team tolerance for dual data paths vs risk tolerance.

---

### Data Policy Change for Sales Performance Page
**Memory ID:** 3bf3acc7-491a-4956-9db0-f18b2c3b00f0

**Policy:** Use daily pre-aggregated data as default, only query line-level fact tables for drilldowns.

**Details:**
- Default Sales page queries use pre-aggregated tables (agg_sales_daily, agg_sales_daily_by_product, agg_sales_daily_by_principal)
- Line-level fact tables (fact_sales, fact_invoice_sales, fact_sales_all) only used for detailed drilldown analysis
- This reduces query cost by orders of magnitude for typical 30-day date ranges

**Rationale:**
- Original issue: 30-day sales queries were taking way too long
- Root cause: Full table scans on fact_sales_all with derived date column preventing partition pruning
- WSL2 bind mount I/O penalty exacerbated the problem
- Solution: Pre-aggregate daily metrics during ETL, query aggregates instead of raw facts

**Aggregate Grain:**
- Daily totals: revenue, transactions, items_sold, lines
- By product: revenue, quantity, lines per product per day
- By principal (brand): revenue, quantity, lines per principal per day

**Trade-offs:**
- Pros: Sub-100ms query times for 30-day ranges; reduced I/O pressure; better cache hit rates
- Cons: Requires daily ETL to maintain aggregates; cannot query hourly patterns from aggregates (hourly heatmap still uses fact_sales_all)
- Mitigation: Hourly/detail queries are edge cases; aggregates cover 95%+ of Sales page use cases

**ETL Schedule:**
- Daily aggregates run at 2:12 AM
- Depends on POS sales (2:00 AM) and invoice sales (2:05 AM) pipelines completing first
- Backfilled for February 2025 to enable immediate testing

**Validation:**
- Performance validated: 30-day queries now 0.037s - 0.121s DuckDB time
- Data correctness: Aggregates derived from same fact sources as original queries

---

### Daily Sales Aggregates ETL Pipeline Implementation
**Memory ID:** 90ff4433-09c1-4bc7-a9e1-482cff12e3e2

**Implementation Details:**
- New aggregate tables: agg_sales_daily (daily totals), agg_sales_daily_by_product (by product), agg_sales_daily_by_principal (by principal/brand)
- ETL task: _build_sales_aggregates() reads from fact_sales and fact_invoice_sales, joins with dim_products for principal enrichment
- Celery task: update_sales_aggregates(target_date) writes partitioned parquet files with hive partitioning
- Pipeline: daily_sales_aggregates_pipeline() scheduled at 2:12 AM (after POS/invoice sales at 2:00/2:05 AM)
- Task routing: update_sales_aggregates routed to 'loading' queue

**DuckDB Views:**
- agg_sales_daily: date, revenue, transactions, items_sold, lines
- agg_sales_daily_by_product: date, product_id, revenue, quantity, lines
- agg_sales_daily_by_principal: date, principal, revenue, quantity, lines
- All views use hive_partitioning=1 for partition pruning

**Query Function Updates:**
- query_sales_trends: now uses agg_sales_daily
- query_top_products: now uses agg_sales_daily_by_product
- query_revenue_comparison: now uses agg_sales_daily
- query_sales_by_principal: now uses agg_sales_daily_by_principal
- query_overview_summary: now uses agg_sales_daily_by_product

**Performance (30-day range):**
- query_sales_trends: 0.054s DuckDB time
- query_revenue_comparison: 0.037s DuckDB time
- query_top_products: 0.121s DuckDB time
- query_sales_by_principal: 0.050s DuckDB time

**Files Modified:**
- etl/config.py: Added AGG_SALES_* path constants
- etl_tasks.py: Added _build_sales_aggregates, update_sales_aggregates, daily_sales_aggregates_pipeline; beat schedule entry; task routing
- etl/pipelines/daily.py: Added daily_sales_aggregates_pipeline_impl
- services/duckdb_connector.py: Added aggregate views; updated query functions to use aggregates

**Workstream:** NK_20260408_sales_aggregates_optimization_9d2e  
**Milestone:** M8 - Sales aggregates optimization (Validated 2026-04-08)

---

### Profit & Cost ETL Implementation
**Memory ID:** 94e0544e-8c58-4eaf-97e0-2d0296e54770

Profit & Cost ETL implementation completed and documented. Added comprehensive validation (unit tests + manual scripts), updated SSOT with workstream NK_20260206_profit_etl_9a2b (validated), extended DOCUMENTATION.md with profit ETL catalog and validation procedures, and created detailed implementation guide in docs/PROFIT_ETL_IMPLEMENTATION.md.

**Key features:**
- Tax-adjusted cost calculation
- Latest known cost rule
- Bonus item exclusion
- Daily profit aggregates
- DuckDB views
- Complete validation tooling

---

### ABC Analysis Change
**Memory ID:** 613f4364-8409-4155-8af5-bf8d3eec565d

ABC analysis in services/inventory_metrics.py was changed from cumulative revenue-share thresholds (e.g., A<=80% revenue) to SKU-share thresholds (default A=top 20% of SKUs by revenue, B=next 30% up to 50%, C=rest). Pareto curve still uses cumulative revenue share for display.

---

### SSOT Creation
**Memory ID:** 2fc043d5-d93d-41d4-8960-86142b1fbf38

Created canonical planning/coordination SSOT at d:/NKLabs/Plotly/nkdash/SSOT.md and linked it from README.md under 'Project SSOT'. SSOT includes goals/phase, milestone plan (M0–M5), oversight team roles/cadence/checklist, decision log, and active workstream NK_20260119_ssot_0001 (status Done). SSOT now codifies validation standard Option C: KPI spot-check vs Odoo + freshness + performance thresholds, with evidence recorded. Added acting-owner rule until named owners are assigned.

---

### ETL Tasks Optimization Analysis
**Memory ID:** d24175f9-3a18-4d87-98d5-eb7726b77792

#### Current State Assessment
- MV refresh integration successfully completed and operational
- All code quality issues in operational.py resolved
- System running smoothly in Docker environment
- User provided detailed architectural analysis for etl_tasks.py improvements

#### Identified Critical Issues in etl_tasks.py

**1. Massive Code Duplication (3000+ lines)**
- force_refresh_day() contains repetitive orchestration logic
- Similar patterns repeated across 7+ dataset types
- Difficult to extend and maintain

**2. Performance Bottlenecks**
- Repeated parquet scans and DataFrame operations
- Multiple DuckDB connection creation
- Inefficient data loading patterns

**3. Architectural Complexity**
- Single file handling multiple responsibilities
- Mixed concerns (ETL, storage, caching, scheduling)
- Poor separation of concerns

**4. Maintainability Issues**
- Repeated exception handling patterns
- Duplicated schema normalization logic
- Hardcoded dataset orchestration

#### Proposed Modular Architecture
```
etl/
├── tasks/
│   ├── extraction.py      # Data extraction tasks
│   ├── cleaning.py       # Data cleaning and validation  
│   ├── loading.py        # Data loading and writing
│   ├── dimensions.py     # Dimension management
│   ├── profit.py         # Profit calculation tasks
│   ├── aggregates.py     # Aggregate calculations
│   ├── orchestration.py  # Pipeline orchestration
│   └── materialized_views.py  # MV refresh tasks
├── pipelines/
│   ├── registry.py       # Dataset pipeline definitions
│   └── base.py          # Base pipeline class
├── utils/
│   ├── parquet.py        # Parquet operations
│   ├── duckdb.py         # DuckDB utilities
│   ├── redis.py          # Redis operations
│   └── decorators.py     # Task decorators
└── schemas/
    └── definitions.py   # Schema definitions
```

#### Implementation Strategy

**Phase 1: Foundation (High Priority)**
1. Create Pipeline Registry System
2. Extract Core Utilities
3. Implement Base Pipeline Class

**Phase 2: Migration (Medium Priority)**
1. Refactor force_refresh_day()
2. Split Large Functions

**Phase 3: Optimization (Low Priority)**
1. Performance Improvements
2. Enhanced Error Handling

#### Benefits Expected
- 50% reduction in code duplication
- Improved maintainability through modular structure
- Better performance via optimized data flows
- Enhanced reliability with proper error handling
- Easier testing through separated concerns

---

### Fact Purchases and Invoice Sales Implementation Approval
**Memory ID:** f82f233b-5fba-4f38-9275-892eadcb69ba

User approved plan for adding fact_purchases (invoices: move_type=in_invoice, state=posted) and fact_invoice_sales (out_invoice, posted) using line-grain from account.move.invoice_line_ids. Naming should follow Odoo semantics: purchases use vendor_*, sales use customer_*. Invoice lines with no product must be kept. Should update services/sales_metrics.py to include cleaned invoice sales in calculations. Prefer minimal disruption: keep existing POS fact_sales; new fact named fact_invoice_sales.

---

### Odoo Connection Pooling Implementation
**Memory ID:** 586fb957-3a6b-45fd-b20a-6822e864f141

etl_tasks.py now includes Odoo connection pooling with caching and metadata tracking, incremental dimension refresh/incremental merge logic (replacing full refresh via cleaned data), and Celery pipeline updates (new queue routing, beat schedule, parallel date pipeline).

---

### Data Lake Path Configuration
**Memory ID:** ff18b048-da7e-4566-ac13-b0893cf89099

Host data lake should live on Windows drive D:\data-lake, but inside Docker containers DATA_LAKE_ROOT must be /data-lake (bind-mounted from D:/data-lake:/data-lake). Avoid using Windows-style paths inside container code.

---

### Actual Price Implementation
**Memory ID:** 0642a131-4c4d-4e5b-8f89-7641cf785878

#### Status: ✅ DONE

**What was implemented:**
- Invoice-level discount percentage calculation in clean_purchase_invoice_lines()
- actual_price column logic: SKU lines apply discount, discount lines set to 0 (prevents double-discounting)
- actual_price included in fact_purchases star schema write
- actual_price exposed in DuckDB fact_purchases view

**Validation completed:**
- Move ID 102886 (Feb 10, 2025): 20% discount working correctly
- Move IDs 114526, 114624, 114635 (Feb 11, 2025): Consistent 20% discount behavior
- All discount lines have actual_price = 0
- SKU lines correctly discounted by invoice-level percentage
- No double-discounting occurs

**Next steps (planned for tomorrow):**
- Calculate actual revenue by adding taxes to actual_price
- Build new aggregated data table for revenue calculations

**Files modified:**
- etl_tasks.py: Added actual_price calculation logic
- services/duckdb_connector.py: Exposed actual_price in DuckDB view

---

### Cost & Profit Implementation Plan
**Memory ID:** 1b1fb020-4a71-492a-838f-2b0ff8ed21ec

#### User Objective
Implement tax-adjusted cost and gross profit calculation with new materialized aggregates for performance.

#### Key Decisions Made
1. **Cost Rule:** "Latest known cost" - use most recent purchase actual_price as of the sale date (not future prices)
2. **Tax Adjustments:** 
   - Purchase cost: tax_id IN (5,2) → 1.0x, tax_id IN (7,6) → 1.11x, default 1.0
   - Sales revenue: tax-included for both POS and invoice sales
3. **Bonus Items:** Exclude from cost calculation when actual_price <= 0 OR quantity <= 0
4. **Implementation:** Hybrid approach - ETL materialization for aggregates, DuckDB views for flexibility
5. **Audit Trail:** Record source_move_id for cost source tracking

#### Technical Design
**New Tables:**
- fact_product_cost_events: Grain=purchase line, filtered actual_price > 0
- fact_product_cost_latest_daily: Grain=date+product, latest cost as of each day
- fact_sales_lines_profit: Grain=sales line, with revenue_tax_in, cost_unit_tax_in, cogs_tax_in, gross_profit
- agg_profit_daily: Daily profit aggregates
- agg_profit_daily_by_product: Daily profit by product (optional)

**Cost Calculation Logic:**
- cost_unit_tax_in = actual_price * tax_multiplier
- For March 2025 sales: use most recent cost as of March 2025 (not Feb 2026 prices)
- Incremental daily updates using previous day's snapshot + today's cost events

#### Current Status
**Completed:**
- Added tax multiplier helper and parquet utilities in etl_tasks.py
- Added path constants and directory creation in etl/config.py
- Updated fact_invoice_sales to include tax_id for revenue calculations
- Updated fact_purchases to include actual_price (post-discount prices)

**In Progress:**
- Implement ETL tasks for cost events, latest daily cost, sales-line profit, and aggregates

**Pending:**
- Wire profit pipeline into daily scheduling
- Expose new tables/columns in DuckDB views
- Validation: spot-check tax adjustments, bonus item exclusion, profit calculations

#### Files Modified
- etl_tasks.py: Helpers, imports, tax_id inclusion
- etl/config.py: New paths for cost/profit aggregates
- services/duckdb_connector.py: actual_price exposed in fact_purchases view

#### User Clarifications
- Q: "if I were about to calculate revenue as of March 2025, what cost did I use?"
- A: Currently no cost data exists. After implementation: use latest known cost as of March 2025 (not future 2026 prices)
- Q: "with price_unit with purchase data as of March 2025 or prior, or today's price_unit, on which February 2026?"
- A: Use March 2025 latest cost for March 2025 sales; Feb 2026 sales would use Feb 2026 latest cost (which could be 2025 cost if no new purchases)

---

### Inventory KPI Implementation Plan
**Memory ID:** 964563ed-ad10-430e-b5a3-73d83475d08e

Added detailed inventory KPI/page implementation plan to SSOT.md (Section 9) and new milestone M6 + workstream NK_20260119_inventory_kpis_3f2a. Key finding: ABC analysis is computable now from fact_sales_all + dim_products. Stock Levels and Sell-through require adding a stock-on-hand snapshot dataset (proposed fact_stock_on_hand_snapshot from Odoo stock.quant; fallback product.product qty_available). Also proposed optional persistence of movement_type/adjustment flags in fact_inventory_moves. Includes UX plan (3 tabs), backend approach, and Option C validation steps.

---

### Odoo Data Sources Documentation
**Memory ID:** 7608fc64-62ab-4b99-ae1b-ce2b02599c79

Added comprehensive Odoo data sources documentation to SSOT.md (Section 11) and DOCUMENTATION.md (Section 2). Documented all 5 transactional tables (pos.order, account.move for sales/purchases, stock.move.line, stock.quant), 5 dimension tables, 4 derived/aggregate tables, ETL data flow, and key business rules. Provides complete reference for understanding what data is pulled from Odoo and how it's transformed.

---

### Stock Quant Snapshot Implementation
**Memory ID:** 03c78ff5-15ee-4f87-baed-c480be26003d

Implemented stock quant snapshot ETL and wiring (extract/save/clean/star schema + daily pipeline + DuckDB view) and built Stock Levels/Sell-through metrics, charts, and full inventory page UI + callbacks.

---

### ETL Data Purge Documentation
**Memory ID:** d480ee69-febb-48b0-b75f-482b893da3cc

Commands to purge /data-lake layers (fact_sales, clean/pos_order_lines, raw/pos_order_lines) and reset metadata via docker-compose run --rm celery-worker bash -c "..." have been documented in DOCUMENTATION.md under Troubleshooting. The ETL metadata reset script writes last_processed_date back to 2023-01-01.

---

### POS Refactor Decisions
**Memory ID:** 36afc688-4536-4c62-a379-98bd9b61e1dc

POS refactor decisions (Dec 27 2025): Source POS data from pos.order (expand lines and payments_id). Payment method is multi-valued (store all methods with amount>0; avoid exploding facts per payment). Include refunds/returns (allow negative qty/totals). Revenue for analytics defined as price_paid = lines.price_subtotal_incl - lines.x_studio_discount_amount (discount defaults to 0). Priority: keep existing dashboards stable first; implement ETL/clean/star-schema changes in a backward-compatible way, then extend dashboards later.

---

### Inventory Moves ETL Implementation
**Memory ID:** d3818f22-a16d-497a-8fe1-361c4cee9c4a

Added fact_inventory_moves ETL built from Odoo stock.move.line (executed moves) with joins to stock.move / stock.picking / stock.picking.type / stock.location. Data lake layers: raw/inventory_moves, clean/inventory_moves, star-schema/fact_inventory_moves. qty_moved signed based on internal->external (-) and external->internal (+). movement_type classified via picking_type_code (incoming/outgoing/internal), scrap via scrap_location, adjustment via usage=inventory, manufacturing via raw_material_production_id/production_id, returns via picking type name containing 'return'. Added daily_inventory_moves_pipeline and force refresh target 'inventory-moves'. Added DuckDB view fact_inventory_moves.

---

### ETL System: Odoo Integration & Field Update Guidelines
**Memory ID:** b23b12e1-5e9e-4523-a81a-b455ce2b0e9f

#### Components to Update for New Odoo Models

1. **ETL Layer**
   - etl_tasks.py: Add extraction functions following extract_pos_order_lines pattern
   - pos_data.py: Update processing logic for POS-related models
   - clean_*.py: Add model-specific cleaning functions

2. **Data Processing**
   - fact_*.py: New fact table handlers
   - dim_*.py: New dimension table handlers
   - aggregation_*.py: New aggregation logic

3. **Dashboard**
   - app.py: New callbacks/layouts
   - components/: New visualization components
   - pages/: New/updated pages

#### Common Error Patterns & Mitigations

1. **Field Reference Errors**
   - Use constants for field names
   - Implement test coverage
   - Use IDE refactoring tools

2. **Type Mismatches**
   - Explicit type casting in extraction
   - Schema validation in cleaning

3. **Dependency Issues**
   - Document data contracts
   - Version critical interfaces
   - Maintain backward compatibility

4. **Performance Issues**
   - Profile queries
   - Add indexes
   - Batch process large datasets

5. **Cache Invalidation**
   - Version cache keys
   - Implement invalidation strategies
   - Add cache warming

#### Best Practices

1. **Schema Evolution**
   - Use migration scripts
   - Maintain backward compatibility
   - Document deprecation timelines

2. **Testing**
   - Unit tests for extraction
   - Integration tests for pipelines
   - Edge case testing

3. **Monitoring**
   - Data quality checks
   - Performance monitoring
   - Anomaly alerts

4. **Documentation**
   - Update data dictionaries
   - Document field mappings
   - Maintain change logs

---

### Statistical Formulas YAML Request
**Memory ID:** e7b5be3b-68eb-4995-8a94-224ff2ddba90

Create a YAML file to collect all statistical formulas used in the system. This should include formulas for inventory metrics (ABC analysis, stock levels, sell-through), sales metrics (revenue calculations, growth rates), and any other KPI calculations. The YAML should serve as a centralized reference for all statistical formulas used in dashboards and reports.

---

### ETL Ops Operational Page Changes
**Memory ID:** e600afbf-0ccd-4483-919e-fee8d05a2a96

ETL Ops Operational page changes and fixes:

- Added Bulk Scan + Repair (All Datasets) feature: scans all datasets for missing/empty partitions across a date range, queues async Celery refresh only for affected days, and shows progress in a modal with a table and progress bar. Guardrail: max 31 days per bulk run.

- Sync mode: blocked for POS (web worker timeout risk). Added optional dimension refresh toggle for inventory_moves and stock_quants; runs once before date loop.

- Fixed DMC 2.4.0 LoadingOverlay usage (no children prop) by wrapping modal content in a relative Box and rendering overlay separately.

- Replaced async refresh logic to use the same per-day Celery task chain pattern as force-refresh scripts (extract -> save_raw -> clean -> update_fact) instead of trigger_dataset_refresh, ensuring actual data extraction occurs.

- Guardrails: max 31 days for async refresh; bulk repair only queues for Missing/Empty partitions; progress polls every 2s via AsyncResult.

All changes are contained within pages/operational.py only.

---

## Summary

This workspace contains 18 memories covering:
- Global rules for AI-assisted software development
- ETL system architecture and optimization plans
- Sales performance optimization through aggregation
- Profit and cost calculation implementation
- Inventory metrics and KPI implementation
- Odoo integration patterns and guidelines
- Data lake configuration and path management
- Various specific feature implementations (actual_price, stock quant snapshot, inventory moves, etc.)

**Total Memories:** 18  
**Date Range:** December 2025 - May 2026  
**Primary Focus:** ETL pipeline optimization, profit/cost calculations, inventory management, and Odoo data integration
