# NKDash — Single Source of Truth (SSOT)

## 0) Capability declaration (this environment)
- I can read and modify files in this repo.
- I can propose commands to run on your machine, but I cannot claim CI or production validation unless you run it.

## 1) Purpose and scope
This document is the **canonical SSOT** for planning and coordination of this repository.

- It links to the authoritative technical docs already in the repo.
- It defines the **implementation plan milestones**, **current status tracking**, **decision log**, and **open questions**.
- It defines an **oversight team** and a recurring **progress-vs-plan review** process.

## 2) Canonical references (authoritative docs)
- `DOCUMENTATION.md`
  - Operational runbook + ETL task catalog (Bahasa Indonesia)
  - Troubleshooting and force-refresh commands
- `docs/ARCHITECTURE.md`
  - Data Lake + DuckDB architecture overview
- `RELIABILITY.md`
  - ETL reliability/catch-up/health monitoring notes
- `etl_tasks.py`
  - **ETL entry point**: Celery app, task names, schedule
- `docker-compose.yml`
  - Local stack definition (Redis, Celery worker/beat, Dash)

## 3) Current goals / phase
**Phase:** Stabilize local ETL+Dashboard stack and keep dashboards stable while extending datasets.

**Primary objectives (near-term):**
1. Ensure daily pipelines run reliably (freshness, catch-up, clear ops runbook).
2. Keep dashboard KPIs stable while ingest expands (POS + invoice sales + purchases + inventory moves).
3. Make progress visible and reviewable (status, metrics, and a repeatable oversight cadence).

## 4) Implementation plan (milestones) and progress tracking
Status legend:
- **Done (code present)** = implemented in repo (not necessarily validated end-to-end).
- **Validated** = Option C: KPI spot-check vs Odoo **and** performance thresholds met (see below).

### Validation standard (Option C)
When a milestone is marked **Validated**, it must meet all of the following.

1. **Correctness (KPI spot-check vs Odoo)**
   - For at least 3 representative dates (recommended: today-1, a recent peak day, and a quiet day), compare DuckDB outputs to Odoo:
     - revenue (daily total)
     - quantity sold (daily total)
     - order count (daily total)
   - Acceptance:
     - revenue matches within tolerance: **<= 0.5% relative difference** (or within rounding noise if currencies/rounding differ).
     - quantity and order counts match exactly (or differences are explained and recorded in Decision log).
2. **Freshness (no gaps)**
   - `ETLMetadata` last processed date is within 1 day of today (or gaps are explained and recorded).
3. **Performance**
   - Daily ETL pipelines complete within Celery task limits (hard limit 30 minutes) and do not routinely approach the limit.
   - Dashboard KPI queries remain responsive (target: common KPI queries < 2s on a typical dev machine).
4. **Evidence recorded**
   - The validation run date, sample dates, and results are recorded in this SSOT (or in a linked artifact).

### M0 — MVP dashboard (sync Odoo calls)
- **Status:** Done (code present)
- **Artifacts:** `README.md`, `app.py`, `pages/*`

### M1 — Data lake + ETL decoupling (Odoo → Parquet → DuckDB → Dash)
- **Status:** Done (code present)
- **Artifacts:** `docs/ARCHITECTURE.md`, `etl/`, `services/duckdb_connector.py` (view creation)

### M2 — Daily ETL pipelines (dataset expansion)
- **Status:** Done (code present)
- **Artifacts:** `etl_tasks.py` schedules + `etl/pipelines/daily.py`
- **Included pipelines:**
  - POS: `daily_etl_pipeline`
  - Invoice sales: `daily_invoice_sales_pipeline`
  - Purchases: `daily_invoice_purchases_pipeline`
  - Inventory moves: `daily_inventory_moves_pipeline`

### M3 — Reliability (catch-up + health checks)
- **Status:** Done (code present)
- **Artifacts:** `RELIABILITY.md`, scheduled `health_check` in `etl_tasks.py`, restart policies in `docker-compose.yml`

### M4 — Operational ergonomics (manual refresh tooling)
- **Status:** Done (code present)
- **Artifacts:** `scripts/*`, `manage-etl.bat`, `DOCUMENTATION.md` troubleshooting section

### M5 — Next steps (future)
- **Status:** Planned
- **Candidates (from `docs/ARCHITECTURE.md`):**
  - Benchmark runtime + query latency
  - Monitoring (Flower / metrics)
  - Cloud storage support

### M6 — Inventory KPIs and pages (stock levels, sell-through, ABC)
- **Status:** Done (code present)
- **Artifacts:** `pages/inventory.py`, `services/inventory_metrics.py`, `services/inventory_charts.py`
- **Note:** Inventory page is functional but does not yet handle inventory adjustments cleanly (see workstream NK_20260121_adjustments_8d9b).

### M7 — UI/UX Design Enhancement (DMC-based)
- **Status:** Planned
- **Goal:** Enhance visual design and user experience using existing Dash Mantine Components (DMC) framework
- **Scope:** Professional styling, modern layouts, improved data visualization, responsive design
- **Artifacts:** Enhanced `app.py`, `pages/*`, custom CSS, DMC design system
- **Plan:** See workstream NK_20260126_design_enhancement_4a7c

## 5) Oversight team (progress vs plan)
This is a lightweight team to **regularly compare actual progress to the plan** and keep work aligned.

### Roles (fill names)
- **Project Owner (PO):** TEAM-001
  - Owns priorities, acceptance criteria, and release readiness.
- **Technical Lead:** TEAM-002
  - Owns architecture decisions, implementation plan updates, and technical risk.
- **Data/ETL Owner:** TEAM-003
  - Owns correctness, freshness, and ETL performance.
- **Dashboard Owner:** TEAM-004
  - Owns KPI definitions, UX, and regression prevention.
- **Ops/Release Owner:** TEAM-005
  - Owns deploy/run procedures, secrets/env hygiene, and incident response.

### Acting owners (until names are assigned)
- The person running the weekly oversight meeting is the **acting PO** for that meeting.
- If a role is TBD, the **acting owner** is whoever is currently implementing changes in that area (ETL vs dashboard vs ops) for the active workstream.
- Any ownership ambiguity is resolved by the acting PO and recorded in the Decision log.

### Cadence
- **Weekly 30 minutes** (or twice-weekly during heavy refactors)

### Progress-vs-plan checklist (run every meeting)
1. **Milestone status**
   - For each milestone M0–M5: confirm status and list blockers.
2. **Data freshness**
   - Latest successful processing date (`ETLMetadata` / metadata JSON) vs today.
3. **Reliability signals**
   - Any failing scheduled pipelines? Any repeated retries? Any Redis instability?
4. **Dashboard regression check**
   - Critical KPIs still match expectations (spot check: today, last 7 days, last 30 days).
5. **Change log**
   - What changed since last review (code + data format + dashboard behavior).
6. **Decisions + risks**
   - Record new decisions (below) and track top 3 risks.

## 6) Decision log
Keep this brief and append-only.

- **2025-12 (repo docs):** `etl_tasks.py` remains the single ETL entry point; implementations may live under `etl/` but task names must remain stable.
- **2025-12 (repo docs):** Data lake root inside containers should be `/data-lake` (Windows host bind mount typically `D:\data-lake → /data-lake`).
- **2026-01 (ABC definition):** ABC classification changed from cumulative revenue-share to SKU-share thresholds (default A=top 20% SKUs by revenue, B=next 30%, C=rest). Pareto curve still uses cumulative revenue share for visualization.
- **2026-01 (Inventory adjustments):** Inventory adjustments and manufacturing (production output/consumption) break sell-through and days-of-cover. Planned workstream NK_20260121_adjustments_8d9b to tag moves, exclude them from receipts, and add reconciliation visibility.
- **2026-02 (Cost & Profit ETL):** Implemented tax-adjusted cost and gross profit calculation with materialized aggregates. Cost rule: "latest known cost" as of sale date (not future prices). Tax multipliers: purchase tax_id 5/2 → 1.0x, 7/6 → 1.11x, default 1.0. Bonus items (actual_price ≤ 0 or quantity ≤ 0) excluded from cost calculation. Daily pipeline scheduled at 02:20. Validated with unit tests and manual validation scripts.
- **2026-02 (Odoo Data Sources):** Documented all Odoo tables used in ETL: pos.order (POS), account.move (sales/purchases), stock.move.line (inventory), stock.quant (snapshots), plus dimensions (product, category, brand, tax, partner). Derived tables: cost events, latest daily cost, sales line profit, profit aggregates.

## 7) Work tracking (active workstreams)
Use globally unique workstream IDs.

- **NK_20260119_ssot_0001** — Establish SSOT + oversight cadence
  - **Status:** Done
  - **Deliverables:** `SSOT.md` created; link added to `README.md`.

- **NK_20260119_inventory_kpis_3f2a** — Inventory KPIs + Inventory page build-out
  - **Status:** Done (code present)
  - **Deliverables:** Stock quant snapshot ETL, inventory metrics, inventory page UI, stock levels/sell-through/ABC charts.

- **NK_20260206_profit_etl_9a2b** — Cost & Profit ETL implementation and validation
  - **Status:** Done (validated)
  - **Deliverables:** 
    - ETL tasks: `_build_product_cost_events`, `_build_product_cost_latest_daily`, `_build_sales_lines_profit`, `_build_profit_aggregates`
    - Celery tasks: `update_product_cost_events`, `update_product_cost_latest_daily`, `update_sales_lines_profit`, `update_profit_aggregates`
    - Daily pipeline: `daily_profit_pipeline_impl` scheduled at 02:20
    - DuckDB views: `fact_product_cost_events`, `fact_product_cost_latest_daily`, `fact_sales_lines_profit`, `agg_profit_daily`, `agg_profit_daily_by_product`
    - Validation: Unit tests (`tests/test_profit_etl.py`), manual validation scripts (`scripts/validate_profit_etl.py`, `scripts/run_profit_etl.py`)
  - **Validation evidence:** All 7 unit tests pass; tax multipliers correct; bonus item exclusion working; profit calculations accurate; DuckDB views accessible.

- **NK_20260121_adjustments_8d9b** — Inventory adjustments and manufacturing handling in reconciliation
  - **Status:** In progress
  - **Problem:** Adjustments (stock counts, write-offs, manual corrections) and manufacturing (production output/consumption) break sell-through and days-of-cover because they change on-hand without corresponding receipts/sales.
  - **Deliverables:**
    1. Tag moves in `fact_inventory_moves` with `movement_type` and `inventory_adjustment_flag` in ETL (including manufacturing: `'production_in'`, `'production_out'`). (Done)
    2. Update sell-through query to exclude or isolate adjustments/manufacturing from "units received". (Done)
    3. Add "Adjustments" and "Manufacturing +/-" columns to Sell-through table for reconciliation visibility. (Planned)
    4. Add a reconciliation KPI to Stock Levels: `Stock variance = (begin_on_hand + receipts - sales) - end_on_hand`. (Planned)
    5. Update UX: Add toggles to include/exclude adjustments and manufacturing output from sell-through calculations. (Planned)
  - **Notes:** This improves data reconciliation and prevents misleading KPIs when adjustments or manufacturing occur.

- **NK_20260126_design_enhancement_4a7c** — UI/UX Design Enhancement using DMC framework
  - **Status:** In Progress
  - **Goal:** Enhance visual design and user experience using existing Dash Mantine Components (DMC) framework
  - **Approach:** Maximize DMC 2.4.0 capabilities + custom CSS + modern design patterns
  - **Deliverables:**
    1. **DMC Design System:** Create consistent color palette, typography, spacing using DMC theme provider
    2. **Enhanced Layouts:** Implement card-based designs, grid systems, responsive navigation
    3. **Advanced Components:** Use DMC's Carousel, Stepper, Timeline, Loading overlays, transitions
    4. **Custom Styling:** Add modern effects (gradients, shadows, glassmorphism) via custom CSS
    5. **Data Visualization Enhancement:** Interactive charts with zoom, filters, custom color schemes
    6. **UX Improvements:** Dark/light mode toggle, breadcrumb navigation, beautiful forms
  - **Priority Pages:** Start with `pages/operational.py` (completed) and `app.py` header (completed)
  - **Validation:** Visual review + responsive testing on mobile/desktop
  - **Notes:** Avoid conflicts with existing DMC setup; enhance rather than replace components
  - **🎯 DESIGN POLICY UPDATE:** All pages now use `dmc.Container(size='100%', px='md', py='lg')` for full viewport width with proper edge padding

## 8) Open questions (needs your confirmation)
1. Who are the named owners for the oversight roles in section 5?
2. Is M5 (cloud + monitoring) in-scope for the next iteration, or should we freeze scope at stabilization?

## 9) Inventory pages — thorough plan

### 9.1 Scope
Create an inventory dashboard (currently `pages/inventory.py` is a placeholder) covering:
1. **Stock levels**
2. **Sell-through ratio**
3. **ABC analysis**

Implementation principle: compute from DuckDB views (data lake) as the default, only falling back to Odoo for validation/debugging.

### 9.2 KPI definitions and feasibility (with current datasets)

#### A) Stock Levels
**Business question:** “What do we have on hand today (and on a selected date), and which items are low / overstocked?”

**Recommended metric set:**
- On-hand units (and optionally reserved units) per product, optionally grouped by category/brand.
- Days of cover: `on_hand_units / avg_daily_units_sold` (trailing N days).
- Low stock flag: `days_of_cover < X` (configurable; start with X=7).
- Dead stock flag: `on_hand_units > 0 AND sold_units_last_N_days = 0`.

**Required fields:**
- `product_id`, `location_id` (optional), `on_hand_qty`, optionally `reserved_qty`, `snapshot_date`.

**Can we compute with current fetched data?**
- **Partially only.** Current `fact_inventory_moves` can provide **net changes**, but **not absolute on-hand** unless we have an opening balance.

**Plan to enable accurate stock levels:**
- Add a daily (or at least periodic) snapshot dataset from Odoo:
  - Preferred source: **`stock.quant`** (accurate on-hand by product + location).
  - Fallback: `product.product` fields such as `qty_available` (less granular, depends on Odoo settings and can be slower/less reliable).

#### B) Sell-through Ratio
**Business question:** “How efficiently did we sell inventory we had available during a time period?”

**Recommended definition (units):**
`sell_through = units_sold / (begin_on_hand + units_received)` for the selected period.

**Inputs needed:**
- `units_sold` from `fact_sales_all`.
- `begin_on_hand`, `end_on_hand` from stock snapshots (see Stock Levels).
- `units_received` from `fact_inventory_moves`:
  - default: `SUM(qty_moved)` where `qty_moved > 0` and movement corresponds to receipts into internal stock.
  - note: `qty_moved` sign already encodes internal/external boundary.

**Can we compute with current fetched data?**
- **Not accurately end-to-end** without stock snapshots for beginning inventory.
- We can compute partial components now (units sold; net received), but the ratio needs `begin_on_hand`.

#### C) ABC Analysis
**Business question:** “Which products contribute most to sales (or units) so we can prioritize attention?”

**Recommended definition:** SKU-share-based ABC per product over a selected period:
- A = top 20% of SKUs by revenue
- B = next 30% of SKUs by revenue (up to 50%)
- C = remaining SKUs
(thresholds configurable; defaults follow inventory best practices)

**Data needed:**
- revenue, quantity from `fact_sales_all` + product attributes from `dim_products`.

**Can we compute with current fetched data?**
- **Yes.** ABC analysis is feasible immediately from `fact_sales_all` + `dim_products`.
- Note: profit/margin-based ABC requires cost/COGS, which is not currently in the star schema (future enhancement).

### 9.3 Data model / ETL additions (minimal, inventory-focused)

#### A) Add `fact_stock_on_hand_snapshot`
**Why:** enables Stock Levels and Sell-through; unlocks days-of-cover and dead-stock.

**Proposed Odoo source:** `stock.quant`
- Grain: `(snapshot_date, product_id, location_id, lot_id?, owner_id?)`
- Fields (minimum):
  - `snapshot_date`
  - `product_id`
  - `location_id`
  - `quantity`
  - `reserved_quantity` (if available)

**Storage layout:**
- Raw: `/raw/stock_quants/year=YYYY/month=MM/day=DD/stock_quants_YYYY-MM-DD.parquet`
- Clean: `/clean/stock_quants/year=YYYY/month=MM/day=DD/stock_quants_clean_YYYY-MM-DD.parquet`
- Star schema: `/star-schema/fact_stock_on_hand_snapshot/year=YYYY/month=MM/day=DD/fact_stock_on_hand_snapshot_YYYY-MM-DD.parquet`

**DuckDB view:**
- `fact_stock_on_hand_snapshot` via `read_parquet(..., union_by_name=True)`.

#### B) Improve inventory-move classification (optional but recommended)
To support better filtering (adjustments/scrap/returns/receipts), consider persisting:
- `movement_type` and/or `inventory_adjustment_flag`
into `fact_inventory_moves`.

**Next step:** This is now planned as workstream NK_20260121_adjustments_8d9b.

### 9.4 Dashboard UX plan (inventory page)

#### Information architecture
Implement `pages/inventory.py` as a real page with 3 tabs:
1. **Stock Levels**
2. **Sell-through**
3. **ABC Analysis**

#### Common controls (shared across tabs)
- Date picker:
  - Stock levels: a single “As-of date”
  - Sell-through/ABC: date range
- Filters:
  - category, brand, product search
  - optional location (if we implement location-grain snapshots)
- Toggles:
  - include/exclude returns (affects sell-through)
  - include/exclude adjustments (if movement_type available)

#### Stock Levels tab
- KPI cards:
  - total on-hand units (sum)
  - count of low-stock SKUs
  - count of dead-stock SKUs
- Table:
  - SKU, on-hand, avg daily sold (N days), days of cover, flags
- Charts:
  - distribution of days-of-cover
  - top low-stock SKUs

#### Sell-through tab
- KPI cards:
  - sell-through % (overall)
  - units sold
  - units received
- Charts:
  - sell-through by category/brand
  - top/bottom SKUs by sell-through

#### ABC Analysis tab
- KPI cards:
  - A/B/C SKU counts
  - revenue share by class
- Charts:
  - Pareto curve (cumulative revenue)
  - ABC distribution by category/brand
- Table:
  - SKU, revenue, cumulative %, class

### 9.5 Backend implementation approach
- Use DuckDB SQL as the SSOT for metrics.
- Follow the existing pattern:
  - `pages/*` for layout + callbacks
  - `services/*` for metrics + chart builders
- Add inventory-specific query functions (new module or extend existing ones) that return `pandas.DataFrame`.

Performance guardrails:
- Avoid repeated full scans of `fact_sales_all` by using single-query aggregates and date predicates.
- Keep page callbacks coarse-grained (one query per tab update rather than per component).

### 9.6 Validation plan (Option C, inventory-specific)
For Stock Levels and Sell-through, validation requires comparing DuckDB-derived results against Odoo.

1. **Correctness**
   - Stock on hand: pick 20 SKUs and compare “as-of date” on-hand to Odoo (by location if enabled).
   - Receipts: pick 3 days and reconcile `SUM(qty_moved>0)` to Odoo stock moves/pickings.
   - Units sold: reconcile to Odoo sales report for same date range.
2. **Freshness**
   - Verify snapshot partition exists for yesterday (or selected as-of date).
3. **Performance**
   - Stock levels query (top N + aggregates) < 2s.
   - ABC query (Pareto + table) < 2s for typical ranges (30–90 days).
4. **Evidence recorded**
   - Add a short “Validation evidence” entry in this SSOT for each release of the inventory page.

## 10) Chart Building Policy and Query Strategy

### 10.1 Purpose
To ensure dashboards remain responsive and scalable as datasets grow, we adopt a consistent policy for building charts and executing queries. This section codifies the patterns and tools used in the `/sales` optimization workstream and serves as the reference for future pages.

### 10.2 Core policies

#### 10.2.1 Fail-fast to DuckDB; no Odoo fallbacks in runtime
- **Rule:** All dashboard metrics must be served from DuckDB (data lake) at runtime.
- **Implementation:** Remove or disable any Odoo RPC fallbacks in `services/*_metrics.py` and `pages/*` callbacks.
- **Rationale:** Guarantees predictable latency and prevents cascading timeouts.

#### 10.2.2 Server-side caching with TTL for expensive results
- **Tool:** `flask-caching` (Redis if `REDIS_URL` is set; otherwise SimpleCache).
- **Policy:** Apply `@cache.memoize()` to:
  - Expensive chart builders (`services/*_charts.py` functions that build Plotly figures).
  - Repeatedly used query results (e.g., overview summary used by multiple charts).
- **TTL:** Controlled by `DASH_CACHE_TTL_SECONDS` (default 600s in docker-compose.yml).
- **Keying:** Automatic by function signature (date range + parameters).
- **Rationale:** Eliminates repeated DuckDB scans and Plotly figure construction for identical date ranges.

#### 10.2.3 Progressive loading via dcc.Store (KPI first, then charts/tables)
- **Pattern:** Add a hidden `dcc.Store(id='*-query-context')` to the page layout.
- **Flow:**
  1. User clicks Apply → a single “KPI” callback runs queries and writes query context (start/end dates) to the store.
  2. Heavy chart/table callbacks trigger off the store (`Input('*-query-context', 'data')`), not the Apply button.
- **UX benefit:** KPIs render immediately; heavy visualizations populate shortly after.
- **Implementation notes:**
  - KPI callback must output the store data (ISO dates and minimal metadata).
  - Heavy callbacks must guard on missing store data (`if not data: raise PreventUpdate`).
  - Keep the store payload small (dates + flags); do not store large DataFrames.

#### 10.2.4 Consolidated queries; minimize per-callback DuckDB roundtrips
- **Goal:** Reduce the number of DuckDB queries per Apply to as few as possible.
- **Techniques:**
  - Use CTEs and FILTER clauses to compute multiple metrics in one scan.
  - Prefer DuckDB-side aggregation over Python-side `pivot_table`/groupby when feasible.
  - For complex charts (Sankey, Heatmap), return “edge list” or pre-pivoted shapes from DuckDB to avoid heavy Python loops.
- **Rationale:** Set-based work in DuckDB is faster than Python loops and reduces Python CPU/memory pressure.

#### 10.2.5 Timing instrumentation everywhere
- **Rule:** Every callback and every query function must log start/end timing.
- **Pattern:** Use a helper `_log_timing(name, start_time)` in callbacks; print query timing in DuckDB connector functions.
- **Output format:** `[TIMING] <function_name>: <elapsed_seconds>.3fs`
- **Rationale:** Enables data-driven performance tuning and rapid regression detection.

### 10.3 Query strategy guidelines

#### 10.3.1 Date predicates must be pushed down
- Always filter by date in DuckDB (`WHERE date >= ? AND date < ? + INTERVAL 1 DAY`).
- Avoid pulling full tables into Pandas and then filtering by date.

#### 10.3.2 Prefer single-scan aggregates
- Use a single query with GROUP BY/CASE WHEN to compute multiple KPIs (e.g., revenue, transactions, items) rather than N separate queries.
- Example: `query_revenue_comparison` uses FILTER to compute current vs previous period in one scan.

#### 10.3.3 Limit Python-side reshaping
- If a chart needs a pivot or edge list, try to produce that shape in DuckDB.
- For Heatmap: return `(date, hour, revenue)` rows from DuckDB; avoid `pivot_table` in Python.
- For Sankey: return `(source, target, value)` edges from DuckDB; limit top-N in SQL.

#### 10.3.4 Use LIMIT/OFFSET for large result sets
- Top-N queries should apply LIMIT in DuckDB (e.g., top 20 products).
- Do not fetch thousands of rows into Python only to slice.

### 10.4 Implementation checklist for new pages
- [ ] Add `dcc.Store(id='*-query-context')` to layout.
- [ ] Implement KPI callback that writes query context to store.
- [ ] Wire heavy callbacks to read from store (not Apply button).
- [ ] Add `@cache.memoize()` to chart builders and expensive query functions.
- [ ] Ensure all queries have date predicates and timing logs.
- [ ] Prefer single-scan aggregates; avoid per-callback full scans.
- [ ] Verify Docker Compose includes `DASH_CACHE_TTL_SECONDS` and `REDIS_URL` if using Redis.
- [ ] Test progressive loading: KPIs should appear before charts.
- [ ] Test cache hit: second Apply with same dates should be faster.

## 11) Odoo Data Sources and ETL Flow

### 11.1 Direct Table Pulls (Raw Extraction)

#### A. Transactional Data Tables

| Data Source | Odoo Table | Purpose | Key Fields |
|-------------|--------------|---------|------------|
| **POS Sales** | `pos.order` | Point-of-sale transactions | `id`, `date_order`, `partner_id`, `user_id`, `state`, `lines`, `payments_id` |
| **Invoice Sales** | `account.move` (filter: `move_type='out_invoice'`, `state='posted'`) | Customer invoices | `move_id`, `date`, `partner_id`, `product_id`, `quantity`, `price_unit`, `tax_id` |
| **Purchases** | `account.move` (filter: `move_type='in_invoice'`, `state='posted'`) | Vendor bills | `move_id`, `date`, `partner_id`, `product_id`, `quantity`, `price_unit`, `actual_price`, `tax_id` |
| **Inventory Moves** | `stock.move.line` (executed moves) | Stock movements | `move_id`, `date`, `product_id`, `quantity`, `location_id`, `location_dest_id` |
| **Stock Quants** | `stock.quant` (optional snapshots) | Inventory levels | `product_id`, `location_id`, `quantity`, `reserved_quantity`, `lot_id` |

#### B. Dimension/Reference Tables

| Dimension | Odoo Table | Purpose | Key Fields |
|-----------|--------------|---------|------------|
| **Products** | `product.product` | Product master data | `id`, `name`, `default_code`, `categ_id`, `brand_id` |
| **Categories** | `product.category` | Product categories | `id`, `name`, `parent_id` |
| **Brands** | `product.brand` (if exists) | Product brands | `id`, `name` |
| **Taxes** | `account.tax` | Tax definitions | `id`, `name`, `amount` |
| **Partners** | `res.partner` | Customers/Vendors | `id`, `name`, `company_type` |

### 11.2 Derived/Aggregated Tables

| Derived Table | Source Data | Logic | Purpose |
|--------------|--------------|-------|---------|
| **Cost Events** | Purchases (`fact_purchases`) | Apply tax multipliers to `actual_price`, filter bonus items | Tax-adjusted cost per purchase line |
| **Latest Daily Cost** | Cost Events | Incremental merge to get latest cost per product per day | Latest known cost as of each date |
| **Sales Line Profit** | POS + Invoice Sales + Latest Cost | Join sales with cost, calculate profit per line | Revenue, COGS, gross profit per sales line |
| **Profit Aggregates** | Sales Line Profit | Group by date and/or product | Daily and by-product profit summaries |

### 11.3 ETL Data Flow

```
Odoo Transactional Tables:
├── pos.order → POS Sales
├── account.move (out_invoice) → Invoice Sales  
├── account.move (in_invoice) → Purchases
├── stock.move.line → Inventory Moves
└── stock.quant → Stock Snapshots

↓ ETL Processing ↓

Derived/Aggregated Tables:
├── Cost Events (from Purchases)
├── Latest Daily Cost (from Cost Events)
├── Sales Line Profit (from Sales + Cost)
└── Profit Aggregates (from Sales Line Profit)

Reference Dimensions:
├── Products, Categories, Brands, Taxes, Partners
```

### 11.4 Key Business Rules Applied

- **Cost Rule**: "Latest known cost" as of sale date (not future prices)
- **Tax Multipliers**: Purchase tax_id 5/2 → 1.0x, 7/6 → 1.11x, default 1.0
- **Bonus Exclusion**: Filter out purchases where `actual_price ≤ 0` or `quantity ≤ 0`
- **Profit Calculation**: `gross_profit = revenue_tax_in - cogs_tax_in`
- **Plan:** `C:\Users\ThinkPad\.windsurf\plans\chart-building-optimizations-7dcd08.md`
- **Code examples:** `pages/sales.py` (progressive loading), `services/sales_charts.py` (cached builders), `services/cache.py` (cache init).
- **Docker:** `docker-compose.yml` (DASH_CACHE_TTL_SECONDS), `requirements.txt` (Flask-Caching).
