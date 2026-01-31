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

## 7) Work tracking (active workstreams)
Use globally unique workstream IDs.

- **NK_20260119_ssot_0001** — Establish SSOT + oversight cadence
  - **Status:** Done
  - **Deliverables:** `SSOT.md` created; link added to `README.md`.

- **NK_20260119_inventory_kpis_3f2a** — Inventory KPIs + Inventory page build-out
  - **Status:** Done (code present)
  - **Deliverables:** Inventory page implementing:
    - Stock Levels
    - Sell-through Ratio
    - ABC Analysis
  - **Plan:** See section 9.

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
