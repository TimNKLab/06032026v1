# NKDash Data Logic Guide

This document is the unified source of truth for all business logic, data transformations, and KPI definitions used within the NKDash ecosystem. It replaces fragmented legacy documents (Profit ETL, Inventory Spec, etc.).

---

## 🏛️ Core Architecture: Render Solo Mode

NKDash operates on a **Solo Mode** architecture to ensure $0 bootstrap cost and maximum simplicity for a solo maintainer.

### Data Flow
`Odoo ERP` $\rightarrow$ `Extraction (odoorpc)` $\rightarrow$ `Raw Parquet` $\rightarrow$ `Clean Parquet` $\rightarrow$ `Star-Schema Parquet` $\rightarrow$ `DuckDB in-memory` $\rightarrow$ `Dash/Streamlit UI`

### Technology Stack
- **Storage**: Local Parquet files on persistent disk (Hive-partitioned by date).
- **Compute**: Polars (for ETL transformations) and DuckDB (for BI queries).
- **Orchestration**: Python `schedule` library (no Celery/Redis).
- **Interface**: Dash (C-Level/Operational) and Streamlit (Maintainer Admin).

---

## 💰 Profit & Cost Logic

### 1. Cost Calculation (The "Latest Known Cost" Principle)
We use the most recent purchase cost as of the sale date to calculate COGS, avoiding future-price contamination.

**Tax Multipliers (Purchase Costs):**
- `tax_id` in {5, 2} $\rightarrow$ 1.0x (No adjustment)
- `tax_id` in {7, 6} $\rightarrow$ 1.11x (11% VAT inclusive)
- Default $\rightarrow$ 1.0x

**Exclusions:**
- Bonus items are excluded from cost calculation if `actual_price <= 0` or `quantity <= 0`.

### 2. Profit Formulas
- **Revenue (Tax-Inclusive)**:
  - **POS**: `price_subtotal_incl`
  - **Invoice**: `price_unit * quantity * tax_multiplier`
- **COGS (Tax-Inclusive)**: `cost_unit_tax_in * quantity`
- **Gross Profit**: `revenue_tax_in - cogs_tax_in`

### 3. Profit Data Pipeline
1. **Cost Events**: Extract tax-adjusted purchase costs.
2. **Daily Cost Snapshot**: Incremental merge to find the latest cost per product per day.
3. **Sales Line Profit**: Join sales with the cost snapshot to calculate per-line profit.
4. **Aggregates**: Roll up to daily and by-product summaries.

---

## 📦 Inventory Logic

### 1. Stock Levels
- **On-hand units**: Total quantity on hand per product.
- **Days of Cover**: `on_hand_units / avg_daily_units_sold` (trailing 30 days).
- **Low Stock**: `days_of_cover < 7` (Default).
- **Dead Stock**: `on_hand_units > 0` AND `sold_units_last_30_days = 0`.

### 2. Sell-through Ratio
Measures inventory efficiency over a period:
$$\text{Sell-through} = \frac{\text{units\_sold}}{\text{begin\_on\_hand} + \text{units\_received}}$$

### 3. ABC Analysis (SKU-share based)
Products are classified by their revenue contribution:
- **Class A**: Top 20% of SKUs by revenue.
- **Class B**: Next 30% of SKUs by revenue.
- **Class C**: Remaining SKUs.

---

## 📐 Data Lake Schema (Star-Schema)

All tables are partitioned by date: `/star-schema/{table}/year=YYYY/month=MM/day=DD/`.

| Table | Grain | Purpose |
|---|---|---|
| `fact_sales` | Sale Line | POS sales transactions |
| `fact_invoice_sales` | Invoice Line | Customer invoice sales |
| `fact_purchases` | Purchase Line | Vendor bill purchases |
| `fact_inventory_moves` | Move Line | Stock movements and adjustments |
| `fact_stock_on_hand_snapshot` | Date + Product | Daily inventory positions |
| `fact_product_cost_latest_daily` | Date + Product | Latest known cost per product |
| `fact_sales_lines_profit` | Sale Line | Revenue, COGS, and Gross Profit |
| `agg_profit_daily` | Date | Daily profit aggregates |
| `dim_products` | Product | Product attributes, brand, category |

---

## ⚡ Performance & Optimization Policy

To maintain sub-2s query latency, the following rules apply:

1. **Date Predicates**: All DuckDB queries MUST include a date filter to enable Hive partition pruning.
2. **No Hybrid Layers**: Do not use SQLite MVs. Use DuckDB in-memory views reading directly from Parquet.
3. **Aggregation**: Heavy aggregations (e.g., `agg_sales_daily`) are pre-computed by the ETL and stored as Parquet.
4. **Memory Management**: Use Polars `streaming=True` for large data transformations to avoid OOM.
5. **Caching**: Use `@cache.memoize()` for expensive chart builders with a 600s TTL.
