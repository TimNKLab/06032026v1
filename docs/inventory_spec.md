# NKDash Inventory Dashboard Specification

This document provides the complete specification for the inventory dashboard implementation, including KPI definitions, data requirements, UX design, and validation procedures.

## Table of Contents
- [Overview](#overview)
- [KPI Definitions](#kpi-definitions)
- [Data Requirements](#data-requirements)
- [Dashboard UX Design](#dashboard-ux-design)
- [Backend Implementation](#backend-implementation)
- [Validation Procedures](#validation-procedures)

## Overview

### Scope
Create a comprehensive inventory dashboard (`pages/inventory.py`) covering three core areas:
1. **Stock Levels** - Current inventory positions and alerts
2. **Sell-through** - Inventory efficiency metrics  
3. **ABC Analysis** - Product classification by revenue contribution

### Implementation Principle
Compute all metrics from DuckDB views (data lake) as the default, only falling back to Odoo for validation/debugging.

---

## KPI Definitions

### Stock Levels

#### Business Question
"What do we have on hand today (and on a selected date), and which items are low / overstocked?"

#### Metrics
- **On-hand units**: Total quantity on hand per product
- **Days of cover**: `on_hand_units / avg_daily_units_sold` (trailing N days)
- **Low stock flag**: `days_of_cover < X` (configurable; default X=7)
- **Dead stock flag**: `on_hand_units > 0 AND sold_units_last_N_days = 0`

#### Required Fields
- `product_id`, `location_id` (optional), `on_hand_qty`, `reserved_qty` (optional), `snapshot_date`

### Sell-through Ratio

#### Business Question  
"How efficiently did we sell inventory we had available during a time period?"

#### Formula (Units)
```
sell_through = units_sold / (begin_on_hand + units_received)
```

#### Inputs Required
- `units_sold` from `fact_sales_all`
- `begin_on_hand`, `end_on_hand` from stock snapshots
- `units_received` from `fact_inventory_moves` (SUM(qty_moved > 0))

### ABC Analysis

#### Business Question
"Which products contribute most to sales (or units) so we can prioritize attention?"

#### Classification Method
**SKU-share-based ABC** (configurable thresholds):
- **A Class**: Top 20% of SKUs by revenue
- **B Class**: Next 30% of SKUs by revenue (up to 50%)
- **C Class**: Remaining SKUs

#### Data Needed
- revenue, quantity from `fact_sales_all` + product attributes from `dim_products`

---

## Data Requirements

### Current Data Feasibility Assessment

| KPI | Feasible with Current Data | Gaps | Dependencies |
|-----|---------------------------|------|--------------|
| Stock Levels | **Partial** | Absolute on-hand quantities | Stock snapshots required |
| Sell-through | **Not accurate** | Beginning inventory | Stock snapshots required |
| ABC Analysis | **Yes** | None | `fact_sales_all` + `dim_products` |

### Required Data Additions

#### Stock Snapshots (Critical)
**Source**: `stock.quant` (preferred) or `product.product qty_available` (fallback)

**Proposed Schema**: `fact_stock_on_hand_snapshot`
- Grain: `(snapshot_date, product_id, location_id)`
- Fields: `snapshot_date`, `product_id`, `location_id`, `quantity`, `reserved_quantity`

**Storage Layout**:
```
/raw/stock_quants/year=YYYY/month=MM/day=DD/stock_quants_YYYY-MM-DD.parquet
/clean/stock_quants/year=YYYY/month=MM/day=DD/stock_quants_clean_YYYY-MM-DD.parquet  
/star-schema/fact_stock_on_hand_snapshot/year=YYYY/month=MM/day=DD/fact_stock_on_hand_snapshot_YYYY-MM-DD.parquet
```

#### Inventory Move Classification (Recommended)
Enhance `fact_inventory_moves` with:
- `movement_type` (incoming/outgoing/internal/adjustment/scrap/production)
- `inventory_adjustment_flag` (boolean)

---

## Dashboard UX Design

### Information Architecture
**Three-tab layout** in `pages/inventory.py`:
1. **Stock Levels** tab
2. **Sell-through** tab  
3. **ABC Analysis** tab

### Common Controls (Shared Across Tabs)

#### Date Selection
- **Stock Levels**: Single "As-of date" picker
- **Sell-through/ABC**: Date range picker (start/end dates)

#### Filters
- Category dropdown
- Brand dropdown  
- Product search box
- Location filter (if location-grain snapshots available)

#### Toggles
- Include/exclude returns (affects sell-through)
- Include/exclude adjustments (if movement_type available)

### Stock Levels Tab

#### KPI Cards (Top Row)
- Total on-hand units (sum across all products)
- Count of low-stock SKUs (days_of_cover < 7)
- Count of dead-stock SKUs (no sales in last 30 days)
- Total inventory value (if cost data available)

#### Data Table
| Column | Description |
|--------|-------------|
| SKU | Product identifier |
| Product Name | Human-readable name |
| On-hand Units | Current quantity |
| Avg Daily Sold | Trailing 30-day average |
| Days of Cover | On-hand ÷ daily avg |
| Status | Low stock/Dead stock/Normal |

#### Charts
- **Distribution Chart**: Histogram of days-of-cover across all SKUs
- **Low Stock Alert**: Bar chart of SKUs with lowest days-of-cover
- **Category Breakdown**: On-hand units by product category

### Sell-through Tab

#### KPI Cards (Top Row)
- Overall sell-through % (units_sold ÷ available_units)
- Total units sold (period)
- Total units received (period)
- Net inventory change (received - sold)

#### Charts
- **Sell-through by Category**: Grouped bar chart
- **Top/Bottom Performers**: SKUs with highest/lowest sell-through
- **Trend Line**: Daily sell-through over selected period

#### Data Table
| Column | Description |
|--------|-------------|
| SKU | Product identifier |
| Units Sold | Total quantity sold |
| Units Received | Total quantity received |
| Beginning On-hand | Start of period |
| Sell-through % | Calculated ratio |
| Status | Good/Fair/Poor |

### ABC Analysis Tab

#### KPI Cards (Top Row)
- A Class SKU count and revenue share
- B Class SKU count and revenue share  
- C Class SKU count and revenue share
- Total revenue (period)

#### Charts
- **Pareto Curve**: Cumulative revenue % vs SKU count %
- **ABC Distribution**: Revenue by class (pie/donut chart)
- **Category ABC**: ABC breakdown by product category

#### Data Table
| Column | Description |
|--------|-------------|
| SKU | Product identifier |
| Revenue | Total revenue (period) |
| Cumulative Revenue % | Running total percentage |
| Class | A/B/C classification |
| Category | Product category |

---

## Backend Implementation

### Architecture Pattern
Follow existing NKDash patterns:
- `pages/inventory.py` - Layout and callbacks
- `services/inventory_metrics.py` - Query functions
- `services/inventory_charts.py` - Chart builders

### Query Strategy

#### Performance Guardrails
- Use single-scan aggregates where possible
- Apply date predicates in DuckDB (not Python)
- Limit result sets with SQL LIMIT clauses
- Cache expensive calculations

#### DuckDB Query Examples

**Stock Levels Query**:
```sql
SELECT 
    p.product_id,
    p.product_name,
    s.quantity as on_hand_units,
    COALESCE(avg_daily.avg_daily, 0) as avg_daily_sold,
    s.quantity / NULLIF(avg_daily.avg_daily, 0) as days_of_cover
FROM fact_stock_on_hand_snapshot s
LEFT JOIN dim_products p USING (product_id)
LEFT JOIN (
    SELECT 
        product_id, 
        AVG(quantity) as avg_daily
    FROM fact_sales_all 
    WHERE date >= current_date - INTERVAL 30 DAY
    GROUP BY product_id
) avg_daily USING (product_id)
WHERE s.snapshot_date = ?
```

**Sell-through Query**:
```sql
WITH period_metrics AS (
    SELECT 
        product_id,
        SUM(CASE WHEN date >= ? AND date <= ? THEN quantity ELSE 0 END) as units_sold,
        SUM(CASE WHEN date >= ? AND date <= ? AND qty_moved > 0 THEN qty_moved ELSE 0 END) as units_received
    FROM fact_sales_all
    WHERE date BETWEEN ? AND ?
    GROUP BY product_id
)
SELECT 
    p.product_id,
    m.units_sold,
    m.units_received,
    COALESCE(begin_stock.quantity, 0) as begin_on_hand,
    m.units_sold / NULLIF(m.units_received + COALESCE(begin_stock.quantity, 0), 0) as sell_through_ratio
FROM period_metrics m
LEFT JOIN dim_products p USING (product_id)
LEFT JOIN fact_stock_on_hand_snapshot begin_stock 
    ON m.product_id = begin_stock.product_id 
    AND begin_stock.snapshot_date = ?
```

### Caching Strategy
Apply `@cache.memoize()` to:
- Expensive chart builders (600s TTL)
- Query results used by multiple components
- ABC classification calculations

---

## Validation Procedures

### Option C Validation (Inventory-Specific)

#### Correctness Validation
1. **Stock on hand**: Pick 20 SKUs and compare "as-of date" on-hand to Odoo
2. **Receipts**: Pick 3 days and reconcile `SUM(qty_moved>0)` to Odoo stock moves
3. **Units sold**: Reconcile to Odoo sales report for same date range

#### Freshness Validation
- Verify snapshot partition exists for yesterday (or selected as-of date)
- Check ETL metadata for last successful stock quant extraction

#### Performance Validation
- Stock levels query (top N + aggregates) < 2 seconds
- ABC query (Pareto + table) < 2 seconds for typical ranges (30-90 days)

#### Evidence Recording
Add validation evidence to decision log:
```
## Inventory Dashboard Validation - YYYY-MM-DD
- Stock on hand: 20 SKUs sampled, 100% match to Odoo
- Receipts: 3 days reconciled, variance < 0.1%
- Performance: Stock levels 1.2s, ABC 0.8s
- Status: Validated
```

### Test Data Scenarios
- **Normal operations**: Typical daily sales and receipts
- **Low stock**: Items with days_of_cover < 7
- **Dead stock**: Items with zero sales in last 30 days
- **Adjustments**: Manual stock counts or write-offs
- **Returns**: Customer returns affecting inventory

---

## Implementation Roadmap

### Phase 1: Data Foundation
1. Implement stock quant snapshot ETL
2. Create `fact_stock_on_hand_snapshot` table and DuckDB view
3. Add movement classification to inventory moves

### Phase 2: Core KPIs
1. Build stock levels metrics and visualizations
2. Implement sell-through calculations
3. Create ABC analysis functionality

### Phase 3: Advanced Features
1. Add location-based analysis (if data available)
2. Implement inventory value calculations
3. Add trend analysis and forecasting

### Phase 4: UX Polish
1. Responsive design optimizations
2. Advanced filtering and search
3. Export functionality for reports

---

## Dependencies & Blockers

### Critical Dependencies
- **Stock quant snapshots**: Required for accurate stock levels and sell-through
- **Movement classification**: Needed for adjustment handling

### Technical Dependencies
- Existing `fact_sales_all` and `dim_products` tables
- DuckDB connection and caching infrastructure
- DMC component library for UI

### External Dependencies
- Odoo `stock.quant` table access
- Product category and brand data
- Historical inventory movement data

---

## Success Metrics

### Technical Metrics
- Query performance < 2 seconds for all tabs
- Data freshness within 1 day
- 100% validation accuracy against Odoo

### Business Metrics  
- Reduced stock-outs through low-stock alerts
- Improved inventory turnover analysis
- Better purchasing decisions through ABC insights

### User Experience Metrics
- Intuitive three-tab navigation
- Responsive design for mobile/desktop
- Fast loading times with progressive loading

---

*This specification is a living document. Updates should be made as requirements evolve and implementation progresses.*  
*Related workstream: NK_20260119_inventory_kpis_3f2a*  
*Last updated: 2026-02-21*
