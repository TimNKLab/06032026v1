# NKDash Decision Log

This is an append-only log of significant decisions made during NKDash development. Each decision includes the date, context, and impact.

---

## 2025-12

### Repository Documentation Standards
**Date:** 2025-12  
**Context:** Establishing documentation patterns for the repository  
**Decision:** `etl_tasks.py` remains the single ETL entry point; implementations may live under `etl/` but task names must remain stable.  
**Impact:** Ensures backward compatibility for scheduled tasks and external integrations.

### Data Lake Path Configuration
**Date:** 2025-12  
**Context:** Docker containerization and cross-platform development  
**Decision:** Data lake root inside containers should be `/data-lake` (Windows host bind mount typically `D:\data-lake → /data-lake`).  
**Impact:** Standardizes paths across development environments and avoids Windows/Linux path conflicts.

---

## 2026-01

### ABC Analysis Definition Change
**Date:** 2026-01  
**Context:** Inventory KPI implementation and business requirements  
**Decision:** ABC classification changed from cumulative revenue-share to SKU-share thresholds (default A=top 20% SKUs by revenue, B=next 30%, C=rest). Pareto curve still uses cumulative revenue share for visualization.  
**Impact:** Aligns with inventory management best practices; changes ABC distribution in inventory dashboard.

### Inventory Adjustments Handling
**Date:** 2026-01  
**Context:** Sell-through calculations showing inaccurate results  
**Decision:** Inventory adjustments and manufacturing (production output/consumption) break sell-through and days-of-cover. Planned workstream NK_20260121_adjustments_8d9b to tag moves, exclude them from receipts, and add reconciliation visibility.  
**Impact:** Improves accuracy of inventory KPIs; requires ETL changes and UI updates.

---

## 2026-02

### Cost & Profit ETL Implementation
**Date:** 2026-02  
**Context:** Business requirement for gross profit analysis  
**Decision:** Implemented tax-adjusted cost and gross profit calculation with materialized aggregates. Cost rule: "latest known cost" as of sale date (not future prices). Tax multipliers: purchase tax_id 5/2 → 1.0x, 7/6 → 1.11x, default 1.0. Bonus items (actual_price ≤ 0 or quantity ≤ 0) excluded from cost calculation. Daily pipeline scheduled at 02:20. Validated with unit tests and manual validation scripts.  
**Impact:** Enables profit analysis across all sales channels; requires new ETL tasks and DuckDB views.

### Odoo Data Sources Documentation
**Date:** 2026-02  
**Context:** Need for comprehensive data inventory  
**Decision:** Documented all Odoo tables used in ETL: pos.order (POS), account.move (sales/purchases), stock.move.line (inventory), stock.quant (snapshots), plus dimensions (product, category, brand, tax, partner). Derived tables: cost events, latest daily cost, sales line profit, profit aggregates.  
**Impact:** Provides complete reference for understanding data sources and transformations.

### Profit ETL Performance Optimization
**Date:** 2026-02  
**Context:** Performance issues with profit queries  
**Decision:** Implemented performance optimizations for profit ETL querying and serving layer. Enabled Hive partition pruning in all profit DuckDB views, added caching layer for profit queries and chart builders, created optimized query functions defaulting to aggregates, and added performance monitoring script for ongoing tracking.  
**Impact:** Significantly improves query performance for profit analytics; adds caching infrastructure.

### Sales Transaction Filtering
**Date:** 2026-02  
**Context:** Data discrepancy in revenue calculations  
**Decision:** Implemented filtering to exclude cancelled transactions where `order_ref = "/"` from sales reconciliation. Updated `fact_sales_all` view and profit ETL pipeline to apply filter at both view and ETL levels. Fixed discrepancy where Feb 10, 2025 showed 247,446,157 instead of correct 247,203,048.  
**Impact:** Corrects revenue calculations; affects both POS and invoice sales data.

### Beginning Costs Implementation
**Date:** 2026-02  
**Context:** Profit calculations showing 100% margin for dates without purchase history  
**Decision:** Created `fact_product_beginning_costs` table with 48,455 products to provide cost fallback for dates without purchase history. Fixed data types (product_id, source_tax_id, effective_date) and validated profit calculations showing realistic margins (1.9% vs previous 100%). ETL pipeline now uses beginning costs when latest purchase costs are unavailable.  
**Impact:** Resolves unrealistic profit margins; provides cost foundation for historical periods.

---

## Decision Categories

### Architecture Decisions
- ETL entry point standardization
- Data lake path configuration
- Performance optimization strategies

### Business Logic Decisions  
- ABC analysis methodology
- Cost calculation rules
- Tax multiplier definitions
- Transaction filtering criteria

### Data Management Decisions
- Inventory adjustments handling
- Beginning costs strategy
- Data source documentation

### Quality & Validation Decisions
- Profit calculation validation
- Data discrepancy resolution
- Performance monitoring implementation

---

## Impact Assessment

### High Impact Decisions
- Cost & Profit ETL implementation (enables new analytics)
- Performance optimization (affects all query performance)
- Beginning costs implementation (fixes profit calculations)

### Medium Impact Decisions
- ABC analysis change (affects inventory KPIs)
- Sales transaction filtering (data quality improvement)
- Inventory adjustments handling (reconciliation accuracy)

### Low Impact Decisions
- Documentation standards (development process)
- Path configuration (development environment)

---

## Related Workstreams
- NK_20260206_profit_etl_9a2b: Cost & Profit ETL implementation
- NK_20260206_profit_etl_perf_9a2b: Performance optimization
- NK_20260121_adjustments_8d9b: Inventory adjustments handling
- NK_20260221_beginning_costs_1f3c: Beginning costs implementation

---

*This document is append-only. New decisions should be added at the top with proper date and context.*  
*Last updated: 2026-02-21*
