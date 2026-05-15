---
inclusion: always
---

# NKDash AI Assistant Steering Guide

This document provides essential context and guidance for AI assistants working on the NKDash project.

## Project Overview

**NKDash** is a real-time sales analytics dashboard for New Khatulistiwa retail operations. Data flows from Odoo ERP through a Celery-based ETL pipeline into a Parquet data lake, then into DuckDB for fast analytical queries, and finally served via a Dash web application.

**Key Technologies:**
- Frontend: Dash 2.14.2 + Dash Mantine Components 2.4.0 + React 18
- Backend: Python 3.9 + Polars + Plotly + DuckDB 1.2.0
- Orchestration: Celery (Redis) + Redis
- Database: DuckDB (in-process analytical DB)
- Container: Docker + Docker Compose
- Auth: Odoo RPC (jsonrpc+ssl)
- Storage: Parquet (columnar, partitioned by date)

## Core Architecture

### Data Flow
```
Odoo (RPC) → ETL (Celery) → Parquet Data Lake → DuckDB → Dash Dashboard
```

### Data Lake Structure
- **raw/**: Raw Odoo data, partitioned by date (year=YYYY/month=MM/day=DD)
- **clean/**: Transformed/cleaned data, same partitioning
- **star-schema/**: Analytics-ready fact and dimension tables

### Dashboard Pages
| Route | Page | Purpose |
|-------|------|---------|
| `/` | Overview | High-level KPIs |
| `/sales` | Sales | Revenue, transactions, trends |
| `/sales-drilldown` | Sales Drilldowns | Detailed breakdowns |
| `/inventory` | Inventory | Stock levels, sell-through, ABC |
| `/customer` | Customer Experience | Customer metrics |
| `/operational` | ETL Ops | Data sync, refresh controls |

## Critical Conventions

### 1. Single Source of Truth (SSOT)
**Always consult `SSOT.md` first** for current phase, decisions, open questions, and workstream tracking. This takes precedence over code comments or historical logs.

### 2. Performance Policy
- **Default to pre-aggregated tables** for dashboard queries (agg_sales_daily*, agg_profit_daily*)
- **Only query line-level fact tables** for drilldowns or edge cases
- **Always use hive_partitioning=1** for date-partitioned DuckDB views
- **Cache expensive charts** with `@cache.memoize()` (600s TTL default)

### 3. ETL Patterns
- **Polars first** for all data processing (no pandas)
- **Atomic writes** via `atomic_write_parquet()` from `etl/io_parquet.py`
- **Partition by date** for all fact tables
- **Single-file dimensions** (dim_products.parquet, etc.)
- **Compression**: zstd (defined in `etl/config.py`)

### 4. Dashboard Service Patterns
- **Metrics first**: Charts call metrics functions, never query DuckDB directly
- **DuckDB only**: No live Odoo queries in metrics (fallback only if DuckDB fails)
- **Error handling**: Return empty DataFrame/figure on error, log exception
- **Date validation**: Swap dates if `start_date > end_date`

## Key Business Rules

### Revenue Calculation
- **POS**: `price_subtotal_incl - x_studio_discount_amount` (discount defaults to 0)
- **Invoices**: Tax-included revenue from invoice lines
- **Refunds**: Allowed (negative qty/totals)

### Cost & Profit
- **Cost Rule**: "Latest known cost" - most recent purchase actual_price as of sale date
- **Tax Multipliers**: 
  - Purchase cost: tax_id IN (5,2) → 1.0x, tax_id IN (7,6) → 1.11x, default 1.0
  - Sales revenue: tax-included
- **Bonus Items**: Exclude when actual_price <= 0 OR quantity <= 0

### ABC Analysis
- **SKU-share thresholds**: A=top 20% SKUs by revenue, B=next 30% (up to 50%), C=rest
- **Pareto curve**: Uses cumulative revenue share for display only

## ETL Schedule (Jakarta Time, Asia/Jakarta)
| Task | Time | Queue |
|------|------|-------|
| POS sales | 02:00 | extraction |
| Invoice sales | 02:05 | extraction |
| Daily aggregates | 02:12 | loading |
| Profit pipeline | 02:20 | transformation |
| MV refresh | 03:00 | orchestration |

## Common Workflows

### Adding a New Odoo Data Source
1. Create extractor in `etl/extract/<dataset>.py`
2. Add to `etl_tasks.py` with proper Celery task wrapper
3. Create DuckDB view in `services/duckdb_connector.py`
4. Add metrics function in `services/<domain>_metrics.py`
5. Add chart builder in `services/<domain>_charts.py`
6. Update SSOT.md with new workstream

### Modifying an Existing Dashboard Page
1. Read SSOT.md for current phase and constraints
2. Check `pages/<page>.py` for existing callbacks
3. Update metrics in `services/<domain>_metrics.py`
4. Update charts in `services/<domain>_charts.py`
5. Test with Docker: `docker-compose exec web pytest tests/`
6. Update SSOT.md if behavior changes

### ETL Pipeline Changes
1. Read `etl/pipelines/<name>.py` for current implementation
2. Update `etl_tasks.py` with new Celery tasks
3. Update `etl/config.py` with new path constants
4. Update DuckDB views in `services/duckdb_connector.py`
5. Update SSOT.md with workstream and validation steps

## Data Lake Paths (Docker vs Host)
- **Docker**: `/data-lake`
- **Host (Windows)**: `D:\data-lake`
- **Environment Variable**: `DATA_LAKE_ROOT`
- **Never hardcode paths** - always use env variable

## Traceability
When modifying code, add short trace tags for non-obvious changes:
```python
# NK_YYYYMMDD_<slug>: why this change exists (one sentence)
```

Use workstream IDs from SSOT.md for consistency.

## Validation Standards
All implementations must include:
1. **Unit tests** for ETL transformations
2. **Integration tests** for pipelines
3. **Spot-checks** against Odoo for KPI accuracy
4. **Performance benchmarks** for dashboard queries
5. **Documentation** in SSOT.md or docs/

## AI Assistant Guidelines

### Team Log Documentation

**Write every finding, decision, and development into the team log immediately when it occurs.** Do not wait for a "task" to be formally complete. Log:
- Bug discoveries
- Architecture decisions
- Implementation patterns discovered
- Performance findings
- Configuration changes
- Open questions that arise

**Format:**
```markdown
### YYYY-MM-DD: [Workstream ID] - Short description
- Changes: [brief summary]
- Validation: [status]
- Notes: [any relevant details]
```

**Example:**
```markdown
### 2026-05-14: NK_20260514_mv_refresh_0001 - MV refresh signal handling
- Changes: Added background MV reload on Redis signal in dash-app
- Validation: May 2026 data now visible in MVs
- Notes: Celery workers now skip MV reload (in-memory DuckDB)
```

### Decision-Making Framework

**Before implementing any change:**
1. Check `SSOT.md` for current phase and open questions
2. Review existing code in relevant module (`etl/`, `services/`, `pages/`)
3. Identify the correct file pattern based on the task
4. Verify no existing implementation already covers the requirement

**When code is unclear:**
- Prefer simple, readable architectures over cleverness
- Avoid indirection unless it clearly reduces complexity
- Follow existing patterns in the codebase
- Document assumptions and trade-offs

**Validation boundaries:**
- Never claim validation of something you cannot actually run
- Test with Docker: `docker-compose exec web pytest tests/`
- Spot-check KPIs against Odoo for accuracy
- Benchmark dashboard query performance

**Logging requirement:**
- Log every finding, decision, and development to SSOT.md immediately
- Include workstream ID, date, and brief summary
- Update open questions section if new questions arise

### Code Style Priorities

**ETL Development:**
- Use Polars for all data processing (no pandas)
- Partition by date for all fact tables
- Use atomic writes via `atomic_write_parquet()`
- Single-file dimensions (dim_products.parquet, etc.)

**Dashboard Development:**
- Query DuckDB first, Odoo fallback only if needed
- Cache expensive operations with `@cache.memoize()`
- Return empty DataFrame/figure on error, log exception
- Swap dates if `start_date > end_date`

### Common Pitfalls to Avoid

1. **Hardcoding paths** - Always use `DATA_LAKE_ROOT` env variable
2. **Pandas in ETL** - Use Polars for all data processing
3. **Direct Odoo queries** - Use DuckDB for dashboard queries
4. **Missing tests** - Include unit/integration tests for changes
5. **No trace tags** - Add `NK_YYYYMMDD_<slug>` comments for non-obvious changes
6. **Ignoring SSOT** - Always check SSOT.md before implementing new features
7. **Delayed logging** - Log findings immediately, don't wait for task completion

## Quick Reference

| Task | File/Location |
|------|---------------|
| Dashboard entry point | `app.py` |
| Celery tasks | `etl_tasks.py` |
| ETL module | `etl/` |
| Dashboard services | `services/` |
| Dashboard pages | `pages/` |
| Reusable components | `components/` |
| Tests | `tests/` |
| Documentation | `docs/` |
| SSOT | `SSOT.md` |
| Architecture | `docs/ARCHITECTURE.md` |
| ETL Guide | `docs/ETL_GUIDE.md` |

## Environment Variables

**Required (.env):**
```ini
ODOO_HOST, ODOO_PORT, ODOO_PROTOCOL, ODOO_DB, ODOO_USERNAME, ODOO_API_KEY
REDIS_URL=redis://redis:6379/0
DATA_LAKE_ROOT=/data-lake
TZ=Asia/Jakarta
```

**Optional:**
```ini
CELERY_WORKER_CONCURRENCY=4
CELERY_TASK_SOFT_TIME_LIMIT=1800
CELERY_TASK_TIME_LIMIT=1900
PRELOAD_ALL_DUCKDB_VIEWS=0
```
