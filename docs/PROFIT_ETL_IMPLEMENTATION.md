# Profit & Cost ETL Implementation Guide

## Overview

This document describes the implementation of tax-adjusted cost and gross profit calculation with materialized aggregates for performance. The ETL follows the "latest known cost" principle: use the most recent purchase cost as of the sale date (not future prices).

## Architecture

### Data Flow

```
Purchases (actual_price) → Cost Events → Latest Daily Cost → Sales Lines → Profit Aggregates
```

1. **Cost Events**: Extract from purchases with tax adjustments
2. **Latest Daily Cost**: Incremental merge to get latest cost per product per day
3. **Sales Lines Profit**: Join sales with latest cost, calculate profit per line
4. **Profit Aggregates**: Daily and by-product rollups

### Key Tables

| Table | Grain | Purpose |
|--------|--------|---------|
| `fact_product_cost_events` | purchase line | Tax-adjusted cost events from purchases |
| `fact_product_cost_latest_daily` | date+product | Latest known cost per product as of each day |
| `fact_sales_lines_profit` | sales line | Revenue, COGS, and gross profit per sales line |
| `agg_profit_daily` | date | Daily profit aggregates |
| `agg_profit_daily_by_product` | date+product | Daily profit by product |

## Business Rules

### Cost Calculation

- **Tax Multipliers** (applied to purchase costs):
  - `tax_id IN (5, 2)` → 1.0x
  - `tax_id IN (7, 6)` → 1.11x
  - Default → 1.0x

- **Bonus Item Exclusion**: Filter out purchases where:
  - `actual_price <= 0` OR
  - `quantity <= 0`

- **Latest Known Cost Rule**: For sales on date D, use the most recent purchase cost as of D (not future prices)

### Profit Calculation

```
revenue_tax_in = 
  POS: price_subtotal_incl (already tax-inclusive)
  Invoice: price_unit * quantity * tax_multiplier

cogs_tax_in = cost_unit_tax_in * quantity

gross_profit = revenue_tax_in - cogs_tax_in
```

## Implementation Details

### ETL Tasks

#### 1. Cost Events (`_build_product_cost_events`)

```python
def _build_product_cost_events(target_date: str) -> pl.DataFrame:
    # Read fact_purchases for target_date
    # Apply tax multiplier to actual_price
    # Filter out bonus items (actual_price <= 0 or quantity <= 0)
    # Output: date, product_id, cost_unit_tax_in, source_move_id, source_tax_id
```

#### 2. Latest Daily Cost (`_build_product_cost_latest_daily`)

```python
def _build_product_cost_latest_daily(target_date: str) -> pl.DataFrame:
    # Read previous day's latest cost snapshot
    # Read today's cost events
    # Merge: keep latest cost per product
    # Output: date, product_id, cost_unit_tax_in, source_move_id, source_tax_id
```

#### 3. Sales Lines Profit (`_build_sales_lines_profit`)

```python
def _build_sales_lines_profit(target_date: str) -> pl.DataFrame:
    # Combine POS and invoice sales
    # Left join with latest daily cost
    # Calculate profit per line
    # Output: date, txn_id, line_id, product_id, quantity, revenue_tax_in, cogs_tax_in, gross_profit
```

#### 4. Profit Aggregates (`_build_profit_aggregates`)

```python
def _build_profit_aggregates(profit_df: pl.DataFrame):
    # Daily aggregates: sum revenue, COGS, profit, count transactions/lines
    # By-product aggregates: same but grouped by product
    # Return: (daily_agg, by_product_agg)
```

### Celery Tasks

All profit ETL tasks are registered in `etl_tasks.py`:

```python
@app.task(bind=True, queue='profit-etl')
def update_product_cost_events(self, target_date: str) -> Optional[str]:
    # Build and write cost events

@app.task(bind=True, queue='profit-etl') 
def update_product_cost_latest_daily(self, target_date: str) -> Optional[str]:
    # Build and write latest daily cost

@app.task(bind=True, queue='profit-etl')
def update_sales_lines_profit(self, target_date: str) -> Optional[str]:
    # Build and write sales line profit

@app.task(bind=True, queue='profit-etl')
def update_profit_aggregates(self, target_date: str) -> Optional[str]:
    # Build and write profit aggregates
```

### Daily Pipeline

```python
@app.task(bind=True, queue='profit-etl')
def daily_profit_pipeline_impl(self, target_date: str = None) -> Optional[str]:
    # Chain: cost_events → latest_cost → sales_profit → aggregates
    # Scheduled at 02:20 daily
```

## DuckDB Views

All tables are exposed as DuckDB views in `services/duckdb_connector.py`:

```sql
CREATE OR REPLACE VIEW fact_product_cost_events AS
SELECT * FROM read_parquet('/data-lake/star-schema/fact_product_cost_events/**/*.parquet', union_by_name=True);

CREATE OR REPLACE VIEW fact_product_cost_latest_daily AS
SELECT * FROM read_parquet('/data-lake/star-schema/fact_product_cost_latest_daily/**/*.parquet', union_by_name=True);

CREATE OR REPLACE VIEW fact_sales_lines_profit AS
SELECT * FROM read_parquet('/data-lake/star-schema/fact_sales_lines_profit/**/*.parquet', union_by_name=True);

CREATE OR REPLACE VIEW agg_profit_daily AS
SELECT * FROM read_parquet('/data-lake/star-schema/agg_profit_daily/**/*.parquet', union_by_name=True);

CREATE OR REPLACE VIEW agg_profit_daily_by_product AS
SELECT * FROM read_parquet('/data-lake/star-schema/agg_profit_daily_by_product/**/*.parquet', union_by_name=True);
```

## Validation

### Unit Tests

`tests/test_profit_etl.py` provides comprehensive validation:

- **Tax multiplier logic**: Verify correct multipliers applied
- **Cost events**: Test extraction and bonus exclusion
- **Sales profit**: Test cost join and profit calculation
- **Aggregates**: Verify rollup calculations
- **Partitioned writes**: Test file output structure
- **Path helpers**: Test partition path generation

Run with:
```bash
python -m pytest tests/test_profit_etl.py -v
```

### Manual Validation Scripts

#### Validation Runner

```bash
# Validate profit ETL for a specific date
python scripts/validate_profit_etl.py --date 2025-03-15 --write-samples
```

Validates:
- Tax multipliers are correct
- Cost events exclude bonus items
- Sales profit calculations are accurate
- Aggregates match line-level totals
- DuckDB views are accessible

#### Manual ETL Runner

```bash
# Dry run to see what would execute
python scripts/run_profit_etl.py --date 2025-03-15 --dry-run

# Actually run ETL
python scripts/run_profit_etl.py --date 2025-03-15
```

## Operational Procedures

### Running Profit ETL

#### Scheduled Execution

The profit ETL runs automatically daily at 02:20 via Celery Beat:

```
02:20 - daily_profit_pipeline_impl
  ├── update_product_cost_events
  ├── update_product_cost_latest_daily  
  ├── update_sales_lines_profit
  └── update_profit_aggregates
```

#### Manual Execution

```bash
# Run specific date via Celery
docker-compose exec celery-worker python -c "
from etl_tasks import daily_profit_pipeline_impl
daily_profit_pipeline_impl.delay('2025-03-15')
"

# Run all steps manually
docker-compose exec celery-worker python -c "
from etl_tasks import (
    update_product_cost_events,
    update_product_cost_latest_daily,
    update_sales_lines_profit,
    update_profit_aggregates
)
update_product_cost_events.delay('2025-03-15')
update_product_cost_latest_daily.delay('2025-03-15')
update_sales_lines_profit.delay('2025-03-15')
update_profit_aggregates.delay('2025-03-15')
"
```

### Monitoring

```bash
# Check profit ETL queue
docker-compose exec celery-worker celery -A etl_tasks inspect active

# Check task results
docker-compose exec redis redis-cli --scan --pattern "celery-task-meta-*"

# View logs
docker-compose logs -f celery-worker | grep profit
```

## Troubleshooting

### Common Issues

1. **Missing cost data**
   - Check if purchases exist for the date
   - Verify `actual_price` > 0 in purchases
   - Check tax_id values in purchases

2. **Incorrect profit calculations**
   - Verify tax multipliers in `_tax_multiplier_expr`
   - Check cost join logic in `_build_sales_lines_profit`
   - Validate aggregate formulas

3. **Performance issues**
   - Check partition pruning (date predicates)
   - Verify incremental cost merge is working
   - Monitor DuckDB query performance

### Data Quality Checks

```sql
-- Check for missing costs
SELECT date, COUNT(*) as sales_without_cost
FROM fact_sales_lines_profit 
WHERE cost_unit_tax_in IS NULL OR cost_unit_tax_in = 0
GROUP BY date;

-- Check for negative profits (potential data issues)
SELECT date, COUNT(*) as negative_profit_lines
FROM fact_sales_lines_profit 
WHERE gross_profit < 0
GROUP BY date;

-- Validate cost timeline integrity
SELECT product_id, date, cost_unit_tax_in,
       LAG(cost_unit_tax_in) OVER (PARTITION BY product_id ORDER BY date) as prev_cost,
       LEAD(cost_unit_tax_in) OVER (PARTITION BY product_id ORDER BY date) as next_cost
FROM fact_product_cost_latest_daily
WHERE product_id = <specific_product>
ORDER BY date;
```

## Performance Considerations

### Incremental Cost Updates

The latest daily cost uses incremental merge:
- Read previous day's snapshot
- Merge with today's cost events
- Keep only the latest cost per product
- Write new snapshot

This avoids reprocessing entire history daily.

### Partitioning Strategy

All tables are partitioned by date:
```
star-schema/
├── fact_product_cost_events/year=YYYY/month=MM/day=DD/
├── fact_product_cost_latest_daily/year=YYYY/month=MM/day=DD/
├── fact_sales_lines_profit/year=YYYY/month=MM/day=DD/
├── agg_profit_daily/year=YYYY/month=MM/day=DD/
└── agg_profit_daily_by_product/year=YYYY/month=MM/day=DD/
```

### Query Optimization

DuckDB views use `union_by_name=True` for efficient partition reading:
- Only reads partitions matching date predicates
- Avoids full table scans
- Maintains query performance as data grows

## Future Enhancements

### Potential Extensions

1. **Margin Analysis**: Add profit margin percentages by product/category
2. **Cost Trending**: Track cost changes over time
3. **Profit Forecasting**: Predict future profit based on trends
4. **Cost Attribution**: Track cost sources by vendor/category

### Scalability Considerations

- **Data retention**: Archive old partitions beyond certain age
- **Compression**: Consider Snappy compression for parquet files
- **Materialized views**: Pre-compute common profit metrics
- **Caching**: Add Redis caching for expensive profit calculations
