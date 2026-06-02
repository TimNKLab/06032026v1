# NKDash Performance Policy & Chart Building Guidelines

This document codifies the performance patterns and chart building policies used in NKDash to ensure responsive dashboards as datasets grow. It serves as the reference for all new page development.

## Table of Contents
- [Core Policies](#core-policies)
- [Query Strategy](#query-strategy)
- [Implementation Checklist](#implementation-checklist)
- [Performance Monitoring](#performance-monitoring)
- [Code Examples](#code-examples)

---

## Core Policies

### 1. Fail-fast to DuckDB; No Odoo Fallbacks in Runtime

**Policy**: All dashboard metrics must be served from DuckDB (data lake) at runtime.

**Implementation**:
- Remove or disable any Odoo RPC fallbacks in `services/*_metrics.py` and `pages/*` callbacks
- Use DuckDB as the single source of truth for all queries
- Implement data validation in ETL, not in dashboard queries

**Rationale**: Guarantees predictable latency and prevents cascading timeouts from external Odoo dependencies.

### 2. Server-side Caching with TTL for Expensive Results

**Tool**: `flask-caching` (Redis if `REDIS_URL` is set; otherwise SimpleCache)

**Policy**: Apply `@cache.memoize()` to:
- Expensive chart builders (`services/*_charts.py` functions that build Plotly figures)
- Repeatedly used query results (e.g., overview summary used by multiple charts)

**Configuration**:
```python
# TTL controlled by environment variable
DASH_CACHE_TTL_SECONDS = 600  # Default 10 minutes

# Apply to functions
@cache.memoize(timeout=DASH_CACHE_TTL_SECONDS)
def build_revenue_chart(start_date, end_date):
    # Chart building logic
    pass
```

**Keying**: Automatic by function signature (date range + parameters)

**Rationale**: Eliminates repeated DuckDB scans and Plotly figure construction for identical date ranges.

### 3. Progressive Loading via dcc.Store (KPI First, Then Charts/Tables)

**Pattern**: Add a hidden `dcc.Store(id='*-query-context')` to the page layout.

**Implementation Flow**:
```python
# 1. User clicks Apply → KPI callback runs
@app.callback(
    Output('sales-query-context', 'data'),
    Input('apply-button', 'n_clicks'),
    [State('date-picker', 'start_date'), State('date-picker', 'end_date')]
)
def update_kpi_and_store(n_clicks, start_date, end_date):
    # Run KPI queries
    kpi_data = query_kpi_summary(start_date, end_date)
    
    # Write query context to store
    query_context = {
        'start_date': start_date,
        'end_date': end_date,
        'timestamp': datetime.now().isoformat()
    }
    
    return query_context

# 2. Heavy callbacks trigger from store
@app.callback(
    Output('revenue-chart', 'figure'),
    Input('sales-query-context', 'data')
)
def update_heavy_charts(data):
    if not data:
        raise PreventUpdate
    
    # Heavy chart building
    return build_revenue_chart(data['start_date'], data['end_date'])
```

**UX Benefit**: KPIs render immediately; heavy visualizations populate shortly after.

**Implementation Notes**:
- KPI callback must output the store data (ISO dates and minimal metadata)
- Heavy callbacks must guard on missing store data (`if not data: raise PreventUpdate`)
- Keep store payload small (dates + flags); do not store large DataFrames

### 4. Consolidated Queries; Minimize Per-Callback DuckDB Roundtrips

**Goal**: Reduce the number of DuckDB queries per Apply to as few as possible.

**Techniques**:
```sql
-- Use CTEs and FILTER clauses for multiple metrics
WITH daily_metrics AS (
    SELECT 
        date,
        SUM(revenue) as total_revenue,
        COUNT(DISTINCT order_id) as order_count,
        SUM(quantity) as total_quantity
    FROM fact_sales 
    WHERE date >= ? AND date <= ?
    GROUP BY date
)
SELECT 
    SUM(total_revenue) as revenue,
    SUM(order_count) as orders,
    SUM(total_quantity) as items
FROM daily_metrics;
```

**Best Practices**:
- Use CTEs and FILTER clauses to compute multiple metrics in one scan
- Prefer DuckDB-side aggregation over Python-side `pivot_table`/groupby when feasible
- For complex charts (Sankey, Heatmap), return "edge list" or pre-pivoted shapes from DuckDB
- Avoid multiple roundtrips for related data

**Rationale**: Set-based work in DuckDB is faster than Python loops and reduces Python CPU/memory pressure.

### 5. Timing Instrumentation Everywhere

**Policy**: Every callback and every query function must log start/end timing.

**Implementation Pattern**:
```python
import time
import logging

def _log_timing(name, start_time):
    elapsed = time.time() - start_time
    logging.info(f"[TIMING] {name}: {elapsed:.3f}s")

# In callbacks
def update_dashboard(n_clicks, start_date, end_date):
    start = time.time()
    # ... callback logic
    _log_timing('update_dashboard', start)
    return results

# In query functions
def query_revenue_summary(start_date, end_date):
    start = time.time()
    # ... query logic
    elapsed = time.time() - start
    print(f"[TIMING] query_revenue_summary: {elapsed:.3f}s")
    return result
```

**Output Format**: `[TIMING] <function_name>: <elapsed_seconds>.3fs`

**Rationale**: Enables data-driven performance tuning and rapid regression detection.

---

## Query Strategy Guidelines

### 1. Date Predicates Must Be Pushed Down

**Always** filter by date in DuckDB:
```sql
-- GOOD: Date filter in SQL
SELECT * FROM fact_sales 
WHERE date >= '2026-01-01' AND date < '2026-02-01';

-- BAD: Pull full table then filter in Python
df = duckdb.query("SELECT * FROM fact_sales").to_df()
filtered = df[df['date'] >= '2026-01-01']
```

### 2. Prefer Single-Scan Aggregates

Use a single query with GROUP BY/CASE WHEN to compute multiple KPIs:
```sql
-- GOOD: Single scan for multiple metrics
SELECT 
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(quantity) as total_items,
    AVG(revenue) as avg_order_value
FROM fact_sales 
WHERE date >= ? AND date <= ?;

-- BAD: Multiple separate queries
revenue = duckdb.query("SELECT SUM(revenue) FROM fact_sales WHERE date >= ?")
orders = duckdb.query("SELECT COUNT(DISTINCT order_id) FROM fact_sales WHERE date >= ?")
```

### 3. Limit Python-side Reshaping

If a chart needs a pivot or edge list, try to produce that shape in DuckDB:
```sql
-- Heatmap: Return pre-shaped data
SELECT 
    date,
    hour,
    SUM(revenue) as hourly_revenue
FROM fact_sales 
WHERE date >= ? AND date <= ?
GROUP BY date, hour
ORDER BY date, hour;

-- Sankey: Return edge list
SELECT 
    source_category,
    target_category,
    SUM(revenue) as flow_value
FROM fact_sales f
JOIN dim_products p USING (product_id)
WHERE date >= ? AND date <= ?
GROUP BY source_category, target_category
ORDER BY flow_value DESC
LIMIT 50;
```

### 4. Use LIMIT/OFFSET for Large Result Sets

Top-N queries should apply LIMIT in DuckDB:
```sql
-- GOOD: Limit in SQL
SELECT product_id, SUM(revenue) as total_revenue
FROM fact_sales 
WHERE date >= ? AND date <= ?
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 20;

-- BAD: Fetch all then slice in Python
all_products = duckdb.query("SELECT product_id, SUM(revenue) FROM fact_sales GROUP BY product_id").to_df()
top_20 = all_products.nlargest(20, 'total_revenue')
```

---

## Implementation Checklist for New Pages

### Layout Setup
- [ ] Add `dcc.Store(id='*-query-context')` to page layout
- [ ] Implement KPI cards that render immediately
- [ ] Design heavy visualizations (charts, tables) to load progressively

### Callback Implementation
- [ ] Implement KPI callback that writes query context to store
- [ ] Wire heavy callbacks to read from store (not Apply button)
- [ ] Add `raise PreventUpdate` guards for missing store data
- [ ] Keep store payload small (dates + flags, not DataFrames)

### Performance Optimization
- [ ] Add `@cache.memoize()` to chart builders and expensive query functions
- [ ] Ensure all queries have date predicates and timing logs
- [ ] Prefer single-scan aggregates; avoid per-callback full scans
- [ ] Use LIMIT clauses for top-N results

### Caching Configuration
- [ ] Verify Docker Compose includes `DASH_CACHE_TTL_SECONDS`
- [ ] Check `REDIS_URL` configuration if using Redis
- [ ] Test cache hit: second Apply with same dates should be faster

### Testing & Validation
- [ ] Test progressive loading: KPIs should appear before charts
- [ ] Test cache behavior: repeated queries should be faster
- [ ] Verify timing logs appear in console output
- [ ] Check query performance: target < 2s for typical use cases

---

## Performance Monitoring

### Built-in Timing Logs
Every function should output timing information:
```
[TIMING] query_revenue_summary: 0.234s
[TIMING] build_revenue_chart: 0.156s
[TIMING] update_dashboard_callback: 0.392s
```

### Performance Monitoring Script
Use the monitoring script for ongoing performance tracking:
```bash
# Monitor profit ETL performance
python scripts/monitor_profit_performance.py --days 30 --verbose

# Check file counts and partition distribution
python scripts/monitor_profit_performance.py --check-files
```

### Performance Targets
- **KPI Queries**: < 1 second
- **Chart Queries**: < 2 seconds  
- **Full Page Load**: < 5 seconds (progressive loading)
- **Cache Hit Ratio**: > 70% for repeated queries

### Performance Regression Detection
- Monitor timing logs in production
- Set up alerts for queries exceeding targets
- Compare performance before/after code changes
- Track file counts and partition sizes

---

## Code Examples

### Cached Chart Builder
```python
from flask_caching import cache
import plotly.express as px

@cache.memoize(timeout=600)  # 10 minutes TTL
def build_revenue_trend_chart(start_date, end_date):
    start = time.time()
    
    query = """
    SELECT date, SUM(revenue) as daily_revenue
    FROM fact_sales 
    WHERE date >= ? AND date <= ?
    GROUP BY date
    ORDER BY date
    """
    
    df = duckdb.query(query, [start_date, end_date]).to_df()
    
    fig = px.line(df, x='date', y='daily_revenue', 
                  title='Revenue Trend')
    
    _log_timing('build_revenue_trend_chart', start)
    return fig
```

### Progressive Loading Callback
```python
@app.callback(
    [Output('kpi-cards', 'children'),
     Output('query-context', 'data')],
    Input('apply-button', 'n_clicks'),
    [State('date-picker', 'start_date'), 
     State('date-picker', 'end_date')]
)
def update_kpis_and_context(n_clicks, start_date, end_date):
    if not n_clicks:
        raise PreventUpdate
    
    start = time.time()
    
    # Fast KPI queries
    kpi_data = query_kpi_summary(start_date, end_date)
    kpi_cards = build_kpi_cards(kpi_data)
    
    # Store context for heavy callbacks
    context = {
        'start_date': start_date,
        'end_date': end_date,
        'timestamp': datetime.now().isoformat()
    }
    
    _log_timing('update_kpis_and_context', start)
    return kpi_cards, context

@app.callback(
    Output('revenue-chart', 'figure'),
    Input('query-context', 'data')
)
def update_revenue_chart(context):
    if not context:
        raise PreventUpdate
    
    return build_revenue_trend_chart(
        context['start_date'], 
        context['end_date']
    )
```

### Optimized Query Function
```python
def query_sales_summary(start_date, end_date):
    start = time.time()
    
    query = """
    WITH daily_metrics AS (
        SELECT 
            date,
            SUM(revenue) as daily_revenue,
            COUNT(DISTINCT order_id) as daily_orders,
            SUM(quantity) as daily_items,
            COUNT(DISTINCT customer_id) as daily_customers
        FROM fact_sales 
        WHERE date >= ? AND date <= ?
        GROUP BY date
    )
    SELECT 
        SUM(daily_revenue) as total_revenue,
        SUM(daily_orders) as total_orders,
        SUM(daily_items) as total_items,
        COUNT(DISTINCT date) as active_days,
        AVG(daily_customers) as avg_daily_customers
    FROM daily_metrics
    """
    
    result = duckdb.query(query, [start_date, end_date]).to_df()
    
    _log_timing('query_sales_summary', start)
    return result.iloc[0].to_dict()
```

---

## Environment Configuration

### Docker Compose Settings
```yaml
services:
  dash-app:
    environment:
      - DASH_CACHE_TTL_SECONDS=600
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - redis
```

### Cache Configuration
```python
# In app.py or services/cache.py
from flask_caching import Cache

cache = Cache()
cache.init_app(app, config={
    'CACHE_TYPE': 'redis' if os.getenv('REDIS_URL') else 'simple',
    'CACHE_REDIS_URL': os.getenv('REDIS_URL'),
    'CACHE_DEFAULT_TIMEOUT': int(os.getenv('DASH_CACHE_TTL_SECONDS', 600))
})
```

---

## Troubleshooting Performance Issues

### Common Symptoms & Solutions

#### Slow Initial Load
- **Symptom**: First query takes > 5 seconds
- **Cause**: Cold DuckDB, file scanning, no partition pruning
- **Solution**: Check date predicates, verify partition structure

#### Cache Not Working
- **Symptom**: Repeated queries same speed
- **Cause**: Missing cache decorator, Redis connection issues
- **Solution**: Verify `@cache.memoize()` applied, check Redis logs

#### Memory Issues
- **Symptom**: High memory usage, slow performance
- **Cause**: Large DataFrames in callbacks, insufficient limits
- **Solution**: Use LIMIT clauses, process in chunks, clear cache

#### Progressive Loading Not Working
- **Symptom**: Everything loads at once
- **Cause**: Missing dcc.Store, callbacks not chained correctly
- **Solution**: Verify store implementation, check callback dependencies

---

## Best Practices Summary

1. **Always** use DuckDB as the data source, never Odoo at runtime
2. **Always** apply server-side caching to expensive operations
3. **Always** implement progressive loading for better UX
4. **Always** consolidate queries to minimize roundtrips
5. **Always** log timing for performance monitoring
6. **Never** pull full tables into Python then filter
7. **Never** store large DataFrames in dcc.Store
8. **Never** skip date predicates in queries
9. **Never** ignore performance regression warnings
10. **Always** validate performance against targets

---

*This policy should be followed for all new dashboard development. Updates should be made as patterns evolve and new performance insights are discovered.*  
*Related workstream: NK_20260206_profit_etl_perf_9a2b*  
*Last updated: 2026-02-21*
