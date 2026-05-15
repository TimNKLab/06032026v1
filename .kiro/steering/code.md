# Code Style & Conventions

## General Principles

- **Polars first** → ETL uses Polars, no pandas for data processing
- **DuckDB first** → Dashboard queries DuckDB, Odoo fallback only if DuckDB fails
- **Atomic writes** → All Parquet writes use `atomic_write_parquet()` from `etl/io_parquet.py`
- **Partition by date** → All fact tables partitioned `year=YYYY/month=MM/day=DD`
- **Single-file dimensions** → Dimensions stored as single parquet files (`dim_*.parquet`)
- **Compression** → `zstd` (defined in `etl/config.py`)

## ETL Code Style

### File Organization

```
etl/
├── extract/          # Extractors: extract_<dataset>_impl()
├── transform/        # Transformers: clean_<dataset>() (if implemented)
├── load/             # Loaders: update_<dataset>() (if implemented)
└── pipelines/        # Pipelines: <name>_pipeline_impl()
```

### Extractor Pattern

```python
# etl/extract/<dataset>.py
def extract_<dataset>_impl(target_date: str) -> Dict[str, Any]:
    """Extract <dataset> from Odoo for target_date."""
    odoo = get_pooled_odoo_connection()
    # Implementation
    return {
        'lines': list_of_records,
        'count': len(list_of_records),
        'target_date': target_date,
    }
```

### Cleaner Pattern

```python
# etl/transform/<dataset>.py
def clean_<dataset>(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw DataFrame to clean format."""
    return (
        raw_df
        .with_columns([
            pl.col('date').cast(pl.Date, strict=False),
            pl.col('quantity').fill_null(0),
        ])
    )
```

### Loader Pattern

```python
# etl/load/<dataset>.py
def update_<dataset>(clean_df: pl.DataFrame, target_date: str) -> str:
    """Load clean data to star schema, return output file path."""
    output_path = f'{STAR_SCHEMA_PATH}/<dataset>/year={year}/month={month}/day={day}/<dataset>_{target_date}.parquet'
    atomic_write_parquet(clean_df, output_path)
    return output_path
```

### Pipeline Pattern

```python
# etl/pipelines/<name>.py
def <name>_pipeline_impl(target_date: str) -> Dict[str, Any]:
    """Execute <name> pipeline for target_date."""
    # Extract
    extraction = extract_<dataset>_impl(target_date)
    
    # Save raw
    raw_path = save_raw_<dataset>.run(extraction)
    
    # Clean
    clean_path = clean_<dataset>.run(raw_path, target_date)
    
    # Load
    fact_path = update_<dataset>_star_schema.run(clean_path, target_date)
    
    return {
        'dataset': '<dataset>',
        'date': target_date,
        'records': extraction.get('count', 0),
        'raw_path': raw_path,
        'clean_path': clean_path,
        'fact_path': fact_path,
    }
```

### Task Wrapper Pattern (in `etl_tasks.py`)

```python
@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_<dataset>(self, target_date: str) -> Dict[str, Any]:
    """Celery task wrapper for <dataset> extraction."""
    from etl.extract.<dataset> import extract_<dataset>_impl
    return extract_<dataset>_impl(target_date)

@app.task
def save_raw_<dataset>(extraction_result: Dict[str, Any]) -> Optional[str]:
    """Celery task to save raw <dataset> to Parquet."""
    # Implementation
    return output_path

@app.task
def clean_<dataset>(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Celery task to clean <dataset>."""
    # Implementation
    return output_path
```

## Dashboard Service Code Style

### Metrics Pattern

```python
# services/<domain>_metrics.py
def get_<metric>_data(start_date: date, end_date: date, **kwargs) -> pd.DataFrame:
    """Get <metric> data using DuckDB. Return empty DataFrame on failure."""
    try:
        return query_<metric>(start_date, end_date, **kwargs)
    except Exception as e:
        print(f"DuckDB query failed in get_<metric>_data: {e}")
        return pd.DataFrame(columns=['expected', 'columns'])
```

### Chart Pattern

```python
# services/<domain>_charts.py
@cache.memoize()
def build_<chart>_chart(start_date: date, end_date: date, **kwargs) -> go.Figure:
    """Build Plotly chart for <chart>. Reuse metrics functions."""
    df = get_<metric>_data(start_date, end_date, **kwargs)
    
    if df.empty:
        # Return empty chart with "No data" annotation
        fig = go.Figure()
        fig.add_annotation(
            text='No data available for the selected period.',
            x=0.5, y=0.5,
            xref='paper', yref='paper',
            showarrow=False,
        )
        return fig
    
    # Build chart using Plotly
    fig = go.Figure()
    # ... chart implementation
    return fig
```

### Key Rules

- **Metrics first** → Charts call metrics, never query DuckDB directly
- **DuckDB only** → No live Odoo queries in metrics (fallback only if DuckDB fails)
- **Cache charts** → Use `@cache.memoize()` decorator on chart functions
- **Error handling** → Return empty DataFrame/figure on error, log exception
- **Date range validation** → Swap dates if `start_date > end_date`

## Testing Conventions

### Test Location

```
tests/
├── test_etl_*.py       # ETL task tests
├── test_metrics_*.py   # Metrics function tests
├── test_charts_*.py    # Chart rendering tests
└── test_integration.py # End-to-end pipeline tests
```

### Test Pattern

```python
# tests/test_etl_<dataset>.py
def test_extract_<dataset>_impl():
    """Test <dataset> extraction logic."""
    result = extract_<dataset>_impl('2025-01-01')
    assert 'lines' in result
    assert 'count' in result
    assert isinstance(result['lines'], list)

def test_clean_<dataset>():
    """Test <dataset> cleaning logic."""
    raw_df = pl.DataFrame({'date': ['2025-01-01'], 'quantity': [1]})
    clean_df = clean_<dataset>(raw_df)
    assert 'cleaned_column' in clean_df.columns
```

## Error Handling

### Odoo Operations

```python
@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_<dataset>(self, target_date: str) -> Dict[str, Any]:
    """Task with Odoo retry decorator."""
    # Implementation
```

### Parquet Writes

```python
from etl.io_parquet import atomic_write_parquet

def save_data(df: pl.DataFrame, path: str):
    """Atomic write to Parquet."""
    atomic_write_parquet(df, path)
```

### DuckDB Queries

```python
def query_<metric>(start_date: date, end_date: date) -> pd.DataFrame:
    """Query DuckDB, raise on error."""
    conn = get_duckdb_connection()
    try:
        return conn.execute(sql, params).fetchdf()
    except Exception as e:
        logger.error(f"DuckDB query failed: {e}")
        raise
```

## Naming Conventions

| Type | Pattern | Example |
|------|---------|---------|
| Extractor | `extract_<dataset>_impl()` | `extract_pos_order_lines_impl()` |
| Cleaner | `clean_<dataset>()` | `clean_pos_data()` |
| Loader | `update_<dataset>()` | `update_star_schema()` |
| Pipeline | `<name>_pipeline_impl()` | `daily_etl_pipeline_impl()` |
| Metric | `get_<metric>_data()` | `get_sales_trends_data()` |
| Query | `query_<metric>()` | `query_sales_trends()` |
| Chart | `build_<chart>_chart()` | `build_revenue_trend_chart()` |
| Task | `etl_tasks.<name>` | `etl_tasks.extract_pos_order_lines` |

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

## Security

- **Never commit `.env`** → Use `.gitignore`
- **Odoo credentials** → Read from environment only
- **API keys** → Store in `.env`, never in code
- **Data lake paths** → Use `DATA_LAKE_ROOT` env, never hardcode

## Performance

- **Batch Odoo queries** → Use `ODOO_BATCH_SIZE=500`
- **Polars streaming** → Use `streaming=True` for large transforms
- **DuckDB MVs** → Pre-load materialized views on startup
- **Cache charts** → Use `@cache.memoize()` for expensive charts
- **Atomic writes** → Prevent partial writes with temp file + rename