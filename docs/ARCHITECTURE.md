# NK Dash – Data Lake & ETL Architecture

This document explains the decoupled **Data Lake + DuckDB** architecture implemented in December 2025.  It supersedes the original MVP (synchronous Odoo calls) and describes how data flows from Odoo → Parquet → DuckDB → Dash.

---

## 1. High-Level Components

| Component | Responsibility |
|-----------|----------------|
| **Celery Worker** | Executes ETL tasks (extract, clean, load, star-schema refresh). |
| **Celery Beat** | Schedules the daily ETL pipeline (2 AM default). |
| **Redis** | Message broker / backend for Celery. |
| **Parquet Data Lake** | Durable storage for raw, clean and star-schema files. |
| **DuckDB (in-process)** | Analytical engine queried directly by Dash. |
| **Dash App** | Serves the KPI dashboard, now reading from DuckDB instead of Odoo. |

The entire stack runs locally via **docker-compose** and can later be lifted to the cloud unchanged.

---

## 2. Directory Layout
```
/data-lake/
 ├── raw/                 # untouched Odoo JSON → Parquet
 │   └── pos_order_lines/
 │        year=YYYY/
 │          month=MM/
 │            day=DD/pos_order_lines_YYYY-MM-DD.parquet
 ├── clean/               # validated & typed records
 │   └── pos_order_lines/ (same partitioning)
 └── star-schema/         # analytics-ready tables (hybrid layout)
      ├── fact_sales/     # partitioned by date (large / append-only)
      │    year=YYYY/month=MM/day=DD/fact_sales_YYYY-MM-DD.parquet
      ├── dim_products.parquet      # slowly changing
      ├── dim_categories.parquet    # slowly changing
      └── dim_brands.parquet        # slowly changing
```

### Why Hybrid?
* **Facts** grow daily → date partitioning keeps files small and improves query pruning.
* **Dimensions** change rarely → single files avoid directory scans and are easier to back-up.

---

## 3. ETL Pipeline

*Entry-point:* `etl_tasks.daily_etl_pipeline`

1. **Extract** `pos.order.line` records for `target_date` via `odoorpc`.
2. **Save Raw** to `/raw/pos_order_lines/.../*.parquet`.
3. **Clean** rows (types, nulls, categories) → `/clean/pos_order_lines/.../*.parquet`.
4. **Load**
   * Append to `fact_sales/…` (partitioned)
   * Merge/Upsert into single-file dimensions (`dim_products`, `dim_categories`, `dim_brands`).

The pipeline is chained via Celery signatures so each step begins only after the previous completes.

### Scheduling
Configured in `etl_tasks.py`:
```python
app.conf.beat_schedule = {
    'daily-etl': {
        'task': 'etl_tasks.daily_etl_pipeline',
        'schedule': crontab(hour=2, minute=0),  # 02:00 every day
    },
}
```

---

## 4. DuckDB Integration

`services/duckdb_connector.py` creates **views** on start-up:

| View | Backing Files |
|------|---------------|
| `fact_sales` | `star-schema/fact_sales/*.parquet` |
| `dim_products` | `star-schema/dim_products.parquet` |
| `dim_categories` | `star-schema/dim_categories.parquet` |
| `dim_brands` | `star-schema/dim_brands.parquet` |

Because DuckDB is embedded, no server deployment is required—Dash imports this module and queries as if it were a SQL database.

---

## 5. docker-compose Usage
```bash
# Build & start all services
docker-compose up --build -d

# View logs
docker-compose logs -f dash-app
```

Environment variables (Odoo creds, etc.) are read from `.env` and automatically passed into worker containers.

---

## 6. Common Queries
```sql
-- Last 30-day sales trend
SELECT *
FROM fact_sales
WHERE date >= current_date - INTERVAL 30 DAY;

-- Top 20 products (current month)
SELECT p.product_id,
       p.product_category,
       SUM(f.quantity)        AS qty,
       SUM(f.revenue)         AS revenue
FROM fact_sales f
LEFT JOIN dim_products p USING (product_id)
WHERE date_trunc('month', date) = date_trunc('month', current_date)
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT 20;
```

---

## 7. Next Steps
1. **Benchmark** ETL runtime & DuckDB query latency.
2. **Cloud Storage**: Point `DATA_LAKE_PATH` to S3 / GCS and add DuckDB S3 credentials.
3. **Monitoring**: Add Flower for Celery, and Prometheus/Grafana for metrics.

---

*Last updated: 2025-12-05*
