# Project Structure

## Directory Layout

```
nkdash/
├── app.py                  # Dash app entry point (Flask server exposed as `server`)
├── etl_tasks.py            # Celery tasks (single entry point, all task names `etl_tasks.<name>`)
├── etl/                    # ETL module
│   ├── __init__.py
│   ├── config.py           # Constants, env parsing, data lake paths
│   ├── io_parquet.py       # Atomic write helper (`atomic_write_parquet`)
│   ├── metadata.py         # ETLMetadata class
│   ├── dimension_cache.py  # Dimension table loader
│   ├── odoo_pool.py        # Odoo connection pooling
│   ├── odoo_helpers.py     # Field extraction helpers (`safe_extract_m2o`, `batch_ids`, etc.)
│   ├── cache.py            # Redis cache wrapper
│   ├── extract/            # Extractors (`pos.py`, `invoices.py`, `inventory_moves.py`, `stock_quants.py`)
│   └── pipelines/          # Pipeline implementations (`daily.py`, `ranges.py`, `health.py`)
├── services/               # Dashboard backend services
│   ├── cache.py            # Flask-Caching wrapper
│   ├── versioned_cache.py  # Versioned cache helper
│   ├── duckdb_connector.py # DuckDB singleton manager (`DuckDBManager`)
│   ├── sales_metrics.py    # Sales metric calculations (DuckDB first, no Odoo fallback)
│   ├── sales_charts.py     # Sales Plotly charts (reuse metrics functions)
│   ├── profit_metrics.py   # Profit metric calculations
│   ├── profit_charts.py    # Profit Plotly charts
│   ├── inventory_metrics.py
│   ├── inventory_charts.py
│   ├── overview_metrics.py
│   └── etl_ops.py          # ETL operation helpers
├── pages/                  # Dash pages
│   ├── __init__.py
│   ├── home.py             # Overview page
│   ├── sales.py            # Sales page
│   ├── sales_drilldown.py  # Sales drilldowns
│   ├── inventory.py        # Inventory page
│   ├── customer.py         # Customer experience page
│   └── operational.py      # Data sync/ETL ops page
├── components/             # Reusable Dash components
│   ├── __init__.py
│   └── loading_modal.py
├── data-lake/              # Parquet data lake (via `DATA_LAKE_ROOT` env)
│   ├── raw/                # Raw Odoo data (partitioned by date)
│   │   ├── pos_order_lines/year=YYYY/month=MM/day=DD/
│   │   ├── account_move_out_invoice_lines/year=YYYY/month=MM/day=DD/
│   │   ├── account_move_in_invoice_lines/year=YYYY/month=MM/day=DD/
│   │   ├── inventory_moves/year=YYYY/month=MM/day=DD/
│   │   └── stock_quants/year=YYYY/month=MM/day=DD/
│   ├── clean/              # Cleaned/transformed data (same partitioning)
│   └── star-schema/        # Analytics-ready tables
│       ├── fact_sales/year=YYYY/month=MM/day=DD/      # POS facts
│       ├── fact_invoice_sales/year=YYYY/month=MM/day=DD/
│       ├── fact_purchases/year=YYYY/month=MM/day=DD/
│       ├── fact_inventory_moves/year=YYYY/month=MM/day=DD/
│       ├── fact_stock_on_hand_snapshot/year=YYYY/month=MM/day=DD/
│       ├── fact_product_cost_events/
│       ├── fact_product_cost_latest_daily/
│       ├── fact_product_beginning_costs/
│       ├── fact_product_legacy_costs/
│       ├── fact_product_costs_unified/
│       ├── fact_sales_lines_profit/
│       ├── agg_profit_daily/year=YYYY/month=MM/day=DD/
│       ├── agg_profit_daily_by_product/year=YYYY/month=MM/day=DD/
│       ├── agg_sales_daily/year=YYYY/month=MM/day=DD/
│       ├── agg_sales_daily_by_product/year=YYYY/month=MM/day=DD/
│       ├── agg_sales_daily_by_principal/year=YYYY/month=MM/day=DD/
│       └── dim_*.parquet  # Single-file dimensions (products, locations, uoms, partners, users, companies, lots)
├── docs/                   # Documentation
│   ├── ARCHITECTURE.md
│   ├── ETL_GUIDE.md
│   ├── DESIGN.md
│   ├── glossary.md
│   └── superpowers/        # Planning docs (specs/plans)
├── tests/                  # Unit/integration tests
├── scripts/                # CLI scripts (force_refresh_*.py, validate_*.py, run_profit_etl.py)
├── .kiro/                  # Kiro config (steering, hooks)
│   └── steering/           # Steering rules (product.md, tech.md, structure.md, code.md)
├── assets/                 # Static assets (custom.css)
├── docker-compose.yml      # Service definitions (dash-app, celery-worker, celery-beat, redis)
├── Dockerfile              # Multi-stage build
├── requirements.txt        # Python dependencies
└── .env                    # Environment variables (not committed)
```

## Key Conventions

- **ETL entry point:** `etl_tasks.py` (all tasks `etl_tasks.<name>`, Beat schedule/routing depend on this)
- **Data lake path:** `DATA_LAKE_ROOT` env (Docker: `/data-lake`, Windows host: `D:\data-lake`)
- **Partitioning:** All fact tables partitioned by date (`year=YYYY/month=MM/day=DD`)
- **Dimensions:** Single-file parquet (`dim_products.parquet`, etc.)
- **Atomic writes:** `atomic_write_parquet(df, path)` in `etl/io_parquet.py`
- **Compression:** `zstd` (defined in `etl/config.py`)

## Service Roles

| Service | Command | Purpose |
|---------|---------|---------|
| `dash-app` | `gunicorn -b 0.0.0.0:8050 app:server` | Dash web server |
| `celery-worker` | `celery -A etl_tasks worker -Q celery,extraction,transformation,loading,dimensions` | ETL task execution |
| `celery-beat` | `celery -A etl_tasks beat` | Scheduled task triggers |
| `redis` | `redis-server --appendonly yes --maxmemory 512mb` | Message broker + cache |

## Page Routes

- `/` → Overview (home.py)
- `/sales` → Sales (sales.py)
- `/sales-drilldown` → Sales Drilldowns (sales_drilldown.py)
- `/inventory` → Inventory (inventory.py)
- `/customer` → Customer Experience (customer.py)
- `/operational` → Data Sync/ETL Ops (operational.py)

## DuckDB Views

**Fast (overview page):** `mv_sales_daily`, `mv_profit_daily`  
**Full (all pages):** `fact_sales`, `fact_invoice_sales`, `fact_purchases`, `fact_inventory_moves`, `dim_products`, `dim_categories`, `dim_brands`, `agg_sales_daily*`, `agg_profit_daily*`, `mv_inventory_status`, `mv_product_velocity`

## ETL Pipeline Flow

```
Odoo (RPC) → extract_* → save_raw_* → clean_* → update_*_star_schema → DuckDB views
```

**Task queues:** `extraction`, `transformation`, `loading`, `dimensions`, `orchestration`

## Naming Conventions

- **Metrics:** `services/*_metrics.py` → `get_*_data()` / `query_*()` (DuckDB)
- **Charts:** `services/*_charts.py` → `build_*_chart()` (Plotly, reuse metrics)
- **Extractors:** `etl/extract/*.py` → `extract_*_impl()`
- **Cleaners:** `etl/transform/*.py` → `clean_*()` (if implemented)
- **Loaders:** `etl/load/*.py` → `update_*()` (if implemented)
- **Pipelines:** `etl/pipelines/*.py` → `*_pipeline_impl()`

## Environment Variables

```ini
ODOO_HOST, ODOO_PORT, ODOO_PROTOCOL, ODOO_DB, ODOO_USERNAME, ODOO_API_KEY
REDIS_URL=redis://redis:6379/0
DATA_LAKE_ROOT=/data-lake
TZ=Asia/Jakarta
```