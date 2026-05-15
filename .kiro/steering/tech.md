# Tech Stack

**Frontend:** Dash 2.14.2 + Dash Mantine Components 2.4.0 + React 18  
**Backend:** Python 3.9 + Polars + Plotly + DuckDB 1.2.0  
**Orchestration:** Celery [redis] + Redis  
**Database:** DuckDB (in-process analytical DB)  
**Container:** Docker + Docker Compose  
**Auth:** Odoo RPC (jsonrpc+ssl)  
**Storage:** Parquet (columnar, partitioned by date)  

**Build/Run:**
- `docker-compose up --build` (all services)
- `docker-compose up dash-app celery-worker celery-beat redis` (selective)
- `docker-compose exec celery-worker python -c "from etl_tasks import daily_etl_pipeline; daily_etl_pipeline.delay('2025-12-24')"` (ETL)
- `docker-compose exec celery-worker python scripts/force_refresh_pos_data.py --start 2026-01-06 --end 2026-01-07 --targets pos` (manual refresh)
- `docker-compose exec web pytest tests/` (tests)
- `docker-compose exec web black .` / `flake8` (lint)

**Env vars (.env):**
```
ODOO_HOST, ODOO_PORT, ODOO_PROTOCOL, ODOO_DB, ODOO_USERNAME, ODOO_API_KEY
REDIS_URL=redis://redis:6379/0
DATA_LAKE_ROOT=/data-lake
```