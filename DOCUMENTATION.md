# New Khatulistiwa KPI Dashboard - Complete Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Running the Application](#running-the-application)
6. [ETL Process](#etl-process)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)
9. [Development](#development)

## Overview

This dashboard provides real-time sales analytics for New Khatulistiwa, built with:
- **Frontend**: Plotly Dash with Dash Mantine Components
- **Backend**: Python (Polars, Celery, Redis)
- **Data Pipeline**: Odoo → Raw Parquet → Clean Parquet → Star Schema
- **Containerization**: Docker with multi-stage builds

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Odoo API access credentials
- Redis (included in Docker setup)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nkdash.git
   cd nkdash
   ```

2. Create and configure `.env` file:
   ```bash
   cp .env.example .env
   # Edit with your credentials
   ```

## Configuration

### Environment Variables (`.env`)
```ini
# Odoo Connection
ODOO_HOST=your-odoo-instance.odoo.com
ODOO_PORT=443
ODOO_PROTOCOL=jsonrpc+ssl
ODOO_DB=your_database
ODOO_USERNAME=your_email@example.com
ODOO_API_KEY=your_api_key

# Redis
REDIS_URL=redis://redis:6379/0

# Data Lake (defaults shown)
DATA_LAKE_ROOT=/app/data-lake
```

### Data Lake Structure
```
data-lake/
├── raw/            # Raw data from Odoo
├── clean/          # Processed data
└── star-schema/    # Analytics-ready data
```

## Running the Application

### Development Mode
```bash
# Start all services
docker-compose up --build

# Or start specific services
docker-compose up web celery-worker redis
```

### Production Mode
```bash
docker-compose -f docker-compose.prod.yml up --build -d
```

## ETL Process

### Manual Trigger
```bash
# Trigger ETL for specific date
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.daily_etl_pipeline --args='["2025-12-23"]'

# Force full refresh
docker-compose exec celery-worker celery -A etl_tasks purge -f
docker-compose restart celery-worker
```

### Scheduled Tasks
- **Daily ETL**: Runs at 2 AM UTC
- **Data Retention**: 90 days (configurable in `etl_tasks.py`)

## Monitoring

### Logs
```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f celery-worker
docker-compose logs -f web

# Check ETL progress
docker exec -it nkdash-redis-1 redis-cli KEYS "etl:*"
```

### Performance Metrics
```bash
# Check Celery stats
docker-compose exec celery-worker celery -A etl_tasks inspect stats

# Monitor task queue
docker-compose exec celery-worker celery -A etl_tasks inspect active
```

### Data Quality
```bash
# Check data volumes
docker volume ls
docker volume inspect nkdash_data-lake

# Sample data check
docker exec nkdash-celery-worker-1 python -c "import polars as pl; df = pl.read_parquet('/app/data-lake/star-schema/dim_products.parquet'); print(f'Total products: {len(df)}')"
```

## Troubleshooting

### Common Issues
1. **Connection Errors**
   ```bash
   # Test Odoo connection
   docker-compose exec web python -c "from odoorpc_connector import get_odoo_connection; print(get_odoo_connection().db.list())"
   ```

2. **ETL Stuck**
   ```bash
   # List active tasks
   docker-compose exec redis redis-cli KEYS "celery-task-meta-*" | xargs docker-compose exec redis redis-cli DEL
   ```

3. **Disk Space**
   ```bash
   # Clean old Parquet files
   find data-lake/ -name "*.parquet" -mtime +90 -delete
   ```

## Development

### Adding New Pages
1. Create new file in `pages/` directory
2. Follow Dash Mantine Components patterns
3. Access at `http://localhost:8050/page-name`

### Testing
```bash
# Run tests
docker-compose exec web pytest tests/

# Lint code
docker-compose exec web black .
docker-compose exec web flake8
```

### Backup & Restore
```bash
# Backup data
docker-compose exec -T redis redis-cli SAVE
docker cp nkdash-redis-1:/data/dump.rdb ./backup_$(date +%Y%m%d).rdb

# Restore
docker cp backup.rdb nkdash-redis-1:/data/dump.rdb
docker-compose restart redis
```
