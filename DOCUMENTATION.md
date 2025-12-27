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

version:

markdown
## ETL Process
### Scheduled ETL
- **Daily ETL**: Automatically runs at 2 AM UTC
- **Data Retention**: 90 days (configurable in [etl_tasks.py](cci:7://file:///d:/NKLabs/Plotly/nkdash/etl_tasks.py:0:0-0:0))
### Manual Triggers
#### Single Date ETL
```bash
# Basic usage
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.daily_etl_pipeline --args='["2025-12-23"]'
# With error handling
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.date_range_etl_pipeline --args='["2025-12-23"]'
Date Range ETL
bash
# Process a range of dates (inclusive)
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.date_range_etl_pipeline --args='["2025-12-01", "2025-12-31"]'
# Process current month (example for December 2025)
docker-compose exec celery-worker bash -c 'celery -A etl_tasks call etl_tasks.date_range_etl_pipeline --args="[\"2025-12-01\", \"2025-12-31\"]"'
Task Management
bash
# Force restart ETL workers
docker-compose exec celery-worker celery -A etl_tasks purge -f
docker-compose restart celery-worker
# List active tasks
docker-compose exec celery-worker celery -A etl_tasks inspect active
# Check task status
docker-compose exec celery-worker celery -A etl_tasks result <task_id>
Monitoring ETL Progress
bash
# View logs in real-time
docker-compose logs -f celery-worker
# Check Redis for ETL status
docker-compose exec redis redis-cli KEYS "etl:*"
docker-compose exec redis redis-cli GET "etl:status:<task_id>"
# Check processed dates
docker-compose exec celery-worker bash -c 'ls -l /app/data-lake/clean/pos_order_lines/year=2025/month=12/'
Error Recovery
bash
# Retry failed tasks
docker-compose exec celery-worker celery -A etl_tasks retry <task_id>
# Clean up stuck tasks
docker-compose exec redis redis-cli --scan --pattern "celery-task-meta-*" | xargs docker-compose exec redis redis-cli del
Data Validation
bash
# Check latest processed data
docker-compose exec celery-worker python -c "
import polars as pl
df = pl.scan_parquet('/app/data-lake/star-schema/fact_sales/*.parquet')
print(f'Total records: {df.collect().height:,}')
print(f'Date range: {df.select(pl.col(\"date\").min().alias(\"min_date\"), pl.col(\"date\").max().alias(\"max_date\")).collect().to_dicts()}')
"
Performance Tips
For large date ranges: Process in smaller batches (e.g., weekly chunks)
Memory management: Monitor Redis memory usage
Error handling: Check logs in /app/logs/etl.log
Storage: Clean up old data with:
bash
# Remove data older than 90 days
find /app/data-lake/raw -type f -mtime +90 -delete
find /app/data-lake/clean -type f -mtime +90 -delete
Common ETL Scenarios
1. Backfilling Historical Data
bash
# Process entire year 2025 in monthly chunks
for month in {1..12}; do
  month_padded=$(printf "%02d" $month)
  echo "Processing 2025-$month_padded"
  docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.date_range_etl_pipeline --args="[\"2025-$month_padded-01\", \"2025-$month_padded-31\"]"
done
2. Re-processing Failed Dates
bash
# Find failed dates (no clean data)
docker-compose exec celery-worker bash -c '
for date in {01..31}; do
  if [ ! -f "/app/data-lake/clean/pos_order_lines/year=2025/month=12/day=$date/pos_order_lines_clean_2025-12-$date.parquet" ]; then
    echo "2025-12-$date"
  fi
done'
3. Monitoring Daily Job
bash
# Check today's ETL status
TODAY=$(date +%Y-%m-%d)
docker-compose exec celery-worker bash -c '
  if [ -f "/app/data-lake/clean/pos_order_lines/year=${TODAY:0:4}/month=${TODAY:5:2}/day=${TODAY:8:2}/pos_order_lines_clean_$TODAY.parquet" ]; then
    echo "✅ ETL completed for $TODAY"
  else
    echo "❌ ETL pending for $TODAY"
  fi
'
This enhanced documentation now includes:

Clear examples for both single date and date range ETL
Comprehensive monitoring commands
Common scenarios with ready-to-use scripts
Error recovery procedures
Performance optimization tips
Would you like me to add any other specific scenarios or commands to the documentation?

Feedback submitted


Your recent terminal commands:
> docker-compose exec celery-worker celery -A etl_tasks inspect active
> docker-compose exec celery-worker celery -A etl_tasks inspect stats

yeah actually write that into the docs
I'll update the documentation with the enhanced monitoring and management commands. Here's the improved section:

markdown
## Monitoring
### Task Monitoring
#### Active Tasks
```bash
# View currently running tasks
docker-compose exec celery-worker celery -A etl_tasks inspect active
# Expected output when idle:
# -> celery@<hostname>: OK
#     - empty -
Worker Statistics
bash
# Get detailed worker metrics
docker-compose exec celery-worker celery -A etl_tasks inspect stats
# Sample output includes:
# - Broker connection details
# - Process stats (CPU/memory usage)
# - Task execution counts
# - Uptime and performance metrics
Scheduled Tasks
bash
# View scheduled (not yet started) tasks
docker-compose exec celery-worker celery -A etl_tasks inspect scheduled
# View registered tasks
docker-compose exec celery-worker celery -A etl_tasks inspect registered
Data Quality
Volume Inspection
bash
# List all Docker volumes
docker volume ls
# Inspect data volume
docker volume inspect nkdash_data-lake
Data Validation
bash
# Check product dimension
docker exec nkdash-celery-worker-1 python -c "
import polars as pl
df = pl.read_parquet('/app/data-lake/star-schema/dim_products.parquet')
print('📊 Product Dimension:')
print(f'Total products: {len(df):,}')
print('Sample categories:', df['product_category'].unique().to_list()[:5])
"
# Verify fact table
docker exec nkdash-celery-worker-1 python -c "
import polars as pl
from datetime import datetime, timedelta
# Find the most recent parquet file
import glob
latest_file = max(glob.glob('/app/data-lake/star-schema/fact_sales/*.parquet'), key=os.path.getmtime)
df = pl.scan_parquet(latest_file)
print(f'📅 Latest data file: {latest_file}')
print(f'Total records: {df.select(pl.count()).collect().item():,}')
print('Date range:', 
      df.select(pl.col('order_date').min().alias('min_date'), 
               pl.col('order_date').max().alias('max_date'))
       .collect().to_dicts())
"
Performance Tuning
Memory Management
bash
# Check Redis memory usage
docker-compose exec redis redis-cli info memory
# Monitor Docker container resources
docker stats $(docker ps --format '{{.Names}}')
Task Rate Limiting
bash
# Set concurrency (adjust based on system resources)
docker-compose exec celery-worker celery -A etl_tasks worker --concurrency=4 -l INFO
# Rate limit tasks (e.g., 10 tasks per minute)
docker-compose exec celery-worker celery -A etl_tasks control rate_limit etl_tasks.extract_pos_order_lines 10/m
Logging
View Logs
bash
# Follow all logs
docker-compose logs -f
# View specific service logs
docker-compose logs -f celery-worker
docker-compose logs -f web
# Check ETL logs
docker-compose exec celery-worker tail -f /app/logs/etl.log
Log Levels
bash
# Increase log verbosity
docker-compose exec celery-worker celery -A etl_tasks worker -l DEBUG
# Filter logs by log level
docker-compose logs celery-worker | grep -E 'ERROR|WARNING'
Health Checks
Service Health
bash
# Check Redis health
docker-compose exec redis redis-cli ping
# Check Celery worker status
docker-compose exec celery-worker celery -A etl_tasks inspect ping
# Check web server
curl -I http://localhost:8050/health
Data Health
bash
# Check for data completeness
docker exec nkdash-celery-worker-1 python -c "
import polars as pl
from datetime import datetime, timedelta
# Check last 7 days of data
dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
missing = []
for date in dates:
    path = f'/app/data-lake/clean/pos_order_lines/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/'
    if not pl.scan_parquet(f'{path}/*.parquet').collect().is_empty():
        print(f'✅ {date}: Data exists')
    else:
        print(f'❌ {date}: Missing data')
        missing.append(date)
if missing:
    print(f'\nMissing data for {len(missing)} days. To backfill:')
    print(f'docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.date_range_etl_pipeline --args=\'["{min(missing)}", "{max(missing)}"]\'')
"
Alerting
Setup Email Alerts
python
# In your Celery configuration
app.conf.worker_send_task_events = True
app.conf.task_send_sent_event = True
app.conf.worker_send_task_events = True
@app.task(bind=True, max_retries=3)
def alert_on_failure(self, exc, task_id, *args, **kwargs):
    # Implement your alerting logic here
    print(f"Alert! Task {task_id} failed: {exc}")
