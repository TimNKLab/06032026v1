# NKDash Troubleshooting Guide

This document provides comprehensive troubleshooting procedures for common issues, system failures, and operational problems in NKDash.

## Table of Contents
- [Quick Diagnostics](#quick-diagnostics)
- [Common Issues](#common-issues)
- [System Failures](#system-failures)
- [Data Issues](#data-issues)
- [Performance Issues](#performance-issues)
- [Recovery Procedures](#recovery-procedures)
- [Preventive Maintenance](#preventive-maintenance)

---

## Quick Diagnostics

### Health Check Commands
```bash
# Check all services status
docker-compose ps

# Check service logs
docker-compose logs --tail=50

# Check Celery workers
docker-compose exec celery-worker celery -A etl_tasks inspect active
docker-compose exec celery-worker celery -A etl_tasks stats

# Check Redis connection
docker-compose exec redis redis-cli ping

# Check data lake integrity
docker-compose exec celery-worker python -c "
import os
from pathlib import Path
dl = Path('/data-lake')
print(f'Data lake exists: {dl.exists()}')
print(f'Raw layer: {(dl / \"raw\").exists()}')
print(f'Clean layer: {(dl / \"clean\").exists()}')
print(f'Star schema: {(dl / \"star-schema\").exists()}')
"
```

### Dashboard Health Check
```bash
# Test dashboard accessibility
curl -f http://localhost:8050/health || echo "Dashboard not responding"

# Check DuckDB views
docker-compose exec dash-app python -c "
import sys
sys.path.append('/app')
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
tables = conn.execute('SHOW TABLES').fetchall()
print(f'DuckDB tables: {tables}')
"
```

---

## Common Issues

### 1. Connection Problems

#### Odoo Connection Failed
**Symptoms**: ETL tasks fail with connection errors, timeout messages

**Diagnosis**:
```bash
# Test Odoo connection
docker-compose exec web python -c "
from odoorpc_connector import get_odoo_connection
try:
    conn = get_odoo_connection()
    print('Odoo connection successful')
    print(f'Database: {conn.db}')
except Exception as e:
    print(f'Odoo connection failed: {e}')
"
```

**Solutions**:
1. **Check credentials** in `.env`:
   ```bash
   grep ODOO_ .env
   ```

2. **Verify network connectivity**:
   ```bash
   docker-compose exec web ping your-odoo-instance.odoo.com
   ```

3. **Check SSL certificates**:
   ```bash
   docker-compose exec web openssl s_client -connect your-odoo-instance.odoo.com:443
   ```

4. **Update credentials** and restart services:
   ```bash
   docker-compose restart celery-worker dash-app
   ```

#### Redis Connection Failed
**Symptoms**: Celery tasks not processing, connection refused errors

**Diagnosis**:
```bash
# Test Redis connection
docker-compose exec redis redis-cli ping
docker-compose exec celery-worker python -c "
import redis
r = redis.from_url('redis://redis:6379/0')
print(r.ping())
"
```

**Solutions**:
1. **Restart Redis**:
   ```bash
   docker-compose restart redis
   ```

2. **Clear Redis cache**:
   ```bash
   docker-compose exec redis redis-cli FLUSHALL
   ```

3. **Check Redis logs**:
   ```bash
   docker-compose logs redis
   ```

### 2. ETL Pipeline Issues

#### Tasks Not Running
**Symptoms**: No data processing, scheduled tasks not executing

**Diagnosis**:
```bash
# Check Celery Beat schedule
docker-compose exec celery-beat celery -A etl_tasks inspect scheduled

# Check active queues
docker-compose exec celery-worker celery -A etl_tasks inspect active_queues

# Check worker logs
docker-compose logs celery-worker | grep -E "(ERROR|WARNING|FAIL)"
```

**Solutions**:
1. **Restart Celery services**:
   ```bash
   docker-compose restart celery-worker celery-beat
   ```

2. **Clear task queue**:
   ```bash
   docker-compose exec celery-worker celery -A etl_tasks purge
   ```

3. **Manually trigger task**:
   ```bash
   docker-compose exec celery-worker python -c "
from etl_tasks import health_check
health_check.delay()
"
   ```

#### Data Processing Errors
**Symptoms**: Tasks fail with data validation errors, type mismatches

**Diagnosis**:
```bash
# Check specific task failure
docker-compose exec celery-worker celery -A etl_tasks inspect failed

# View detailed error logs
docker-compose logs celery-worker | grep -A 10 -B 5 "ERROR"

# Check data quality
docker-compose exec celery-worker python -c "
import polars as pl
try:
    df = pl.read_parquet('/data-lake/raw/pos_order_lines/year=2025/month=02/day=21/pos_order_lines_2025-02-21.parquet')
    print(f'Records: {len(df)}')
    print(f'Columns: {df.columns}')
    print(f'Null counts: {df.null_count()}')
except Exception as e:
    print(f'Data read error: {e}')
"
```

**Solutions**:
1. **Validate data schema**:
   ```bash
   # Check expected vs actual columns
   docker-compose exec celery-worker python -c "
expected = ['id', 'date_order', 'partner_id', 'lines', 'payments_id']
# Add validation logic
   "
   ```

2. **Reprocess failed date**:
   ```bash
   docker-compose exec celery-worker python -c "
from etl_tasks import daily_etl_pipeline
daily_etl_pipeline('2025-02-21')
"
   ```

3. **Check Odoo data availability**:
   ```bash
   docker-compose exec web python -c "
from odoorpc_connector import get_odoo_connection
conn = get_odoo_connection()
# Check if data exists for target date
   "
   ```

### 3. Dashboard Issues

#### Dashboard Not Loading
**Symptoms**: Browser shows loading spinner, 500 errors

**Diagnosis**:
```bash
# Check Dash app logs
docker-compose logs dash-app | tail -50

# Test app startup
docker-compose exec dash-app python -c "
import sys
sys.path.append('/app')
try:
    import app
    print('App import successful')
except Exception as e:
    print(f'App import failed: {e}')
"
```

**Solutions**:
1. **Restart Dash app**:
   ```bash
   docker-compose restart dash-app
   ```

2. **Check DuckDB connection**:
   ```bash
   docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
print('DuckDB connection successful')
   "
   ```

3. **Clear browser cache** and reload dashboard

#### Data Not Displaying
**Symptoms**: Charts empty, tables show no data

**Diagnosis**:
```bash
# Check DuckDB views
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('SELECT COUNT(*) FROM fact_sales').fetchone()
print(f'Fact sales count: {result[0]}')
"
```

**Solutions**:
1. **Refresh DuckDB views**:
   ```bash
   docker-compose restart dash-app
   ```

2. **Check data freshness**:
   ```bash
   # Check latest data date
   docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('SELECT MAX(date) FROM fact_sales').fetchone()
print(f'Latest data date: {result[0]}')
"
   ```

---

## System Failures

### Container Crashes

#### All Containers Down
**Symptoms**: `docker-compose ps` shows no running containers

**Recovery**:
```bash
# Restart all services
docker-compose down
docker-compose up -d

# Check for resource issues
docker system df
docker system prune -f
```

#### Specific Container Crashing
**Symptoms**: One container repeatedly restarts

**Diagnosis**:
```bash
# Check container logs
docker-compose logs <service-name>

# Check container status
docker-compose ps <service-name>

# Inspect container
docker inspect <container-name>
```

**Recovery**:
```bash
# Recreate container
docker-compose up -d --force-recreate <service-name>

# Check resource limits
docker stats
```

### Data Corruption

#### Parquet File Corruption
**Symptoms**: DuckDB queries fail with file read errors

**Diagnosis**:
```bash
# Check file integrity
docker-compose exec celery-worker python -c "
import pyarrow.parquet as pq
try:
    table = pq.read_table('/data-lake/star-schema/fact_sales/year=2025/month=02/day=21/fact_sales_2025-02-21.parquet')
    print('File read successfully')
except Exception as e:
    print(f'File corruption: {e}')
"
```

**Recovery**:
```bash
# Remove corrupted partition
docker-compose run --rm celery-worker bash -c "
rm -rf /data-lake/star-schema/fact_sales/year=2025/month=02/day=21/
"

# Reprocess data
docker-compose exec celery-worker python -c "
from etl_tasks import daily_etl_pipeline
daily_etl_pipeline('2025-02-21')
"
```

#### Metadata Corruption
**Symptoms**: ETL tasks can't determine last processed date

**Recovery**:
```bash
# Reset metadata
docker-compose run --rm celery-worker bash -c "
python - <<'PY'
import json, os
metadata_file = '/data-lake/metadata/etl_status.json'
os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
data = {'last_processed_date': '2023-01-01', 'last_updated': '2023-01-01T00:00:00'}
with open(metadata_file, 'w', encoding='utf-8') as fh:
    json.dump(data, fh, indent=2)
print('Metadata reset')
PY
"
```

---

## Data Issues

### Missing Data

#### Data Gaps in Time Series
**Symptoms**: Charts show gaps, missing dates in analysis

**Diagnosis**:
```bash
# Check for missing dates
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('''
    SELECT date 
    FROM generate_series('2025-02-01'::date, '2025-02-21'::date, INTERVAL '1 day') d(date)
    LEFT JOIN fact_sales f ON d.date = f.date 
    WHERE f.date IS NULL
''').fetchall()
print(f'Missing dates: {result}')
"
```

**Recovery**:
```bash
# Reprocess missing dates
docker-compose exec celery-worker python -c "
from etl_tasks import date_range_etl_pipeline
missing_dates = ['2025-02-15', '2025-02-16']  # Add missing dates
for date in missing_dates:
    date_range_etl_pipeline(date, date)
"
```

#### Incomplete Data for Date
**Symptoms**: Some records missing for specific date

**Diagnosis**:
```bash
# Compare record counts
docker-compose exec celery-worker python -c "
import polars as pl
raw_path = '/data-lake/raw/pos_order_lines/year=2025/month=02/day=21/'
clean_path = '/data-lake/clean/pos_order_lines/year=2025/month=02/day=21/'
fact_path = '/data-lake/star-schema/fact_sales/year=2025/month=02/day=21/'

raw_files = list(Path(raw_path).glob('*.parquet'))
clean_files = list(Path(clean_path).glob('*.parquet'))
fact_files = list(Path(fact_path).glob('*.parquet'))

print(f'Raw files: {len(raw_files)}')
print(f'Clean files: {len(clean_files)}')
print(f'Fact files: {len(fact_files)}')
"
```

### Data Quality Issues

#### Negative Values Where Not Expected
**Symptoms**: Negative revenue, impossible quantities

**Diagnosis**:
```bash
# Check for data anomalies
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('''
    SELECT COUNT(*) as negative_revenue 
    FROM fact_sales 
    WHERE revenue < 0
''').fetchone()
print(f'Negative revenue records: {result[0]}')
"
```

**Recovery**:
```bash
# Identify and fix source data
docker-compose exec web python -c "
from odoorpc_connector import get_odoo_connection
conn = get_odoo_connection()
# Query Odoo for problematic records
"
```

#### Duplicate Records
**Symptoms**: Overstated totals, duplicate transactions

**Diagnosis**:
```bash
# Check for duplicates
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('''
    SELECT order_id, COUNT(*) as duplicate_count
    FROM fact_sales 
    GROUP BY order_id 
    HAVING COUNT(*) > 1
''').fetchall()
print(f'Duplicate orders: {len(result)}')
"
```

---

## Performance Issues

### Slow Queries

#### Dashboard Response Time > 5 seconds
**Symptoms**: Charts take long to load, timeouts

**Diagnosis**:
```bash
# Check query performance
docker-compose exec dash-app python -c "
import time
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()

start = time.time()
result = conn.execute('SELECT COUNT(*) FROM fact_sales WHERE date >= CURRENT_DATE - INTERVAL 30 DAY').fetchone()
elapsed = time.time() - start
print(f'Query time: {elapsed:.3f}s')
"
```

**Solutions**:
1. **Check partition pruning**:
   ```bash
   # Verify partition structure
   find /data-lake/star-schema/fact_sales -name "*.parquet" | head -10
   ```

2. **Optimize queries**:
   ```bash
   # Add date predicates
   docker-compose exec dash-app python -c "
   # Example optimized query
   "
   ```

3. **Enable caching**:
   ```bash
   # Check cache configuration
   grep DASH_CACHE_TTL .env
   ```

### Memory Issues

#### Container Out of Memory
**Symptoms**: Container restarts, OOM killer messages

**Diagnosis**:
```bash
# Check memory usage
docker stats

# Check OOM events
dmesg | grep -i "killed process"
```

**Solutions**:
1. **Increase memory limits**:
   ```yaml
   # docker-compose.yml
   services:
     celery-worker:
       deploy:
         resources:
           limits:
             memory: 4G
   ```

2. **Optimize batch sizes**:
   ```bash
   # Reduce batch processing size
   grep -r "batch_size" etl/
   ```

3. **Process data in smaller chunks**:
   ```bash
   # Use date range processing instead of full history
   ```

---

## Recovery Procedures

### Complete System Recovery

#### Disaster Recovery Scenario
**Scenario**: Complete data loss, system corruption

**Recovery Steps**:
```bash
# 1. Stop all services
docker-compose down

# 2. Restore data lake from backup (if available)
# docker cp backup_data_lake/ nkdash-celery-worker-1:/data-lake/

# 3. Reset metadata
docker-compose run --rm celery-worker bash -c "
python - <<'PY'
import json, os
metadata_file = '/data-lake/metadata/etl_status.json'
os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
data = {'last_processed_date': '2023-01-01', 'last_updated': '2023-01-01T00:00:00'}
with open(metadata_file, 'w', encoding='utf-8') as fh:
    json.dump(data, fh, indent=2)
PY
"

# 4. Restart services
docker-compose up -d

# 5. Reprocess data (choose appropriate range)
docker-compose exec celery-worker python -c "
from etl_tasks import date_range_etl_pipeline
date_range_etl_pipeline('2025-01-01', '2025-02-21')
"
```

### Partial Recovery

#### Single Dataset Recovery
**Scenario**: One dataset corrupted, others intact

```bash
# Identify corrupted dataset
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
tables = conn.execute('SHOW TABLES').fetchall()
for table in tables:
    try:
        count = conn.execute(f'SELECT COUNT(*) FROM {table[0]}').fetchone()
        print(f'{table[0]}: {count[0]} records')
    except Exception as e:
        print(f'{table[0]}: ERROR - {e}')
"
```

```bash
# Remove corrupted dataset layers
docker-compose run --rm celery-worker bash -c "
rm -rf /data-lake/raw/pos_order_lines
rm -rf /data-lake/clean/pos_order_lines
rm -rf /data-lake/star-schema/fact_sales
mkdir -p /data-lake/raw/pos_order_lines
mkdir -p /data-lake/clean/pos_order_lines
mkdir -p /data-lake/star-schema/fact_sales
"

# Reprocess dataset
docker-compose exec celery-worker python -c "
from etl_tasks import date_range_etl_pipeline
date_range_etl_pipeline('2025-02-01', '2025-02-21')
"
```

---

## Preventive Maintenance

### Regular Health Checks

#### Daily Checklist
```bash
#!/bin/bash
# daily_health_check.sh

echo "=== Daily Health Check ==="

# 1. Service status
echo "1. Service Status:"
docker-compose ps

# 2. Recent ETL success
echo "2. Recent ETL Status:"
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('SELECT MAX(date) FROM fact_sales').fetchone()
print(f'Latest data: {result[0]}')
"

# 3. Error logs
echo "3. Recent Errors:"
docker-compose logs --since=24h | grep -i error | tail -5

# 4. Disk space
echo "4. Disk Usage:"
df -h /data-lake

# 5. Memory usage
echo "5. Memory Usage:"
docker stats --no-stream | grep -E "(CONTAINER|MEM USAGE)"
```

#### Weekly Maintenance
```bash
#!/bin/bash
# weekly_maintenance.sh

echo "=== Weekly Maintenance ==="

# 1. Clear old logs
find logs/ -name "*.log" -mtime +7 -delete

# 2. Optimize DuckDB
docker-compose exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
conn.execute('VACUUM')
conn.execute('CHECKPOINT')
print('DuckDB optimized')
"

# 3. Clean old parquet files (optional)
# find data-lake/ -name "*.parquet" -mtime +90 -delete

# 4. Backup Redis
docker-compose exec -T redis redis-cli SAVE
docker cp nkdash-redis-1:/data/dump.rdb ./backup_redis_$(date +%Y%m%d).rdb
```

### Monitoring Setup

#### Log Monitoring
```bash
# Set up log rotation
sudo nano /etc/logrotate.d/nkdash
```

```
/path/to/nkdash/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 root root
}
```

#### Alert Configuration
```bash
# Example health check with alerts
#!/bin/bash
# health_check_with_alerts.py

import subprocess
import smtplib
from datetime import datetime

def check_service_health():
    # Implementation for health checks
    pass

def send_alert(message):
    # Implementation for sending alerts
    pass

if __name__ == "__main__":
    if not check_service_health():
        send_alert("NKDash health check failed")
```

---

## Contact and Escalation

### When to Escalate
- System down for > 30 minutes
- Data corruption affecting business operations
- Security incidents or suspected breaches
- Performance degradation impacting users

### Information to Collect
```bash
# System information dump
docker-compose version
docker version
uname -a
df -h
free -h

# Service logs
docker-compose logs > nkdash_logs_$(date +%Y%m%d_%H%M%S).txt

# Configuration
docker-compose config > nkdash_config_$(date +%Y%m%d_%H%M%S).yml
```

---

*This troubleshooting guide should be updated as new issues are discovered and resolved.*  
*For ETL technical details, see ETL_GUIDE.md*  
*For operational procedures, see docs/runbook.md*  
*Last updated: 2026-02-21*
