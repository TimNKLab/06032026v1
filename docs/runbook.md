# NKDash Operational Runbook

This document contains operational procedures, troubleshooting guides, and ETL task management for NKDash.

## Table of Contents
- [ETL Operations](#etl-operations)
- [Pipeline Management](#pipeline-management)
- [Troubleshooting](#troubleshooting)
- [System Health](#system-health)
- [Data Freshness](#data-freshness)

## ETL Operations

### Daily Pipeline Schedule
- **POS Data:** 02:00 daily
- **Invoice Sales:** 02:05 daily  
- **Purchases:** 02:10 daily
- **Inventory Moves:** 02:15 daily
- **Profit Aggregates:** 02:20 daily

### Manual ETL Operations

#### Force Refresh Specific Dataset
```bash
# POS data
python scripts/force_refresh_pos_data.py --date YYYY-MM-DD

# Invoice data
python scripts/force_refresh_purchase_data.py --date YYYY-MM-DD

# Stock data
python scripts/force_refresh_stock_quants.py --date YYYY-MM-DD

# Dimensions
python scripts/force_refresh_dimensions.py
```

#### Run Full Profit ETL
```bash
# Single date
python scripts/run_profit_etl.py --date YYYY-MM-DD

# Date range
python scripts/run_profit_etl.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD

# Validate profit calculations
python scripts/validate_profit_etl.py --date YYYY-MM-DD
```

## Pipeline Management

### Using Web Interface (ETL Ops)
1. Navigate to `/operational` in the dashboard
2. Select dataset from dropdown:
   - POS Sales
   - Invoice Sales  
   - Purchases
   - Inventory Moves
   - Profit (Cost + Aggreg)
3. Choose date range (max 31 days for async operations)
4. Click "Async Refresh" for large ranges or "Sync Refresh" for single days

### Using Docker Compose
```bash
# Start all services
docker-compose up -d

# View specific service logs
docker-compose logs -f celery-worker
docker-compose logs -f celery-beat
docker-compose logs -f dash-app

# Restart specific service
docker-compose restart celery-worker
```

## Troubleshooting

### Common Issues

#### ETL Pipeline Stuck
1. Check Redis connection: `docker-compose logs redis`
2. Check Celery worker: `docker-compose logs celery-worker`
3. Clear Redis cache: `docker-compose exec redis redis-cli FLUSHALL`
4. Restart services: `docker-compose restart`

#### Data Gaps in Dashboard
1. Check ETL metadata: View `/operational` page for last processed dates
2. Run force refresh for missing dates
3. Verify Odoo connection credentials in `.env`

#### DuckDB Performance Issues
1. Restart Dash app: `docker-compose restart dash-app`
2. Check parquet file sizes in `/data-lake/`
3. Monitor query performance: `python scripts/monitor_profit_performance.py`

#### Memory Issues
1. Check Docker resource limits
2. Reduce date range for ETL operations
3. Clear Redis cache: `docker-compose exec redis redis-cli FLUSHALL`

### Error Scenarios

#### Connection Timeout to Odoo
- **Symptoms:** ETL tasks fail with connection errors
- **Solution:** Verify ODOO_URL, ODOO_DB, ODOO_USER, ODOO_PASSWORD in `.env`
- **Prevention:** Check Odoo server status and network connectivity

#### Parquet File Corruption
- **Symptoms:** DuckDB queries return errors for specific dates
- **Solution:** Delete corrupted partition and re-run ETL for that date
- **Command:** `rm -rf /data-lake/star-schema/fact_sales/year=YYYY/month=MM/day=DD/`

#### Celery Task Queue Backlog
- **Symptoms:** ETL tasks running slowly, delayed execution
- **Solution:** 
  1. Check active tasks: `docker-compose exec celery-worker celery -A etl_tasks inspect active`
  2. Clear queue: `docker-compose exec celery-worker celery -A etl_tasks purge`
  3. Restart workers: `docker-compose restart celery-worker`

## System Health

### Health Check Endpoints
- **Dash App:** http://localhost:8050/health (if implemented)
- **Celery Flower:** http://localhost:5555 (if enabled)

### Monitoring Scripts
```bash
# Check profit ETL performance
python scripts/monitor_profit_performance.py --days 30 --verbose

# Validate data quality
python scripts/validate_profit_etl.py --date $(date -d "yesterday" +%Y-%m-%d)
```

### Log Locations
- **Application logs:** `logs/web/`
- **Celery logs:** `logs/celery/`
- **Docker logs:** `docker-compose logs`

## Data Freshness

### Checking Freshness
1. **Web Interface:** Visit `/operational` page
2. **Direct Query:** Check ETLMetadata in database
3. **File System:** Verify latest parquet partitions exist

### Freshness SLA
- **Target:** Data processed within 1 day of current date
- **Critical:** Gaps > 3 days require immediate attention
- **Acceptable:** Occasional 1-day delays during maintenance

### Recovery Procedures
1. **Identify missing dates** using operational dashboard
2. **Run force refresh** for each missing date
3. **Validate results** using validation scripts
4. **Update documentation** if recovery was due to known issue

## Performance Optimization

### Query Performance
- Use date filters in all queries
- Prefer aggregate tables for summary data
- Enable caching for repeated queries
- Monitor with timing logs

### ETL Performance  
- Process data in daily batches
- Use incremental updates for dimensions
- Monitor Celery task execution times
- Optimize Odoo API calls

### Storage Management
- Monitor parquet file sizes
- Compact old partitions if needed
- Archive old data periodically
- Clean up temporary files

## Security & Access

### Environment Variables
Keep sensitive data in `.env` file:
```
ODOO_URL=https://your-odoo-instance.com
ODOO_DB=database_name
ODOO_USER=username
ODOO_PASSWORD=password
REDIS_URL=redis://redis:6379/0
```

### Access Control
- Restrict access to ETL operations
- Use read-only users for dashboard access
- Secure Odoo API credentials
- Monitor access logs

## Backup & Recovery

### Data Backup
- **Parquet files:** Regular snapshots of `/data-lake/`
- **Database:** DuckDB views (recreatable)
- **Configuration:** `.env` and `docker-compose.yml`

### Recovery Procedures
1. Restore parquet files from backup
2. Restart all services
3. Validate data integrity
4. Check dashboard functionality

### Disaster Recovery
- Document recovery procedures
- Test backup restoration regularly
- Maintain off-site backups
- Have contact information for system administrators

---

*Last updated: 2026-02-21*
