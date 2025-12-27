# ETL Reliability Improvements

This document outlines the reliability improvements made to ensure the ETL pipeline runs consistently, even if Docker is not active during scheduled hours.

## Implemented Solutions

### 1. Docker Restart Policies
All services in `docker-compose.yml` now have `restart: unless-stopped` policy, ensuring containers restart automatically if they crash or if the Docker daemon restarts.

### 2. Catch-up Logic
The `daily_etl_pipeline` task now includes catch-up functionality:
- When run, it checks for the last successfully processed date
- Processes any missed dates between then and now
- Prevents data gaps from missed scheduled runs

### 3. Health Monitoring
A new `check_etl_health` task runs every 6 hours to:
- Verify recent data exists
- Detect if the ETL is falling behind
- Automatically trigger catch-up if needed

### 4. Redis Health Check
Redis service includes health checks to ensure it's properly running before other services start.

## Usage

### Manual Management (Windows)
Use the provided batch script:
```bash
# Start the ETL stack
manage-etl.bat start

# Check status
manage-etl.bat status

# View logs
manage-etl.bat logs

# Check ETL health
manage-etl.bat health

# Stop the stack
manage-etl.bat stop
```

### Manual Management (Linux/Mac)
```bash
# Start the stack
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Check health
docker-compose exec celery-worker python -c "from etl_tasks import check_etl_health; print(check_etl_health())"
```

### Systemd Service (Linux)
For production on Linux, install as a systemd service:
```bash
sudo ./setup-systemd.sh
```

## How It Works

### Scheduled Runs
- **Primary Schedule**: Daily at 2:00 AM via Celery Beat
- **Health Checks**: Every 6 hours (12:00 AM, 6:00 AM, 12:00 PM, 6:00 PM)

### Catch-up Process
1. When the ETL runs (scheduled or manual), it finds the last processed date
2. Calculates any missing days
3. Processes each missing day sequentially
4. Logs all catch-up activities

### Auto-Recovery
If health check detects missing data:
1. Logs a warning
2. Automatically triggers the ETL with catch-up enabled
3. Reports the action taken

## Monitoring

### Log Locations
- ETL Logs: `/app/logs/etl.log` (or `D:\logs\etl.log` on Windows)
- Docker Logs: `docker-compose logs -f`

### Health Check Output
The health check returns:
```json
{
  "status": "healthy|unhealthy|unknown|error",
  "last_data_date": "2025-12-26",
  "days_behind": 2,
  "action": "triggered_catch_up"
}
```

## Testing

### Test Catch-up Logic
```python
from etl_tasks import daily_etl_pipeline
# Run with catch-up enabled
result = daily_etl_pipeline("2025-12-26", catch_up=True)
```

### Test Health Check
```python
from etl_tasks import check_etl_health
result = check_etl_health()
print(result)
```

## Troubleshooting

### If Docker Stops at 2 AM
1. Docker will restart containers automatically when it comes back online
2. The next health check (within 6 hours) will detect missing data
3. Catch-up will automatically trigger

### Manual Recovery
If you need to force a catch-up:
```bash
# Using the batch script
manage-etl.bat health

# Or manually trigger
docker-compose exec celery-worker python -c "from etl_tasks import daily_etl_pipeline; daily_etl_pipeline('2025-12-26', catch_up=True)"
```

### Common Issues
- **Redis Connection**: Ensure Redis starts before other services (handled by health checks)
- **Data Gaps**: Check logs for "Catching up from" messages
- **Service Not Starting**: Verify Docker daemon is running and restart policies are active

## Best Practices

1. **Monitor Logs**: Regularly check ETL logs for catch-up activities
2. **Health Checks**: Run manual health checks after system maintenance
3. **Backup Data**: Keep backups of the data-lake directory
4. **Test Recovery**: Periodically test the catch-up mechanism

## Future Improvements

- Add email/webhook notifications for health alerts
- Implement more sophisticated retry logic
- Add metrics dashboard for monitoring
- Consider using Airflow for more complex scheduling needs
