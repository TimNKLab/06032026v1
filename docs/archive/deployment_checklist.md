# NKDash Deployment Checklist

This document provides comprehensive checklists for deploying NKDash in different environments, ensuring consistent and reliable deployments.

## Table of Contents
- [Pre-Deployment Checklist](#pre-deployment-checklist)
- [Production Deployment](#production-deployment)
- [Development Environment Setup](#development-environment-setup)
- [Post-Deployment Verification](#post-deployment-verification)
- [Security Checklist](#security-checklist)
- [Monitoring Setup](#monitoring-setup)
- [Rollback Procedures](#rollback-procedures)

---

## Pre-Deployment Checklist

### Code & Documentation
- [ ] All code changes committed to main branch
- [ ] Version number updated in `SSOT.md` and changelog
- [ ] Documentation updated for new features
- [ ] API changes documented in `docs/api_reference.md`
- [ ] Database schema changes documented
- [ ] Configuration changes documented

### Testing & Quality
- [ ] Unit tests passing (`pytest tests/`)
- [ ] Integration tests passing
- [ ] Code linting completed (`black .`, `flake8`)
- [ ] Type checking completed (`mypy .`)
- [ ] Performance benchmarks run
- [ ] Security scan completed
- [ ] Manual testing of new features completed

### Dependencies & Versions
- [ ] `requirements.txt` updated with exact versions
- [ ] Docker base images updated to latest stable versions
- [ ] External API compatibility verified
- [ ] Database migration scripts prepared
- [ ] Breaking changes identified and documented

### Environment Preparation
- [ ] Target environment provisioned
- [ ] Network connectivity verified
- [ ] Storage requirements calculated and allocated
- [ ] Backup procedures tested
- [ ] Monitoring tools configured
- [ ] Access credentials prepared

---

## Production Deployment

### Infrastructure Setup

#### Server Requirements
```bash
# Minimum system requirements
CPU: 4 cores
RAM: 8GB
Storage: 100GB SSD
Network: 1Gbps

# Recommended for high load
CPU: 8 cores
RAM: 16GB
Storage: 500GB SSD
Network: 10Gbps
```

#### Docker Installation
```bash
# Install Docker and Docker Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### Directory Structure
```bash
# Create application directories
sudo mkdir -p /opt/nkdash/{app,data,logs,backups}
sudo chown -R $USER:$USER /opt/nkdash

# Set up data lake
mkdir -p /opt/nkdash/data/{raw,clean,star-schema,metadata}
mkdir -p /opt/nkdash/logs/{web,celery,redis}
```

### Configuration

#### Environment Variables
```bash
# Production .env template
cat > /opt/nkdash/app/.env << EOF
# Odoo Connection
ODOO_HOST=production-odoo.company.com
ODOO_PORT=443
ODOO_PROTOCOL=jsonrpc+ssl
ODOO_DB=production_db
ODOO_USERNAME=nkdash@company.com
ODOO_API_KEY=${ODOO_API_KEY}

# Data Lake Configuration
DATA_LAKE_ROOT=/opt/nkdash/data

# Redis Configuration
REDIS_URL=redis://redis:6379/0

# Performance Settings
CELERY_WORKER_CONCURRENCY=4
CELERY_TASK_SOFT_TIME_LIMIT=1800
CELERY_TASK_TIME_LIMIT=1900

# Caching
DASH_CACHE_TTL_SECONDS=600

# Security
SECRET_KEY=${SECRET_KEY}
DEBUG=False

# Monitoring
SENTRY_DSN=${SENTRY_DSN}
LOG_LEVEL=INFO
EOF
```

#### Docker Compose Production
```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    restart: unless-stopped
    networks:
      - nkdash-network

  celery-worker:
    build: ./app
    command: celery -A etl_tasks worker --loglevel=info --concurrency=4
    environment:
      - DATA_LAKE_ROOT=/opt/nkdash/data
    volumes:
      - /opt/nkdash/data:/opt/nkdash/data
      - /opt/nkdash/logs:/app/logs
    depends_on:
      - redis
    restart: unless-stopped
    networks:
      - nkdash-network
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G

  celery-beat:
    build: ./app
    command: celery -A etl_tasks beat --loglevel=info
    environment:
      - DATA_LAKE_ROOT=/opt/nkdash/data
    volumes:
      - /opt/nkdash/data:/opt/nkdash/data
      - /opt/nkdash/logs:/app/logs
    depends_on:
      - redis
    restart: unless-stopped
    networks:
      - nkdash-network

  dash-app:
    build: ./app
    ports:
      - "8050:8050"
    environment:
      - DATA_LAKE_ROOT=/opt/nkdash/data
    volumes:
      - /opt/nkdash/data:/opt/nkdash/data
      - /opt/nkdash/logs:/app/logs
    depends_on:
      - redis
    restart: unless-stopped
    networks:
      - nkdash-network
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 1G

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - /etc/letsencrypt:/etc/letsencrypt
    depends_on:
      - dash-app
    restart: unless-stopped
    networks:
      - nkdash-network

volumes:
  redis_data:

networks:
  nkdash-network:
    driver: bridge
```

#### Nginx Configuration
```nginx
# nginx.conf
events {
    worker_connections 1024;
}

http {
    upstream dash-app {
        server dash-app:8050;
    }

    server {
        listen 80;
        server_name nkdash.company.com;
        return 301 https://$server_name$request_uri;
    }

    server {
        listen 443 ssl http2;
        server_name nkdash.company.com;

        ssl_certificate /etc/letsencrypt/live/nkdash.company.com/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/nkdash.company.com/privkey.pem;

        location / {
            proxy_pass http://dash-app;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
```

### Deployment Steps

#### 1. Application Deployment
```bash
# Clone repository
cd /opt/nkdash/app
git clone https://github.com/company/nkdash.git .

# Build and start services
docker-compose -f docker-compose.prod.yml up --build -d

# Verify services are running
docker-compose -f docker-compose.prod.yml ps
```

#### 2. Database Initialization
```bash
# Run initial ETL to populate data
docker-compose -f docker-compose.prod.yml exec celery-worker python -c "
from etl_tasks import date_range_etl_pipeline
date_range_etl_pipeline('2025-01-01', '2025-12-31')
"
```

#### 3. SSL Certificate Setup
```bash
# Install certbot
sudo apt-get update
sudo apt-get install certbot python3-certbot-nginx

# Obtain SSL certificate
sudo certbot --nginx -d nkdash.company.com

# Set up auto-renewal
sudo crontab -e
# Add: 0 12 * * * /usr/bin/certbot renew --quiet
```

---

## Development Environment Setup

### Local Development
```bash
# Clone repository
git clone https://github.com/company/nkdash.git
cd nkdash

# Set up Python environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with development credentials

# Start services
docker-compose up -d redis
python app.py
```

### Docker Development
```bash
# Development docker-compose
docker-compose -f docker-compose.dev.yml up --build

# Access services
# Dashboard: http://localhost:8050
# Redis: localhost:6379
```

### IDE Configuration
```json
// .vscode/settings.json
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests/"]
}
```

---

## Post-Deployment Verification

### Health Checks
```bash
# Service status
docker-compose -f docker-compose.prod.yml ps

# Application health
curl -f https://nkdash.company.com/health

# ETL functionality
docker-compose -f docker-compose.prod.yml exec celery-worker python -c "
from etl_tasks import health_check
health_check.delay()
"

# Data freshness
docker-compose -f docker-compose.prod.yml exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('SELECT MAX(date) FROM fact_sales').fetchone()
print(f'Latest data: {result[0]}')
"
```

### Performance Verification
```bash
# Load testing
ab -n 1000 -c 10 https://nkdash.company.com/

# Memory usage
docker stats

# Query performance
docker-compose -f docker-compose.prod.yml exec dash-app python -c "
import time
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
start = time.time()
result = conn.execute('SELECT COUNT(*) FROM fact_sales WHERE date >= CURRENT_DATE - INTERVAL 30 DAY').fetchone()
elapsed = time.time() - start
print(f'Query time: {elapsed:.3f}s')
"
```

### Functional Testing
- [ ] Dashboard loads without errors
- [ ] All pages accessible
- [ ] Charts render correctly
- [ ] Data displays accurately
- [ ] Filters work properly
- [ ] ETL processes run successfully
- [ ] Error handling works as expected

---

## Security Checklist

### Network Security
- [ ] Firewall configured to allow only necessary ports
- [ ] SSL/TLS certificates installed and valid
- [ ] HTTP to HTTPS redirection configured
- [ ] DDoS protection enabled (if applicable)
- [ ] VPN access for administrative tasks

### Application Security
- [ ] Environment variables secured (no hardcoded secrets)
- [ ] Debug mode disabled in production
- [ ] Error messages don't expose sensitive information
- [ ] Input validation implemented
- [ ] SQL injection protection verified
- [ ] Cross-site scripting (XSS) protection enabled

### Data Security
- [ ] Database connections encrypted
- [ ] Backup data encrypted
- [ ] Access logs enabled and monitored
- [ ] Data retention policies implemented
- [ ] Personal data properly anonymized (if applicable)

### Access Control
- [ ] Strong passwords enforced
- [ ] Multi-factor authentication enabled (if applicable)
- [ ] Role-based access control implemented
- [ ] Administrative access limited
- [ ] Regular access reviews scheduled

---

## Monitoring Setup

### Application Monitoring
```bash
# Install monitoring agent
# Example: Prometheus + Grafana
docker run -d \
  --name prometheus \
  -p 9090:9090 \
  -v ./prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus

docker run -d \
  --name grafana \
  -p 3000:3000 \
  -e "GF_SECURITY_ADMIN_PASSWORD=admin" \
  grafana/grafana
```

### Log Monitoring
```bash
# Configure log rotation
sudo nano /etc/logrotate.d/nkdash

# Content:
/opt/nkdash/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 nkdash nkdash
    postrotate
        docker-compose -f /opt/nkdash/app/docker-compose.prod.yml restart dash-app celery-worker
    endscript
}
```

### Health Monitoring Script
```bash
#!/bin/bash
# health_monitor.sh

# Check service status
if ! docker-compose -f /opt/nkdash/app/docker-compose.prod.yml ps | grep -q "Up"; then
    echo "ERROR: Services not running"
    # Send alert
fi

# Check data freshness
LATEST_DATA=$(docker-compose -f /opt/nkdash/app/docker-compose.prod.yml exec -T dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('SELECT MAX(date) FROM fact_sales').fetchone()
print(result[0])
")

if [[ "$LATEST_DATA" < "$(date -d '2 days ago' +%Y-%m-%d)" ]]; then
    echo "WARNING: Data is stale (latest: $LATEST_DATA)"
    # Send alert
fi

# Check disk space
DISK_USAGE=$(df /opt/nkdash/data | awk 'NR==2 {print $5}' | sed 's/%//')
if [[ $DISK_USAGE -gt 80 ]]; then
    echo "WARNING: Disk usage is ${DISK_USAGE}%"
    # Send alert
fi
```

### Alert Configuration
```bash
# Set up cron job for health monitoring
crontab -e

# Add: */5 * * * * /opt/nkdash/scripts/health_monitor.sh
```

---

## Rollback Procedures

### Quick Rollback (Configuration Issues)
```bash
# Revert configuration changes
git checkout HEAD~1 -- docker-compose.prod.yml
docker-compose -f docker-compose.prod.yml up -d
```

### Application Rollback
```bash
# Tag current version
git tag rollback-$(date +%Y%m%d-%H%M%S)

# Switch to previous version
git checkout previous-release-tag
docker-compose -f docker-compose.prod.yml up --build -d

# Verify rollback
curl -f https://nkdash.company.com/health
```

### Data Rollback
```bash
# Stop ETL services
docker-compose -f docker-compose.prod.yml stop celery-worker celery-beat

# Restore data from backup
sudo systemctl stop docker
sudo rsync -av /opt/nkdash/backups/data-20250221/ /opt/nkdash/data/
sudo systemctl start docker

# Restart services
docker-compose -f docker-compose.prod.yml up -d

# Verify data integrity
docker-compose -f docker-compose.prod.yml exec dash-app python -c "
from services.duckdb_connector import get_duckdb_connection
conn = get_duckdb_connection()
result = conn.execute('SELECT COUNT(*) FROM fact_sales').fetchone()
print(f'Records after rollback: {result[0]}')
"
```

### Complete System Rollback
```bash
# Emergency rollback procedure
#!/bin/bash
# emergency_rollback.sh

echo "Starting emergency rollback..."

# 1. Stop all services
docker-compose -f docker-compose.prod.yml down

# 2. Restore previous Docker images
docker load < /opt/nkdash/backups/images-previous.tar

# 3. Restore configuration
git checkout previous-release-tag

# 4. Restore data
sudo rsync -av /opt/nkdash/backups/data-previous/ /opt/nkdash/data/

# 5. Start services
docker-compose -f docker-compose.prod.yml up -d

# 6. Verify
sleep 30
if curl -f https://nkdash.company.com/health; then
    echo "Rollback successful"
else
    echo "Rollback failed - manual intervention required"
    exit 1
fi
```

---

## Deployment Automation

### CI/CD Pipeline
```yaml
# .github/workflows/deploy.yml
name: Deploy to Production

on:
  push:
    tags:
      - 'v*'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Deploy to server
      uses: appleboy/ssh-action@v0.1.5
      with:
        host: ${{ secrets.HOST }}
        username: ${{ secrets.USERNAME }}
        key: ${{ secrets.SSH_KEY }}
        script: |
          cd /opt/nkdash/app
          git pull origin main
          docker-compose -f docker-compose.prod.yml up --build -d
          ./scripts/health_check.sh
```

### Automated Testing
```bash
# pre-deploy-test.sh
#!/bin/bash

echo "Running pre-deployment tests..."

# 1. Unit tests
pytest tests/ --maxfail=1
if [ $? -ne 0 ]; then
    echo "Unit tests failed"
    exit 1
fi

# 2. Integration tests
pytest tests/integration/ --maxfail=1
if [ $? -ne 0 ]; then
    echo "Integration tests failed"
    exit 1
fi

# 3. Security scan
bandit -r . -f json -o security-report.json
if [ $? -ne 0 ]; then
    echo "Security issues found"
    exit 1
fi

echo "All tests passed - ready for deployment"
```

---

## Maintenance Schedule

### Daily Tasks
- [ ] Check service health
- [ ] Verify data freshness
- [ ] Monitor disk usage
- [ ] Review error logs

### Weekly Tasks
- [ ] Update security patches
- [ ] Review performance metrics
- [ ] Clean up old logs
- [ ] Backup configuration

### Monthly Tasks
- [ ] Update dependencies
- [ ] Security audit
- [ ] Performance optimization review
- [ ] Disaster recovery testing

### Quarterly Tasks
- [ ] Major version updates
- [ ] Architecture review
- [ ] Capacity planning
- [ ] Security assessment

---

*This deployment checklist should be updated as the system evolves and new deployment requirements emerge.*  
*For troubleshooting deployment issues, see TROUBLESHOOTING.md*  
*For system architecture, see docs/ARCHITECTURE.md*  
*Last updated: 2026-02-21*
