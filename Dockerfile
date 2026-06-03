# =============================================================================
# NKDash Solo Mode — Multi-stage Dockerfile
# =============================================================================
# Single container running: gunicorn (Dash) + streamlit (Admin) + scheduler (ETL)
# Process management: supervisord
# =============================================================================

# ── Stage 1: Build ─────────────────────────────────────────────────────
FROM python:3.11-slim AS build-stage

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ── Stage 2: Runtime ───────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Runtime system deps: curl (healthcheck), supervisor
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy from build stage
COPY --from=build-stage /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=build-stage /usr/local/bin /usr/local/bin
COPY --from=build-stage /app /app

# Create runtime directories
RUN mkdir -p /data-lake/star-schema \
             /data-lake/admin \
             /data-lake/admin/logs \
             /app/logs /app/assets \
             /var/run/supervisor /var/log/supervisor

# supervisord config
COPY supervisord.conf /etc/supervisor/supervisord.conf

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    DATA_LAKE_ROOT=/data-lake

# Ports: Dash BI (8050) + Streamlit Admin (8501)
EXPOSE 8050 8501

# Health check matches docker-compose
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD curl -f http://localhost:8050/health || exit 1

# Entrypoint: supervisord manages all 3 processes
CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
