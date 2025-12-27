# Stage 1: Build Stage
FROM python:3.9-slim AS build-stage

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libzstd-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy application code
COPY . .

# Stage 2: Final Stage
FROM python:3.9-slim

WORKDIR /app

# Copy installed packages and binaries from build stage
COPY --from=build-stage /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=build-stage /usr/local/bin /usr/local/bin
COPY --from=build-stage /app /app

# Create necessary directories
RUN mkdir -p /app/data-lake /app/logs /app/assets

ENV PYTHONPATH=/app

EXPOSE 8050

CMD ["gunicorn", "--workers", "1", "--threads", "1", "-b", "0.0.0.0:8050", "app:server"]