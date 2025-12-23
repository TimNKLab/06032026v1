# Stage 1: Build Stage
FROM python:3.9-slim AS build-stage

# Set environment variables to optimize image
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Stage 2: Final Stage
FROM python:3.9-slim

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install gunicorn in the final stage
RUN pip install --no-cache-dir gunicorn

# Copy installed packages from build stage
COPY --from=build-stage /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=build-stage /usr/local/bin/celery /usr/local/bin/celery
COPY --from=build-stage /app /app

# Create necessary directories
RUN mkdir -p /app/data-lake /app/logs /app/assets

# Set environment variables
ENV PYTHONPATH=/app

# Expose port and run the application
EXPOSE 8050

# Default command (can be overridden in docker-compose)
CMD ["gunicorn", "--workers", "1", "--threads", "1", "-b", "0.0.0.0:8050", "app:server"]