# Stage 1: Build Stage
FROM python:3.9-slim-buster AS build-stage

# Set environment variables to optimize image
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Stage 2: Final Stage
FROM python:3.9-slim-buster AS final-stage

# Set work directory
WORKDIR /app

# Copy installed packages from build stage
COPY --from=build-stage /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=build-stage /app .

# Expose port and run the application
EXPOSE 8050
CMD ["gunicorn", "-b", "0.0.0.0:8050", "app:app.server"]