#!/bin/bash
# Validation script for QW-2: Container Memory Limits
# This script verifies that memory limits are properly configured

echo "=== Validating Memory Limits for Docker Services ==="
echo ""

echo "1. Checking dash-app memory limit..."
docker inspect dash-app 2>/dev/null | grep -i "\"Memory\"" | head -5

echo ""
echo "2. Checking celery-worker memory limit..."
docker inspect celery-worker 2>/dev/null | grep -i "\"Memory\"" | head -5

echo ""
echo "3. Checking runtime memory usage with docker stats..."
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}" dash-app celery-worker 2>/dev/null

echo ""
echo "=== Validation Complete ==="
echo ""
echo "Expected Results:"
echo "  - dash-app: Memory limit should be 2147483648 (2GB)"
echo "  - dash-app: MemoryReservation should be 1073741824 (1GB)"
echo "  - celery-worker: Memory limit should be 1073741824 (1GB)"
echo "  - celery-worker: MemoryReservation should be 536870912 (512MB)"
echo ""
echo "Note: These values are starting points. Adjust after measuring with:"
echo "  docker stats dash-app --no-stream"
echo "  docker stats celery-worker --no-stream"
