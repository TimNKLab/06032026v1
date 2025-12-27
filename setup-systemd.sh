#!/bin/bash
# Setup script to install ETL stack as a systemd service

set -e

echo "Setting up ETL stack as systemd service..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root (use sudo)"
    exit 1
fi

# Check if docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Copy the service file
echo "Copying systemd service file..."
cp etl-stack.service /etc/systemd/system/

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# Enable the service
echo "Enabling ETL stack service..."
systemctl enable etl-stack

# Start the service
echo "Starting ETL stack service..."
systemctl start etl-stack

# Check status
echo "Checking service status..."
systemctl status etl-stack

echo ""
echo "ETL stack service has been installed and started!"
echo ""
echo "Commands to manage the service:"
echo "  sudo systemctl status etl-stack    # Check status"
echo "  sudo systemctl stop etl-stack     # Stop the service"
echo "  sudo systemctl start etl-stack    # Start the service"
echo "  sudo systemctl restart etl-stack  # Restart the service"
echo "  sudo journalctl -u etl-stack -f   # View logs"
