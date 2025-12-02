#!/bin/bash
# Stop LeapLogic RAG API service
# Usage: ./stop_service.sh

set -e

echo "⏸️  Stopping LeapLogic RAG API service..."
echo ""

# Check if service exists
if ! systemctl list-unit-files | grep -q leaplogic-api.service; then
    echo "❌ Error: leaplogic-api service not found"
    echo "Service may not be installed or already removed"
    exit 1
fi

# Check if service is running
if ! systemctl is-active --quiet leaplogic-api; then
    echo "ℹ️  Service is already stopped"
    sudo systemctl status leaplogic-api --no-pager
    exit 0
fi

# Stop the service
sudo systemctl stop leaplogic-api

# Wait a moment
sleep 1

# Check service status
echo "✅ Service stopped!"
echo ""
echo "Service status:"
sudo systemctl status leaplogic-api --no-pager

echo ""
echo "To start the service again: sudo systemctl start leaplogic-api"
