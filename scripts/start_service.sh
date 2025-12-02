#!/bin/bash
# Start LeapLogic RAG API service
# Usage: ./start_service.sh

set -e

echo "🚀 Starting LeapLogic RAG API service..."
echo ""

# Check if service exists
if ! systemctl list-unit-files | grep -q leaplogic-api.service; then
    echo "❌ Error: leaplogic-api service not found"
    echo "Please run deploy-ec2.sh first to set up the service"
    exit 1
fi

# Start the service
sudo systemctl start leaplogic-api

# Wait a moment for service to start
sleep 2

# Check service status
echo "✅ Service started!"
echo ""
echo "Service status:"
sudo systemctl status leaplogic-api --no-pager

echo ""
echo "📋 Recent logs:"
sudo journalctl -u leaplogic-api -n 10 --no-pager

echo ""
echo "🌐 View live logs: sudo journalctl -u leaplogic-api -f"
