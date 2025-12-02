#!/bin/bash
# Restart LeapLogic RAG API service
# Usage: ./restart_service.sh

set -e

echo "🔄 Restarting LeapLogic RAG API service..."
echo ""

# Check if service exists
if ! systemctl list-unit-files | grep -q leaplogic-api.service; then
    echo "❌ Error: leaplogic-api service not found"
    echo "Please run deploy-ec2.sh first to set up the service"
    exit 1
fi

# Restart the service
sudo systemctl restart leaplogic-api

# Wait a moment for service to restart
sleep 2

# Check service status
echo "✅ Service restarted!"
echo ""
echo "Service status:"
sudo systemctl status leaplogic-api --no-pager

echo ""
echo "📋 Recent logs:"
sudo journalctl -u leaplogic-api -n 10 --no-pager

echo ""
echo "🌐 View live logs: sudo journalctl -u leaplogic-api -f"
