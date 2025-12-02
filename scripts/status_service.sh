#!/bin/bash
# Check status of LeapLogic RAG API service
# Usage: ./status_service.sh

set -e

echo "📊 Checking LeapLogic RAG API service status..."
echo ""

# Check if service exists
if ! systemctl list-unit-files | grep -q leaplogic-api.service; then
    echo "❌ Error: leaplogic-api service not found"
    echo "Service may not be installed"
    exit 1
fi

# Show service status
echo "Service status:"
sudo systemctl status leaplogic-api --no-pager

echo ""
echo "📋 Recent logs (last 20 lines):"
sudo journalctl -u leaplogic-api -n 20 --no-pager

echo ""
echo "🔧 Service details:"
echo "  Config file: /etc/systemd/system/leaplogic-api.service"
echo "  Enabled on boot: $(systemctl is-enabled leaplogic-api 2>/dev/null || echo 'disabled')"
echo "  Active state: $(systemctl is-active leaplogic-api 2>/dev/null || echo 'inactive')"

echo ""
echo "Useful commands:"
echo "  View live logs: sudo journalctl -u leaplogic-api -f"
echo "  View full logs: sudo journalctl -u leaplogic-api"
echo "  Restart service: sudo systemctl restart leaplogic-api"
echo "  Start service: sudo systemctl start leaplogic-api"
echo "  Stop service: sudo systemctl stop leaplogic-api"
