#!/bin/bash
# View logs for LeapLogic RAG API service
# Usage: ./logs_service.sh [lines]
# Example: ./logs_service.sh 50  (shows last 50 lines)

set -e

LINES=${1:-50}

echo "📜 Viewing LeapLogic RAG API service logs (last $LINES lines)..."
echo ""

# Check if service exists
if ! systemctl list-unit-files | grep -q leaplogic-api.service; then
    echo "❌ Error: leaplogic-api service not found"
    exit 1
fi

# Show logs
sudo journalctl -u leaplogic-api -n $LINES --no-pager

echo ""
echo "💡 Tips:"
echo "  View live logs: sudo journalctl -u leaplogic-api -f"
echo "  View logs since time: sudo journalctl -u leaplogic-api --since '10 minutes ago'"
echo "  View logs with priority: sudo journalctl -u leaplogic-api -p err"
echo "  Show more lines: ./logs_service.sh 100"
