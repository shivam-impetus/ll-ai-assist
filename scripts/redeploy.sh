#!/bin/bash
# Redeploy/Update LeapLogic RAG API on Ubuntu EC2
# Run this script when you have code updates

set -e

echo "🔄 Starting LeapLogic RAG API update..."
echo ""

# Navigate to project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "📁 Project directory: $PROJECT_DIR"
echo ""

# Pull latest code from git
echo "📥 Pulling latest code from repository..."
git pull origin main

# Activate virtual environment
echo "🔧 Activating virtual environment..."
. "$PROJECT_DIR/venv/bin/activate"

# Update dependencies if requirements changed
echo "📚 Updating dependencies..."
pip install --upgrade pip
pip install -r "$PROJECT_DIR/requirements-py313.txt"

# Restart the service
echo "🔄 Restarting service..."
sudo systemctl restart leaplogic-api

# Wait a moment for service to start
sleep 2

# Check service status
echo ""
echo "✅ Update complete!"
echo ""
echo "Service status:"
sudo systemctl status leaplogic-api --no-pager

echo ""
echo "📋 Recent logs:"
sudo journalctl -u leaplogic-api -n 20 --no-pager

echo ""
echo "Useful commands:"
echo "  View live logs: sudo journalctl -u leaplogic-api -f"
echo "  Check status: sudo systemctl status leaplogic-api"
echo "  Restart: sudo systemctl restart leaplogic-api"
