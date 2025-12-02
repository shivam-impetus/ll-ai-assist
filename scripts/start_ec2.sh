#!/bin/bash
# Quick start script for EC2 Ubuntu
# Usage: ./start_ec2.sh

set -e

# Navigate to project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "📁 Project directory: $PROJECT_DIR"

# Activate virtual environment if it exists
if [ -d "$PROJECT_DIR/venv" ]; then
    . "$PROJECT_DIR/venv/bin/activate"
fi

# Set environment variables if needed
export PORT=${PORT:-8000}

echo "Starting LeapLogic RAG API on port $PORT..."
echo ""

# Run the API with venv uvicorn
"$PROJECT_DIR/venv/bin/uvicorn" communication.rag_system_controller:app --host 0.0.0.0 --port $PORT
