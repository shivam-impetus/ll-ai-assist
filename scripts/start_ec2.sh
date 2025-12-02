#!/bin/bash
# Quick start script for EC2 Ubuntu
# Usage: ./start_ec2.sh

set -e

# Navigate to project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    . venv/bin/activate
fi

# Set environment variables if needed
export PORT=${PORT:-8000}

echo "Starting LeapLogic RAG API on port $PORT..."
echo ""

# Run the API with venv uvicorn
venv/bin/uvicorn communication.rag_system_controller:app --host 0.0.0.0 --port $PORT
