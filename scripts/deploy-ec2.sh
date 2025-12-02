#!/bin/bash
# Deploy LeapLogic RAG API on Ubuntu EC2
# Run this script on your EC2 instance

set -e

echo "🚀 Starting LeapLogic RAG API deployment on Ubuntu..."
echo ""

# Update system
echo "📦 Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install Python 3.13 if needed
echo "🐍 Checking Python version..."
if ! command -v python3.13 &> /dev/null; then
    echo "Installing Python 3.13..."
    sudo apt-get install -y software-properties-common
    sudo add-apt-repository -y ppa:deadsnakes/ppa
    sudo apt-get update
    sudo apt-get install -y python3.13 python3.13-venv python3.13-dev
fi

# Install pip
echo "📦 Installing pip..."
sudo apt-get install -y python3-pip

# Create virtual environment
echo "🔧 Creating virtual environment with Python 3.13..."
python3.13 -m venv venv
. venv/bin/activate

# Install dependencies
echo "📚 Installing Python dependencies (Python 3.13 compatible)..."
pip install --upgrade pip
pip install -r requirements-py313.txt

# Prompt for environment variables
echo ""
echo "🔑 Setting up environment variables..."
echo ""
read -p "Enter API_KEY (Google Gemini API key): " API_KEY
read -p "Enter GITHUB_PAT (GitHub Personal Access Token): " GITHUB_PAT
read -p "Enter PORT (default 8000): " PORT
PORT=${PORT:-8000}

# Create .env file for reference
echo "Creating .env file..."
cat > .env <<EOF
API_KEY=$API_KEY
GITHUB_PAT=$GITHUB_PAT
PORT=$PORT
EOF

echo "✅ Environment variables saved to .env file"

# Create systemd service
echo "⚙️ Creating systemd service..."
sudo tee /etc/systemd/system/leaplogic-api.service > /dev/null <<EOF
[Unit]
Description=LeapLogic RAG API Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin"
Environment="API_KEY=$API_KEY"
Environment="GITHUB_PAT=$GITHUB_PAT"
Environment="PORT=$PORT"
ExecStart=$(pwd)/venv/bin/uvicorn communication.rag_system_controller:app --host 0.0.0.0 --port $PORT
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start service
echo "🔄 Starting service..."
sudo systemctl daemon-reload
sudo systemctl enable leaplogic-api
sudo systemctl start leaplogic-api

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📋 Configuration Summary:"
echo "  API Key: ${API_KEY:0:20}..."
echo "  GitHub PAT: ${GITHUB_PAT:0:20}..."
echo "  Port: $PORT"
echo ""
echo "Service status:"
sudo systemctl status leaplogic-api --no-pager
echo ""
echo "🌐 API running at: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):$PORT"
echo "📖 API Docs: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):$PORT/docs"
echo ""
echo "Useful commands:"
echo "  View logs: sudo journalctl -u leaplogic-api -f"
echo "  Restart: sudo systemctl restart leaplogic-api"
echo "  Stop: sudo systemctl stop leaplogic-api"
echo "  Status: sudo systemctl status leaplogic-api"
echo ""
echo "⚠️  Note: Environment variables are stored in .env file and systemd service"
