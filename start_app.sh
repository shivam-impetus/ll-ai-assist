#!/usr/bin/env bash
set -euo pipefail

APP_PORT="8502"

# start_app.sh
# Activates local virtualenv if present, installs requirements (optional),
# and starts the Streamlit UI for the Gemma project.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Try to activate a .venv virtual environment if it exists
if [ -d ".venv" ]; then
  if [ -f ".venv/bin/activate" ]; then
    # Unix-style venv
    # shellcheck source=/dev/null
    source .venv/bin/activate
  elif [ -f ".venv/Scripts/activate" ]; then
    # Windows-style venv (Git Bash/MSYS)
    # shellcheck source=/dev/null
    source .venv/Scripts/activate
  fi
fi


# Allow overriding the port via the PORT environment variable

echo "Starting Streamlit app on port $APP_PORT"
streamlit run app.py --server.port "$APP_PORT"
