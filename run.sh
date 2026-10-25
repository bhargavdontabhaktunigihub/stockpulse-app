#!/usr/bin/env bash
# Launch script for StockPulse Evaluator
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Starting StockPulse Due Diligence Web Application..."
echo "Opening on http://127.0.0.1:8000"
"$DIR/venv/bin/uvicorn" main:app --host 127.0.0.1 --port 8000 --reload
