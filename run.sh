#!/usr/bin/env bash
set -euo pipefail

if [ ! -d "venv" ]; then
  python3 -m venv venv
fi

source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

if [ ! -f "models/yolov8n.pt" ]; then
  mkdir -p models
  wget https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n.pt -O models/yolov8n.pt
fi

uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
