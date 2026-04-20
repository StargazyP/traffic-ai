#!/usr/bin/env bash
set -euo pipefail

# 프로젝트 내부 가상환경(로컬용). 필요하면 Docker 사용 권장.
VENV_DIR="${VENV_DIR:-.venv}"

if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# main.py는 루트의 yolov8n.pt를 참조하지만, 기존 스크립트 호환을 위해 models도 유지
if [ ! -f "yolov8n.pt" ] && [ ! -f "models/yolov8n.pt" ]; then
  mkdir -p models
  wget https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n.pt -O models/yolov8n.pt
fi

python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
