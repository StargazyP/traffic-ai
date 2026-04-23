#!/usr/bin/env bash
set -euo pipefail

# Docker Compose로 app + MySQL 기동 (로컬 venv는 ./run.sh)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker 가 설치되어 있어야 합니다." >&2
  exit 1
fi

docker compose up --build -d "$@"

echo ""
echo "HTTP: http://127.0.0.1:8000  (MySQL: localhost:3306)"
echo "로그: docker compose logs -f app"
