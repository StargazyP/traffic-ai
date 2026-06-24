#!/usr/bin/env python3
"""ITS에서 로테이션 URL을 받아 data/rotation_sites_cache.json 에 저장."""

from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    import app.config  # noqa: F401 — loads .env

    from app.config import refresh_rotation_sites_from_its

    parser = argparse.ArgumentParser(description="Refresh rotation_sites_cache.json from ITS")
    parser.add_argument(
        "--force-its",
        action="store_true",
        help="CCTV_URL 이 있어도 ITS API 를 호출(한도 주의)",
    )
    args = parser.parse_args()

    result = refresh_rotation_sites_from_its(force=True, force_its=args.force_its)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get("skipped"):
        return 0
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
