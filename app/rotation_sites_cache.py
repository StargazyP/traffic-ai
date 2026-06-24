"""Persist ITS-resolved rotation site URLs so quota/network errors do not wipe streams."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]


def cache_file_path() -> Path:
    raw = (os.getenv("ROTATION_SITES_CACHE_FILE") or "").strip()
    if raw:
        return Path(raw)
    return _ROOT / "data" / "rotation_sites_cache.json"


def load_rotation_sites_cache() -> list[tuple[str, str]]:
    path = cache_file_path()
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("rotation_sites_cache: read failed (%s): %s", path, exc)
        return []

    sites_raw = payload.get("sites") if isinstance(payload, dict) else None
    if not isinstance(sites_raw, list):
        return []

    out: list[tuple[str, str]] = []
    for row in sites_raw:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        url = str(row.get("url") or "").strip()
        if name and url:
            out.append((name, url))
    return out


def save_rotation_sites_cache(
    sites: list[tuple[str, str]],
    *,
    source: str = "its",
) -> None:
    if not sites:
        return
    path = cache_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "sites": [{"name": name, "url": url} for name, url in sites],
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    logger.info("rotation_sites_cache: saved %s sites → %s", len(sites), path)
