"""ITS OpenAPI 보조: AUTOFETCH 스위치 (레거시 /start-count 등)."""

from __future__ import annotations

import os


def its_autofetch_disabled() -> bool:
    raw = (os.getenv("ITS_AUTOFETCH_DISABLED") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}
