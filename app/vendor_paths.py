"""Optional vendored `websockets` only — must run before ultralytics/ByteTrack imports."""

from __future__ import annotations

import os
import sys


def ensure_websockets_vendor_on_path() -> None:
    """Prepend `.vendor/websockets` so vendored copy is used without shadowing PyPI `lap`."""
    vendor_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".vendor")
    ws_vendor = os.path.join(vendor_dir, "websockets")
    if os.path.isdir(ws_vendor) and ws_vendor not in sys.path:
        sys.path.insert(0, ws_vendor)
