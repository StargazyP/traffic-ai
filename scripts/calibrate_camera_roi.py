#!/usr/bin/env python3
"""Capture latest frames and suggest line_y_ratio per rotation site."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _latest_frame(cctv_name: str, wait_sec: float = 12.0):
    from queue import Empty

    from app.state import get_or_create_queue

    deadline = time.time() + wait_sec
    latest = None
    q = get_or_create_queue(cctv_name)
    while time.time() < deadline:
        try:
            while True:
                latest = q.get_nowait()
        except Empty:
            pass
        if latest is not None:
            break
        time.sleep(0.25)
    return latest.get("frame") if latest else None


def main() -> int:
    from app.config import LINE_Y_RATIO, get_camera_config, get_effective_rotation_sites
    from app.roi_calibration import calibration_summary, suggest_line_y_ratio
    from app.roi_geometry import compute_roi_rect

    default_top = float(__import__("os").getenv("YOLO_ROI_TOP_RATIO", "0.4"))
    sites = get_effective_rotation_sites()
    if not sites:
        print("no rotation sites", file=sys.stderr)
        return 1

    report = []
    for name, _url in sites:
        cfg = get_camera_config(name)
        frame = _latest_frame(name, wait_sec=8.0)
        suggested = None
        if frame is not None:
            h, w = frame.shape[:2]
            roi_y0, roi_x0, roi_x1, roi_h, _ = compute_roi_rect(h, w, cfg, default_top=default_top)
            roi = frame[roi_y0:h, roi_x0:roi_x1]
            suggested = suggest_line_y_ratio(roi)
            row = calibration_summary(
                name,
                cfg,
                frame_h=h,
                default_top=default_top,
                default_line_y_ratio=float(LINE_Y_RATIO),
                suggested_line_y_ratio=suggested,
            )
        else:
            row = {"cctv": name, "config": cfg, "frame_available": False}
        row["frame_available"] = frame is not None
        report.append(row)
        delta = row.get("delta_ratio")
        print(
            f"{name}: frame={'yes' if frame is not None else 'no'}"
            f" line_y_ratio={cfg.get('line_y_ratio', '-')}"
            f" suggested={suggested if suggested is not None else '-'}"
            f" delta={delta if delta is not None else '-'}"
        )

    out = ROOT / "data" / "roi_calibration_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
