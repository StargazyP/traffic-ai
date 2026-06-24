"""Auto-suggest virtual line position from a single ROI crop."""

from __future__ import annotations

import base64
from io import BytesIO
from typing import Any

import numpy as np


def suggest_line_y_ratio(
    roi_bgr: np.ndarray,
    *,
    search_lo: float = 0.52,
    search_hi: float = 0.90,
) -> float | None:
    """Pick y with strongest horizontal edge in the lower ROI band."""
    if roi_bgr is None or roi_bgr.size == 0:
        return None
    try:
        import cv2
    except ImportError:
        return None

    gray = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
    h, _w = gray.shape
    if h < 24:
        return None

    y0 = max(0, min(h - 8, int(h * search_lo)))
    y1 = max(y0 + 8, min(h, int(h * search_hi)))
    strip = gray[y0:y1, :]
    blur = cv2.GaussianBlur(strip, (5, 5), 0)
    sobel = cv2.Sobel(blur, cv2.CV_64F, 0, 1, ksize=3)
    row_energy = np.abs(sobel).mean(axis=1)
    if row_energy.size == 0:
        return None
    best_row = int(np.argmax(row_energy))
    line_y = y0 + best_row
    return min(0.95, max(0.05, line_y / max(1, h)))


def overlay_roi_preview_jpeg_b64(
    frame_bgr: np.ndarray,
    cam_cfg: dict[str, Any],
    *,
    default_top: float,
    default_line_y_ratio: float,
    suggested_line_y_ratio: float | None = None,
    jpeg_quality: int = 72,
) -> str:
    """Full frame with ROI box + current/suggested counting lines."""
    from PIL import Image, ImageDraw

    from app.roi_geometry import compute_line_y, compute_roi_rect

    if frame_bgr is None or frame_bgr.size == 0:
        return ""

    h, w = frame_bgr.shape[:2]
    roi_y0, roi_x0, roi_x1, roi_h, _roi_w = compute_roi_rect(
        h, w, cam_cfg, default_top=default_top
    )
    line_y, _line_global = compute_line_y(
        cam_cfg,
        frame_h=h,
        roi_y0=roi_y0,
        roi_h=roi_h,
        default_line_y_ratio=default_line_y_ratio,
    )

    rgb = np.ascontiguousarray(frame_bgr[:, :, ::-1])
    im = Image.fromarray(rgb, mode="RGB")
    dr = ImageDraw.Draw(im)
    dr.rectangle([roi_x0, roi_y0, roi_x1 - 1, h - 1], outline=(0, 215, 255), width=2)
    ly_global = roi_y0 + line_y
    dr.line([(roi_x0, ly_global), (roi_x1 - 1, ly_global)], fill=(255, 55, 55), width=3)
    if suggested_line_y_ratio is not None:
        sug_y = roi_y0 + int(float(suggested_line_y_ratio) * roi_h)
        dr.line([(roi_x0, sug_y), (roi_x1 - 1, sug_y)], fill=(80, 255, 120), width=2)
        dr.text((roi_x0 + 4, max(roi_y0 + 4, sug_y - 18)), "suggested", fill=(80, 255, 120))
    dr.text((roi_x0 + 4, roi_y0 + 4), "ROI", fill=(0, 215, 255))

    bio = BytesIO()
    im.save(bio, format="JPEG", quality=jpeg_quality, optimize=True)
    return base64.b64encode(bio.getvalue()).decode("ascii")


def calibration_summary(
    cctv_name: str,
    cam_cfg: dict[str, Any],
    *,
    frame_h: int,
    default_top: float,
    default_line_y_ratio: float,
    suggested_line_y_ratio: float | None,
) -> dict[str, Any]:
    from app.roi_geometry import compute_line_y, compute_roi_rect

    roi_y0, roi_x0, roi_x1, roi_h, roi_w = compute_roi_rect(
        frame_h, 640, cam_cfg, default_top=default_top
    )
    line_y, line_global = compute_line_y(
        cam_cfg,
        frame_h=frame_h,
        roi_y0=roi_y0,
        roi_h=roi_h,
        default_line_y_ratio=default_line_y_ratio,
    )
    current_ratio = line_y / max(1, roi_h)
    out: dict[str, Any] = {
        "cctv": cctv_name,
        "config": dict(cam_cfg),
        "roi": {
            "top": cam_cfg.get("top"),
            "left": cam_cfg.get("left"),
            "width": cam_cfg.get("width"),
            "line_y_ratio": cam_cfg.get("line_y_ratio", round(current_ratio, 4)),
            "line_y_roi_px": line_y,
            "line_y_global_px": line_global,
        },
        "suggested_line_y_ratio": suggested_line_y_ratio,
    }
    if suggested_line_y_ratio is not None:
        out["delta_ratio"] = round(float(suggested_line_y_ratio) - current_ratio, 4)
    return out
