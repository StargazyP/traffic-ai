"""ROI crop and virtual line resolution shared by YOLO workers."""

from __future__ import annotations

from typing import Any


def _float(cfg: dict[str, Any], *keys: str, default: float) -> float:
    for key in keys:
        if key in cfg:
            return float(cfg[key])
    return float(default)


def compute_roi_rect(
    frame_h: int,
    frame_w: int,
    cam_cfg: dict[str, Any],
    *,
    default_top: float,
    default_left: float = 0.0,
    default_width: float = 1.0,
) -> tuple[int, int, int, int, int]:
    """Return roi_y0, roi_x0, roi_x1, roi_h, roi_w."""
    h = max(1, int(frame_h))
    w = max(1, int(frame_w))

    roi_top_ratio = _float(cam_cfg, "top", "roi_top", default=default_top)
    roi_top_ratio = min(0.85, max(0.0, roi_top_ratio))
    roi_y0 = max(0, min(h - 6, int(h * roi_top_ratio)))

    left_ratio = min(0.95, max(0.0, _float(cam_cfg, "left", "roi_left", default=default_left)))
    width_ratio = min(1.0, max(0.2, _float(cam_cfg, "width", "roi_width", default=default_width)))
    roi_x0 = int(w * left_ratio)
    roi_w = int(w * width_ratio)
    roi_x1 = min(w, roi_x0 + roi_w)
    roi_h = max(1, h - roi_y0)
    return roi_y0, roi_x0, roi_x1, roi_h, max(1, roi_x1 - roi_x0)


def compute_line_y(
    cam_cfg: dict[str, Any],
    *,
    frame_h: int,
    roi_y0: int,
    roi_h: int,
    default_line_y_ratio: float,
) -> tuple[int, int]:
    """Return line_y in ROI coords and line_y_global in full-frame coords."""
    roi_h = max(1, int(roi_h))
    h = max(1, int(frame_h))

    if "line_y_ratio" in cam_cfg:
        ratio = min(0.95, max(0.05, float(cam_cfg["line_y_ratio"])))
        line_y = int(ratio * roi_h)
    elif "line_y" in cam_cfg:
        # Legacy pixel value calibrated at FRAME_HEIGHT=360 unless cal_h is set.
        cal_h = max(180, int(float(cam_cfg.get("cal_h", 360))))
        cal_top = _float(cam_cfg, "top", "roi_top", default=0.4)
        ref_roi_h = max(1, cal_h - int(cal_h * cal_top))
        line_y = int(float(cam_cfg["line_y"]) * (roi_h / ref_roi_h))
    else:
        ratio = min(0.95, max(0.05, float(default_line_y_ratio)))
        line_y = int(ratio * roi_h)

    line_y = max(2, min(roi_h - 3, line_y))
    line_y_global = max(2, min(h - 3, roi_y0 + line_y))
    line_y = max(2, min(roi_h - 3, line_y_global - roi_y0))
    return line_y, line_y_global


def crossing_params_for_camera(
    cam_cfg: dict[str, Any],
    *,
    roi_h: int,
    default_min_move: float,
    default_soft_min_dy: float,
    default_soft_margin: int,
) -> tuple[float, float, int]:
    """Per-CCTV line-crossing thresholds with optional overrides."""
    min_move = float(cam_cfg.get("min_move", default_min_move))
    soft_min_dy = float(cam_cfg.get("soft_min_dy", default_soft_min_dy))
    if "soft_margin" in cam_cfg:
        soft_margin = int(cam_cfg["soft_margin"])
    elif "soft_margin_ratio" in cam_cfg:
        soft_margin = int(float(cam_cfg["soft_margin_ratio"]) * max(1, int(roi_h)))
    else:
        soft_margin = int(default_soft_margin)
    soft_margin = max(8, min(max(1, int(roi_h)) - 4, soft_margin))
    return min_move, soft_min_dy, soft_margin
