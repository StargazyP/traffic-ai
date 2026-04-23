"""Debug ROI JPEG overlay for WebSocket payloads."""

from __future__ import annotations

import base64
from io import BytesIO

import numpy as np


def debug_image_roi_jpeg_b64(
    roi: np.ndarray,
    *,
    roi_y0: int,
    boxes: list[dict],
    max_w: int = 640,
    jpeg_quality: int = 60,
) -> str:
    """YOLO에 들어가는 roi 크롭(BGR) + 박스(ROI 로컬 좌표) 오버레이. 전체 원본 프레임은 미포함."""
    from PIL import Image, ImageDraw, ImageFont

    if roi.size == 0:
        return ""
    rgb = np.ascontiguousarray(roi[:, :, ::-1])
    im = Image.fromarray(rgb, mode="RGB")
    dr = ImageDraw.Draw(im)
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    rh, rw = roi.shape[:2]
    for b in boxes:
        x1b = int(b["x1"])
        x2b = int(b["x2"])
        y1b = int(b["y1"]) - int(roi_y0)
        y2b = int(b["y2"]) - int(roi_y0)
        x1b = max(0, min(rw - 1, x1b))
        x2b = max(0, min(rw - 1, x2b))
        y1b = max(0, min(rh - 1, y1b))
        y2b = max(0, min(rh - 1, y2b))
        if x2b <= x1b or y2b <= y1b:
            continue
        dr.rectangle([x1b, y1b, x2b, y2b], outline=(0, 255, 100), width=2)
        tid = b.get("track_id")
        if tid is not None:
            label = f"id:{int(tid)}"
            pos = (x1b, max(0, y1b - 12))
            if font is not None:
                dr.text(pos, label, fill=(255, 255, 80), font=font)
            else:
                dr.text(pos, label, fill=(255, 255, 80))

    tw, th = im.size
    if tw > max_w:
        nh = max(1, int(th * max_w / tw))
        im = im.resize((max_w, nh), Image.Resampling.BILINEAR)

    bio = BytesIO()
    im.save(bio, format="JPEG", quality=jpeg_quality, optimize=True)
    return base64.b64encode(bio.getvalue()).decode("ascii")
