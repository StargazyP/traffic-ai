"""One-shot frame capture for ROI tuning previews."""

from __future__ import annotations

import subprocess

import numpy as np


def grab_single_frame(
    url: str,
    *,
    width: int = 640,
    height: int = 360,
    timeout_sec: float = 12.0,
) -> np.ndarray | None:
    if not (url or "").strip():
        return None
    frame_size = int(width) * int(height) * 3
    cmd = ["ffmpeg", "-loglevel", "quiet"]
    if str(url).lower().startswith("rtsp"):
        cmd += ["-rtsp_transport", "tcp"]
    else:
        cmd += ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2"]
    cmd += [
        "-i",
        url.strip(),
        "-an",
        "-frames:v",
        "1",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "bgr24",
        "-vf",
        f"scale={int(width)}:{int(height)}",
        "pipe:1",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            timeout=max(3.0, float(timeout_sec)),
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    raw = proc.stdout or b""
    if len(raw) < frame_size:
        return None
    return np.frombuffer(raw[:frame_size], dtype=np.uint8).reshape((int(height), int(width), 3)).copy()
