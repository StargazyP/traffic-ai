"""ByteTrack helpers and hybrid line-cross counting logic."""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import numpy as np
from ultralytics.trackers.byte_tracker import BYTETracker

from app import state as st

MAX_TRACK_POSITIONS = 30


class TrackDetections:
    """BYTETracker.update 입력용 최소 detections 컨테이너."""

    def __init__(self, xyxy: np.ndarray, conf: np.ndarray, cls: np.ndarray) -> None:
        self.xyxy = np.asarray(xyxy, dtype=np.float32).reshape(-1, 4)
        self.conf = np.asarray(conf, dtype=np.float32).reshape(-1)
        self.cls = np.asarray(cls, dtype=np.float32).reshape(-1)

    @property
    def xywh(self) -> np.ndarray:
        if len(self.xyxy) == 0:
            return np.empty((0, 4), dtype=np.float32)
        out = self.xyxy.copy()
        out[:, 2] = out[:, 2] - out[:, 0]
        out[:, 3] = out[:, 3] - out[:, 1]
        out[:, 0] = out[:, 0] + out[:, 2] / 2.0
        out[:, 1] = out[:, 1] + out[:, 3] / 2.0
        return out

    def __len__(self) -> int:
        return int(self.xyxy.shape[0])

    def __getitem__(self, idx):
        return TrackDetections(self.xyxy[idx], self.conf[idx], self.cls[idx])


def make_bytetrack_args() -> Any:
    return SimpleNamespace(
        track_high_thresh=float(os.getenv("BYTE_TRACK_THRESH", "0.5")),
        track_low_thresh=float(os.getenv("BYTE_TRACK_LOW_THRESH", "0.1")),
        new_track_thresh=float(os.getenv("BYTE_NEW_TRACK_THRESH", "0.5")),
        match_thresh=float(os.getenv("BYTE_MATCH_THRESH", "0.8")),
        track_buffer=int(os.getenv("BYTE_TRACK_BUFFER", "30")),
        fuse_score=False,
    )


def get_tracker(cctv_name: str) -> Any:
    """CCTV마다 별도 ByteTrack 상태 (track id 혼선 방지)."""
    with st.trackers_lock:
        if cctv_name not in st.trackers:
            args = make_bytetrack_args()
            try:
                st.trackers[cctv_name] = BYTETracker(args, frame_rate=30)
            except TypeError:
                st.trackers[cctv_name] = BYTETracker(args)
        return st.trackers[cctv_name]


def hybrid_cross_decision(
    positions: list[tuple[int, int]],
    line_y: int,
    min_move: float,
    *,
    soft_enable: bool,
    soft_min_dy: float,
    soft_margin: int,
) -> str | None:
    """
    Line-cross(primary) 후 Flow 보정(secondary).
    positions: (infer_seq, bbox_bottom_y) 시계열, 최신이 끝.
    반환: hard_up | hard_down | soft_up | soft_down | None
    """
    if len(positions) < 2:
        return None
    prev_y = positions[-2][1]
    curr_y = positions[-1][1]
    dy = curr_y - prev_y
    crossed = (prev_y - line_y) * (curr_y - line_y) < 0
    if crossed and abs(dy) >= float(min_move) and dy != 0:
        return "hard_down" if dy > 0 else "hard_up"
    if not soft_enable:
        return None
    if crossed:
        return None
    if abs(dy) < float(soft_min_dy):
        return None
    if len(positions) >= 3:
        d1 = positions[-1][1] - positions[-2][1]
        d2 = positions[-2][1] - positions[-3][1]
        if d1 == 0 or d2 == 0:
            return None
        if d1 * d2 < 0:
            return None
    near = min(abs(curr_y - line_y), abs(prev_y - line_y))
    if near > int(soft_margin):
        return None
    if dy > 0:
        return "soft_down"
    if dy < 0:
        return "soft_up"
    return None


__all__ = [
    "TrackDetections",
    "make_bytetrack_args",
    "get_tracker",
    "hybrid_cross_decision",
    "MAX_TRACK_POSITIONS",
]
