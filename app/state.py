# 2026-04-28: DB 롤업 스레드 상태(rollup_thread/stop_event) 필드 추가.
"""Process-wide mutable state for YOLO pipeline, counters, and rotation threads."""

from __future__ import annotations

import os
import threading
from datetime import datetime
from queue import Empty, Full, Queue
from typing import Any

status_lock = threading.Lock()
counter_lock = threading.Lock()
count_status: dict[str, Any] = {
    "cctv_name": "",
    "count": 0,
    "up_count": 0,
    "down_count": 0,
    "main_flow_count": 0,
    "direction_score": 0.0,
    "duration_sec": 0.0,
    "flow_per_sec": 0.0,
    "time_bucket": "",
    "bucket_lag_sec": 0.0,
    "is_valid": True,
    "invalid_reason": "",
    "logs": [],
}
detection_status: dict[str, Any] = {
    "frame_width": 640,
    "frame_height": 360,
    "zone_top": 280,
    "zone_bottom": 320,
    "count": 0,
    "site_count": 0,
    "up_count": 0,
    "down_count": 0,
    "main_flow_count": 0,
    "direction_score": 0.0,
    "duration_sec": 0.0,
    "flow_per_sec": 0.0,
    "time_bucket": "",
    "time_bucket_epoch": 0,
    "bucket_lag_sec": 0.0,
    "is_valid": True,
    "invalid_reason": "",
    "prev_main_flow_count": None,
    "up_count_hard": 0,
    "down_count_hard": 0,
    "up_count_soft": 0,
    "down_count_soft": 0,
    "boxes": [],
    "frame_id": 0,
    "capture_ts": 0.0,
    "capture_time": "",
    "processed_ts": 0.0,
    "processed_time": "",
    "frame_age_ms": 0.0,
    "infer_latency_ms": 0.0,
    "roi_x0": 0,
    "roi_y0": 0,
    "roi_x1": 640,
    "roi_y1": 360,
    "roi_width": 640,
    "roi_height": 360,
    "line_y": 0,
    "line_y_global": 0,
    "zone_top_global": 280,
    "zone_bottom_global": 320,
    "active_tracks": 0,
    "persisted_tracks": 0,
    "counted_tracks": 0,
    "timestamp": "",
    "cctv": "",
}
counter_thread: threading.Thread | None = None
counter_stop_event: threading.Event | None = None
rollup_thread: threading.Thread | None = None
rollup_stop_event: threading.Event | None = None

sequencer_thread: threading.Thread | None = None
sequencer_stop = threading.Event()
sequencer_lock = threading.Lock()
_active_segment_stop: list[threading.Event | None] = [None]
_segment_lock = threading.Lock()

frame_queues: dict[str, Queue] = {}
frame_queues_lock = threading.Lock()
frame_queues_revision: int = 0
last_frames: dict[str, Any] = {}
last_frames_lock = threading.Lock()
yolo_thread: threading.Thread | None = None

MODEL_PATH = os.getenv("MODEL_PATH", "models/yolov8n.pt")
model: Any = None

trackers: dict[str, Any] = {}
trackers_lock = threading.Lock()

rotation_tag_lock = threading.Lock()
rotation_active_cctv: str = ""
rotation_active_group: list[str] = []
rotation_stream_urls: dict[str, str] = {}
rotation_stream_urls_lock = threading.Lock()

rotation_telemetry_lock = threading.Lock()
rotation_telemetry: dict = {
    "ingest_mode": "",
    "yolo_ingest_url_set": False,
    "last_infer_at": None,
    "infer_total": 0,
    "last_cctv_processed": "",
    "line_cross_note": (
        "하이브리드: (1) 가상선 교차=hard (2) 미검출 보정=flow soft(근선·강한 움직임·방향 일관). "
        "DB: up/down × hard·soft + 합계 컬럼."
    ),
}


def load_yolo_model() -> None:
    """Call once at startup after `ensure_websockets_vendor_on_path()`."""
    global model
    from ultralytics import YOLO

    model = YOLO(MODEL_PATH)


def telemetry_reset() -> None:
    with rotation_telemetry_lock:
        rotation_telemetry["last_infer_at"] = None
        rotation_telemetry["infer_total"] = 0
        rotation_telemetry["last_cctv_processed"] = ""


def telemetry_infer(cctv_name: str) -> None:
    with rotation_telemetry_lock:
        rotation_telemetry["last_infer_at"] = datetime.now().isoformat(timespec="seconds")
        rotation_telemetry["infer_total"] = int(rotation_telemetry.get("infer_total") or 0) + 1
        rotation_telemetry["last_cctv_processed"] = cctv_name


def get_or_create_queue(cctv_name: str) -> Queue:
    global frame_queues_revision
    with frame_queues_lock:
        if cctv_name not in frame_queues:
            qsize = 5
            try:
                qsize = max(1, int(os.getenv("YOLO_FRAME_QUEUE_SIZE", "5")))
            except Exception:
                qsize = 5
            frame_queues[cctv_name] = Queue(maxsize=qsize)
            frame_queues_revision += 1
        q = frame_queues[cctv_name]
    return q


def put_latest(q: Queue, item: dict) -> None:
    """큐가 가득 차면 오래된 프레임을 버리고 최신 프레임 삽입."""
    cctv = (item.get("cctv") or "").strip()
    frame = item.get("frame")
    if cctv and frame is not None:
        with last_frames_lock:
            last_frames[cctv] = frame
    try:
        q.put_nowait(item)
    except Full:
        try:
            q.get_nowait()
        except Empty:
            pass
        q.put_nowait(item)
