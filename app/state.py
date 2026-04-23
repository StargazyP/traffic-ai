"""Process-wide mutable state for YOLO pipeline, counters, and rotation threads."""

from __future__ import annotations

import os
import threading
from datetime import datetime
from queue import Empty, Full, Queue
from typing import Any

status_lock = threading.Lock()
counter_lock = threading.Lock()
count_status: dict[str, Any] = {"cctv_name": "", "count": 0, "logs": []}
detection_status: dict[str, Any] = {
    "frame_width": 640,
    "frame_height": 360,
    "zone_top": 280,
    "zone_bottom": 320,
    "count": 0,
    "site_count": 0,
    "up_count": 0,
    "down_count": 0,
    "up_count_hard": 0,
    "down_count_hard": 0,
    "up_count_soft": 0,
    "down_count_soft": 0,
    "boxes": [],
    "timestamp": "",
    "cctv": "",
}
counter_thread: threading.Thread | None = None
counter_stop_event: threading.Event | None = None

sequencer_thread: threading.Thread | None = None
sequencer_stop = threading.Event()
sequencer_lock = threading.Lock()
_active_segment_stop: list[threading.Event | None] = [None]
_segment_lock = threading.Lock()

frame_queues: dict[str, Queue] = {}
frame_queues_lock = threading.Lock()
frame_queues_revision: int = 0
yolo_thread: threading.Thread | None = None

MODEL_PATH = os.getenv("MODEL_PATH", "models/yolov8n.pt")
model: Any = None

trackers: dict[str, Any] = {}
trackers_lock = threading.Lock()

rotation_tag_lock = threading.Lock()
rotation_active_cctv: str = ""

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
    try:
        q.put_nowait(item)
    except Full:
        try:
            q.get_nowait()
        except Empty:
            pass
        q.put_nowait(item)
