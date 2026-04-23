import os
import subprocess
import threading
import time
from datetime import datetime
from typing import Any

import numpy as np

# CPU 환경에서 NNPACK 경고 로그를 억제
os.environ.setdefault("PYTORCH_DISABLE_NNPACK", "1")
os.environ.setdefault("TORCH_CPP_LOG_LEVEL", "ERROR")

import torch
from ultralytics import YOLO

from db_mysql import insert_batch

# 일부 환경에서 환경변수만으로는 NNPACK 경고가 계속 출력되어
# torch 백엔드에서 직접 비활성화 시도
try:
    if hasattr(torch, "backends") and hasattr(torch.backends, "nnpack"):
        torch.backends.nnpack.enabled = False
except Exception:
    pass


model = YOLO("yolov8n.pt")
vehicle_classes = {2, 3, 5, 7}  # car, motorcycle, bus, truck
FRAME_SKIP = 2
MIN_MOVE = 1
BATCH_SIZE = 10

batch_buffer: list[tuple[str, int, int, int, int]] = []


def _iter_ffmpeg_frames(
    url: str,
    *,
    out_w: int = 640,
    out_h: int = 360,
    stop_event: threading.Event | None = None,
) -> Any:
    """ffmpeg decode → rawvideo(bgr24) → numpy frame generator."""
    frame_size = int(out_w) * int(out_h) * 3
    backoff = 0.2

    while True:
        if stop_event is not None and stop_event.is_set():
            return

        base_cmd = [
            "ffmpeg",
            "-loglevel",
            "quiet",
        ]
        if str(url).lower().startswith("rtsp"):
            base_cmd += ["-rtsp_transport", "tcp"]
        else:
            base_cmd += [
                "-reconnect",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_delay_max",
                "2",
            ]

        cmd = base_cmd + [
            "-i",
            url,
            "-an",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "bgr24",
            "-vf",
            f"scale={int(out_w)}:{int(out_h)}",
            "pipe:1",
        ]

        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=10**8,
            )
            if process.stdout is None:
                raise RuntimeError("ffmpeg stdout pipe is not available")

            backoff = 0.2
            while True:
                if stop_event is not None and stop_event.is_set():
                    return
                raw = process.stdout.read(frame_size)
                if not raw or len(raw) != frame_size:
                    break
                yield np.frombuffer(raw, dtype=np.uint8).reshape((int(out_h), int(out_w), 3))
        finally:
            if process is not None:
                try:
                    process.kill()
                except Exception:
                    pass

        if stop_event is not None and stop_event.is_set():
            return
        time.sleep(backoff)
        backoff = min(2.0, backoff * 1.5)


def add_to_batch(
    cctv_name: str,
    up_count: int,
    down_count: int,
    up_soft: int = 0,
    down_soft: int = 0,
) -> None:
    """누적 카운트: (상·하행 hard) + 선택적 soft. DB 스키마에 맞게 insert_batch가 처리."""
    global batch_buffer
    batch_buffer.append((cctv_name, up_count, down_count, up_soft, down_soft))
    if len(batch_buffer) >= BATCH_SIZE:
        flush_batch()


def flush_batch() -> None:
    global batch_buffer
    if not batch_buffer:
        return
    try:
        insert_batch(batch_buffer)
        print(f"[DB] batch insert {len(batch_buffer)}개")
    except Exception as e:
        print("[DB ERROR]", e)
    batch_buffer.clear()


def run_counter(
    cctv_url: str,
    cctv_name: str = "cctv1",
    *,
    stop_event: threading.Event | None = None,
) -> None:
    up_count = 0
    down_count = 0
    frame_count = 0

    track_history: dict[int, list[int]] = {}
    counted_ids: set[int] = set()
    track_last_seen: dict[int, int] = {}

    try:
        for frame in _iter_ffmpeg_frames(cctv_url, stop_event=stop_event):
            if stop_event and stop_event.is_set():
                break

            frame_count += 1
            if frame_count % FRAME_SKIP != 0:
                continue

            results = model.track(
                frame,
                persist=True,
                tracker="bytetrack.yaml",
                conf=0.4,
                iou=0.5,
                verbose=False,
            )
            if not results or results[0].boxes is None or results[0].boxes.id is None:
                continue

            boxes = results[0].boxes.xyxy.cpu().numpy()
            ids = results[0].boxes.id.cpu().numpy()
            classes = results[0].boxes.cls.cpu().numpy()

            for box, track_id_raw, cls in zip(boxes, ids, classes):
                if int(cls) not in vehicle_classes:
                    continue

                track_id = int(track_id_raw)
                x1, y1, x2, y2 = map(int, box)
                anchor_y = int(y2)

                track_history.setdefault(track_id, []).append(anchor_y)
                if len(track_history[track_id]) > 10:
                    track_history[track_id].pop(0)
                track_last_seen[track_id] = frame_count

                if len(track_history[track_id]) < 2:
                    continue

                prev_y = track_history[track_id][-2]
                curr_y = track_history[track_id][-1]
                dy = curr_y - prev_y
                if abs(dy) < MIN_MOVE:
                    continue

                if track_id not in counted_ids:
                    try:
                        from app.config import LINE_Y_RATIO
                        ratio = float(LINE_Y_RATIO)
                    except Exception:
                        ratio = float(os.getenv("LINE_Y_RATIO", "0.6"))
                    frame_h = frame.shape[0]
                    line_y = max(2, min(frame_h - 3, int(ratio * frame_h)))
                    crossed = (prev_y - line_y) * (curr_y - line_y) < 0
                    if crossed and dy > 0:
                        down_count += 1
                        counted_ids.add(track_id)
                        print(f"[하행] {down_count}")
                        add_to_batch(cctv_name, up_count, down_count)
                    elif crossed and dy < 0:
                        up_count += 1
                        counted_ids.add(track_id)
                        print(f"[상행] {up_count}")
                        add_to_batch(cctv_name, up_count, down_count)

            for tid in list(track_last_seen.keys()):
                if frame_count - track_last_seen[tid] > 300:
                    counted_ids.discard(tid)
                    track_last_seen.pop(tid, None)

    finally:
        flush_batch()
def run_counter_stream(
    cctv_url: str,
    cctv_name: str = "cctv1",
    *,
    on_event=None,
    on_detections=None,
    stop_event: Any = None,
) -> None:
    """HLS 원본 재생용: 카운트/DB 저장 + 박스 좌표 이벤트만 전달."""
    up_count = 0
    down_count = 0
    frame_count = 0
    track_history: dict[int, list[int]] = {}
    counted_ids: set[int] = set()
    track_last_seen: dict[int, int] = {}

    try:
        try:
            from app.config import FRAME_SKIP as CFG_FRAME_SKIP, MIN_MOVE as CFG_MIN_MOVE  # type: ignore
            effective_frame_skip = int(CFG_FRAME_SKIP)
            effective_min_move = float(CFG_MIN_MOVE)
        except Exception:
            effective_frame_skip = int(os.getenv("FRAME_SKIP", str(FRAME_SKIP)))
            effective_min_move = float(os.getenv("MIN_MOVE", str(MIN_MOVE)))
        try:
            track_iou = float(os.getenv("YOLO_TRACK_IOU", "0.3"))
        except Exception:
            track_iou = 0.3

        _stop: threading.Event | None = stop_event if isinstance(stop_event, threading.Event) else None
        for frame in _iter_ffmpeg_frames(cctv_url, stop_event=_stop):
            if stop_event and getattr(stop_event, "is_set", None) and stop_event.is_set():
                break

            frame_count += 1
            if effective_frame_skip > 1 and frame_count % effective_frame_skip != 0:
                continue

            frame_h, frame_w = frame.shape[:2]
            payload_boxes = []
            try:
                from app.config import YOLO_TRACK_CONF, get_line_y_ratio_for_cctv

                conf = float(YOLO_TRACK_CONF)
                ratio = float(get_line_y_ratio_for_cctv(cctv_name))
            except Exception:
                conf = float(os.getenv("YOLO_TRACK_CONF", "0.4"))
                raw_ratio = os.getenv("LINE_Y_RATIO", "0.6")
                if "하남" in (cctv_name or ""):
                    raw_ratio = os.getenv("LINE_Y_RATIO_HANAM", raw_ratio)
                ratio = float(raw_ratio)
            line_y = max(2, min(frame_h - 3, int(ratio * frame_h)))

            results = model.track(
                frame,
                persist=True,
                tracker="bytetrack.yaml",
                conf=conf,
                iou=float(track_iou),
                verbose=False,
            )
            if results and results[0].boxes is not None and results[0].boxes.id is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy()
                ids = results[0].boxes.id.cpu().numpy()
                classes = results[0].boxes.cls.cpu().numpy()

                for box, track_id_raw, cls in zip(boxes, ids, classes):
                    if int(cls) not in vehicle_classes:
                        continue

                    track_id = int(track_id_raw)
                    x1, y1, x2, y2 = map(int, box)
                    anchor_y = int(y2)

                    track_history.setdefault(track_id, []).append(anchor_y)
                    if len(track_history[track_id]) > 10:
                        track_history[track_id].pop(0)
                    track_last_seen[track_id] = frame_count

                    if len(track_history[track_id]) >= 2:
                        prev_y = track_history[track_id][-2]
                        curr_y = track_history[track_id][-1]
                        dy = curr_y - prev_y
                        if abs(dy) >= effective_min_move and track_id not in counted_ids:
                            crossed = (prev_y - line_y) * (curr_y - line_y) < 0
                            if crossed and dy > 0:
                                down_count += 1
                                counted_ids.add(track_id)
                                print(f"[하행] {down_count}")
                                add_to_batch(cctv_name, up_count, down_count)
                                if on_event:
                                    on_event(
                                        {
                                            "type": "count",
                                            "direction": "down",
                                            "up_count": up_count,
                                            "down_count": down_count,
                                            "count": up_count + down_count,
                                            "cctv_name": cctv_name,
                                            "track_id": track_id,
                                            "timestamp": datetime.now().isoformat(
                                                timespec="seconds"
                                            ),
                                        }
                                    )
                            elif crossed and dy < 0:
                                up_count += 1
                                counted_ids.add(track_id)
                                print(f"[상행] {up_count}")
                                add_to_batch(cctv_name, up_count, down_count)
                                if on_event:
                                    on_event(
                                        {
                                            "type": "count",
                                            "direction": "up",
                                            "up_count": up_count,
                                            "down_count": down_count,
                                            "count": up_count + down_count,
                                            "cctv_name": cctv_name,
                                            "track_id": track_id,
                                            "timestamp": datetime.now().isoformat(
                                                timespec="seconds"
                                            ),
                                        }
                                    )

                    payload_boxes.append(
                        {
                            "track_id": track_id,
                            "cls": int(cls),
                            "x1": x1,
                            "y1": y1,
                            "x2": x2,
                            "y2": y2,
                        }
                    )

                for tid in list(track_last_seen.keys()):
                    if frame_count - track_last_seen[tid] > 300:
                        counted_ids.discard(tid)
                        track_last_seen.pop(tid, None)

            if on_detections:
                on_detections(
                    {
                        "cctv": cctv_name,
                        "frame_width": frame_w,
                        "frame_height": frame_h,
                        "zone_top": line_y - 2,
                        "zone_bottom": line_y + 2,
                        "up_count": up_count,
                        "down_count": down_count,
                        "count": up_count + down_count,
                        "boxes": payload_boxes,
                        "timestamp": datetime.now().isoformat(),
                    }
                )
    finally:
        flush_batch()

