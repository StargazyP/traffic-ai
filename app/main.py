import asyncio
import base64
import json
import logging
import os
import sys
from queue import Empty, Full, Queue
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from io import BytesIO
from typing import Any

import numpy as np
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse, Response

from app.hls_proxy import router as hls_proxy_router
from app.its_client import get_cctv_list

from event_bus import event_bus

logger = logging.getLogger(__name__)

_VENDOR_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".vendor")
if os.path.isdir(_VENDOR_DIR) and _VENDOR_DIR not in sys.path:
    sys.path.insert(0, _VENDOR_DIR)

from ultralytics import YOLO
from ultralytics.trackers.byte_tracker import BYTETracker


def _schedule_broadcast(loop: asyncio.AbstractEventLoop | None, data: dict) -> None:
    """백그라운드 스레드에서 WS 브로드캐스트. run_coroutine_threadsafe 실패 시 coroutine 누수 방지."""
    if loop is None:
        return
    coro = event_bus.broadcast(data)
    try:
        asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        coro.close()

    #웹소켓으로 메세징 전송 함수
def _ws_segment_message(*, cctv: str) -> dict:
    return {"type": "segment", "cctv": cctv}
    
    
def _ws_detection_message(payload: dict) -> dict:
    """YOLO 결과만 (bbox·카운트·스케일용 메타). 영상 바이너리·stream_url 없음."""
    return {
        "type": "detection",
        "cctv": payload.get("cctv", ""),
        "boxes": payload.get("boxes") or [],
        "count": payload.get("count", 0),
        "site_count": payload.get("site_count", payload.get("count", 0)),
        "up_count": payload.get("up_count", 0),
        "down_count": payload.get("down_count", 0),
        "up_count_hard": payload.get("up_count_hard", 0),
        "down_count_hard": payload.get("down_count_hard", 0),
        "up_count_soft": payload.get("up_count_soft", 0),
        "down_count_soft": payload.get("down_count_soft", 0),
        "frame_width": payload.get("frame_width", 640),
        "frame_height": payload.get("frame_height", 360),
        "roi_y0": payload.get("roi_y0", 0),
        "line_y": payload.get("line_y", 0),
        "zone_top": payload.get("zone_top", 280),
        "zone_bottom": payload.get("zone_bottom", 320),
        "timestamp": payload.get("timestamp", ""),
        "debug_image": payload.get("debug_image", ""),
    }


status_lock = threading.Lock()
counter_lock = threading.Lock()
count_status = {"cctv_name": "", "count": 0, "logs": []}
detection_status = {
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

# 순차 CCTV 라운드로빈 (메인 프로세스 내 단일 워커 스레드, 시작/정지 버튼으로만 기동)
sequencer_thread: threading.Thread | None = None
sequencer_stop = threading.Event()
sequencer_lock = threading.Lock()
_active_segment_stop: list[threading.Event | None] = [None]
_segment_lock = threading.Lock()

# FFmpeg 지점별 → 최신 1프레임만 유지하는 독립 큐 → 단일 YOLO 워커가 라운드로빈 추론
frame_queues: dict[str, Queue] = {}
frame_queues_lock = threading.Lock()
# 새 CCTV 큐 추가·세션 clear 시 증가 → YOLO가 keys() 전체 스캔 없이 목록만 갱신
frame_queues_revision: int = 0
yolo_thread: threading.Thread | None = None
# 전역 YOLO 1개 + CCTV별 ByteTracker 상태
MODEL_PATH = os.getenv("MODEL_PATH", "models/yolov8n.pt")
model = YOLO(MODEL_PATH)
trackers: dict[str, Any] = {}
trackers_lock = threading.Lock()
# YOLO_INGEST_URL(공유 스트림) 시 큐에 붙일 현재 로테이션 CCTV 라벨
rotation_tag_lock = threading.Lock()
rotation_active_cctv: str = ""

# 로테이션 스레드 진단 (프론트 영상과 무관: 서버 OpenCV·YOLO만)
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


def _telemetry_reset() -> None:
    with rotation_telemetry_lock:
        rotation_telemetry["last_infer_at"] = None
        rotation_telemetry["infer_total"] = 0
        rotation_telemetry["last_cctv_processed"] = ""


def _telemetry_infer(cctv_name: str) -> None:
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


def _debug_image_roi_jpeg_b64(
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
    # BGR → RGB (연속 배열로 PIL 호환)
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
        # payload는 전역 y 저장 → ROI 로컬로 변환
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


class _TrackDetections:
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
        return _TrackDetections(self.xyxy[idx], self.conf[idx], self.cls[idx])


def _make_bytetrack_args() -> Any:
    from types import SimpleNamespace

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
    with trackers_lock:
        if cctv_name not in trackers:
            trackers[cctv_name] = BYTETracker(_make_bytetrack_args(), frame_rate=30)
        return trackers[cctv_name]


_MAX_TRACK_POSITIONS = 30


def _hybrid_cross_decision(
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


def yolo_worker(main_loop: asyncio.AbstractEventLoop | None) -> None:
    """CCTV별 큐 라운드로빈 + per-CCTV 샘플 간격, 단일 GPU로 추론."""
    from yolo_mysql_counter import (
        add_to_batch,
        flush_batch,
        vehicle_classes,
    )

    try:
        from app.config import (
            DEBUG_IMAGE,
            DEBUG_IMAGE_EVERY,
            YOLO_TRACK_CONF,
            get_yolo_imgsz_for_cctv,
        )
    except ImportError:
        YOLO_TRACK_CONF = 0.4
        DEBUG_IMAGE = os.getenv("DEBUG_IMAGE", "0").strip().lower() in {"1", "true", "yes"}
        try:
            DEBUG_IMAGE_EVERY = max(1, int(os.getenv("DEBUG_IMAGE_EVERY", "5")))
        except ValueError:
            DEBUG_IMAGE_EVERY = 5

        def get_yolo_imgsz_for_cctv(name: str) -> int:
            try:
                base = max(320, min(1536, int(os.getenv("YOLO_IMGSZ", "960"))))
            except ValueError:
                base = 960
            if "하남" in (name or ""):
                try:
                    return max(320, min(1536, int(os.getenv("YOLO_IMGSZ_HANAM", "1280"))))
                except ValueError:
                    return 1280
            return base

    try:
        SAMPLE_INTERVAL = float(os.getenv("YOLO_SAMPLE_INTERVAL", "0.2"))
    except Exception:
        SAMPLE_INTERVAL = 0.2
    try:
        FRAME_MAX_AGE_SEC = float(os.getenv("YOLO_FRAME_MAX_AGE_SEC", "1.0"))
    except Exception:
        FRAME_MAX_AGE_SEC = 1.0
    try:
        ROI_TOP_RATIO = float(os.getenv("YOLO_ROI_TOP_RATIO", "0.4"))
    except Exception:
        ROI_TOP_RATIO = 0.4
    try:
        ROI_TOP_RATIO_HANAM = float(os.getenv("YOLO_ROI_TOP_RATIO_HANAM", "0.35"))
    except Exception:
        ROI_TOP_RATIO_HANAM = 0.35
    try:
        ROI_LEFT_RATIO_HANAM = float(os.getenv("YOLO_ROI_LEFT_RATIO_HANAM", "0.4"))
    except Exception:
        ROI_LEFT_RATIO_HANAM = 0.4
    try:
        ROI_WIDTH_RATIO_HANAM = float(os.getenv("YOLO_ROI_WIDTH_RATIO_HANAM", "0.4"))
    except Exception:
        ROI_WIDTH_RATIO_HANAM = 0.4
    try:
        ROI_TOP_RATIO_SEOCHANG = float(os.getenv("YOLO_ROI_TOP_RATIO_SEOCHANG", "0.3"))
    except Exception:
        ROI_TOP_RATIO_SEOCHANG = 0.3
    try:
        ROI_LEFT_RATIO_SEOCHANG = float(os.getenv("YOLO_ROI_LEFT_RATIO_SEOCHANG", "0.0"))
    except Exception:
        ROI_LEFT_RATIO_SEOCHANG = 0.0
    try:
        ROI_WIDTH_RATIO_SEOCHANG = float(os.getenv("YOLO_ROI_WIDTH_RATIO_SEOCHANG", "1.0"))
    except Exception:
        ROI_WIDTH_RATIO_SEOCHANG = 1.0
    default_roi_cfg = dict(top=ROI_TOP_RATIO, left=0.0, width=1.0)
    ROI_CONFIG = {
        "하남": dict(
            top=ROI_TOP_RATIO_HANAM,
            left=ROI_LEFT_RATIO_HANAM,
            width=ROI_WIDTH_RATIO_HANAM,
        ),
        "서창": dict(
            top=ROI_TOP_RATIO_SEOCHANG,
            left=ROI_LEFT_RATIO_SEOCHANG,
            width=ROI_WIDTH_RATIO_SEOCHANG,
        ),
        "김포": dict(top=ROI_TOP_RATIO, left=0.0, width=1.0),
    }
    try:
        from app.config import LINE_Y_RATIO as _LINE_Y_RATIO_CFG
    except Exception:
        _LINE_Y_RATIO_CFG = None
    line_y_ratio = float(_LINE_Y_RATIO_CFG) if _LINE_Y_RATIO_CFG is not None else float(
        os.getenv("LINE_Y_RATIO", "0.6")
    )
    line_y_ratio = min(0.95, max(0.05, line_y_ratio))
    try:
        effective_min_move = float(os.getenv("MIN_MOVE", "6"))
    except Exception:
        effective_min_move = 6.0
    try:
        stale_infer_gap = int(os.getenv("YOLO_STALE_INFER_GAP", "50"))
    except Exception:
        stale_infer_gap = 50
    hybrid_soft_enable = os.getenv("HYBRID_SOFT_ENABLE", "1").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    try:
        flow_soft_min_dy = float(os.getenv("FLOW_SOFT_MIN_DY", "8"))
    except Exception:
        flow_soft_min_dy = 0.0
    if flow_soft_min_dy <= 0:
        flow_soft_min_dy = max(float(effective_min_move) * 2.0, 4.0)
    try:
        line_soft_margin = int(os.getenv("LINE_SOFT_MARGIN", "80"))
    except Exception:
        line_soft_margin = 80

    last_infer_time_per_cctv: dict[str, float] = {}

    # 하이브리드: hard=가상선 교차, soft=flow 보정(근선·강한 움직임·방향 일관)
    up_hard_per_cctv: dict[str, int] = {}
    down_hard_per_cctv: dict[str, int] = {}
    up_soft_per_cctv: dict[str, int] = {}
    down_soft_per_cctv: dict[str, int] = {}
    track_history_per_cctv: dict[str, dict[int, list[tuple[int, int]]]] = {}
    counted_ids_per_cctv: dict[str, set[int]] = {}
    track_last_seen_infer: dict[str, dict[int, int]] = {}
    infer_seq_per_cctv: dict[str, int] = {}

    global_count_per_cctv: dict[str, int] = {}

    try:
        track_iou = float(os.getenv("YOLO_TRACK_IOU", "0.3"))
    except Exception:
        track_iou = 0.3

    cctv_list: list[str] = []
    idx = 0
    last_cctv_revision = -1
    debug_infer_count_per_cctv: dict[str, int] = {}

    while not sequencer_stop.is_set():
        now = time.time()

        if frame_queues_revision != last_cctv_revision:
            with frame_queues_lock:
                cctv_list = sorted(frame_queues.keys())
            last_cctv_revision = frame_queues_revision

        if not cctv_list:
            time.sleep(0.05)
            continue

        ready_cctvs: list[str] = []
        for name in cctv_list:
            q0 = frame_queues.get(name)
            if q0 is not None and not q0.empty():
                ready_cctvs.append(name)

        if not ready_cctvs:
            time.sleep(0.01)
            continue

        # 샘플링 주기가 지난 CCTV만 (큐에 프레임이 있어도 간격 전이면 대기 → 불필요한 get 방지)
        eligible = [
            name
            for name in ready_cctvs
            if now - last_infer_time_per_cctv.get(name, 0.0) >= SAMPLE_INTERVAL
        ]
        if not eligible:
            time.sleep(0.01)
            continue

        cctv_name = eligible[idx % len(eligible)]
        idx += 1

        q = frame_queues.get(cctv_name)
        if q is None:
            continue

        try:
            item = q.get(timeout=0.01)
        except Empty:
            continue

        cap_ts = float(item.get("ts") or 0.0)
        if cap_ts and (time.time() - cap_ts > FRAME_MAX_AGE_SEC):
            continue

        last_infer_time_per_cctv[cctv_name] = time.time()

        frame = item["frame"]
        h, w = frame.shape[:2]

        roi_cfg = default_roi_cfg
        for key, cfg in ROI_CONFIG.items():
            if key in (cctv_name or ""):
                roi_cfg = cfg
                break

        roi_top_ratio = float(roi_cfg.get("top", ROI_TOP_RATIO))
        roi_y0 = int(h * roi_top_ratio)
        left_ratio = min(0.95, max(0.0, float(roi_cfg.get("left", 0.0))))
        width_ratio = min(1.0, max(0.2, float(roi_cfg.get("width", 1.0))))
        roi_x0 = int(w * left_ratio)
        roi_w = int(w * width_ratio)
        roi_x1 = min(w, roi_x0 + roi_w)
        roi = frame[roi_y0:h, roi_x0:roi_x1]

        line_y_global = max(2, min(h - 3, int(line_y_ratio * h)))
        line_y = max(2, min((h - roi_y0) - 3, int(line_y_global - roi_y0)))
        infer_seq_per_cctv[cctv_name] = infer_seq_per_cctv.get(cctv_name, 0) + 1
        infer_n = infer_seq_per_cctv[cctv_name]

        _telemetry_infer(cctv_name)

        yolo_imgsz = get_yolo_imgsz_for_cctv(cctv_name)
        tracker_model = get_tracker(cctv_name)
        results = model(
            roi,
            conf=float(YOLO_TRACK_CONF),
            iou=float(track_iou),
            imgsz=yolo_imgsz,
            verbose=False,
        )

        boxes: list[dict] = []
        r0 = results[0] if results else None
        boxes_obj = r0.boxes if r0 is not None else None

        det_xyxy: list[list[float]] = []
        det_conf: list[float] = []
        det_cls: list[float] = []
        if boxes_obj is not None and boxes_obj.xyxy is not None:
            for box, score, cls in zip(
                boxes_obj.xyxy.cpu().numpy(),
                boxes_obj.conf.cpu().numpy(),
                boxes_obj.cls.cpu().numpy(),
            ):
                if int(cls) not in vehicle_classes:
                    continue
                x1, y1, x2, y2 = map(float, box)
                det_xyxy.append([x1, y1, x2, y2])
                det_conf.append(float(score))
                det_cls.append(float(cls))
        detections = _TrackDetections(
            np.asarray(det_xyxy, dtype=np.float32),
            np.asarray(det_conf, dtype=np.float32),
            np.asarray(det_cls, dtype=np.float32),
        )
        tracks = tracker_model.update(detections, img=roi)

        logger.debug(
            "[yolo/tracker] %s roi_shape=%s det=%s tracks=%s",
            cctv_name,
            roi.shape,
            len(detections),
            len(tracks),
        )

        th_map = track_history_per_cctv.setdefault(cctv_name, {})
        counted = counted_ids_per_cctv.setdefault(cctv_name, set())
        tid_infer = track_last_seen_infer.setdefault(cctv_name, {})

        for tr in tracks:
            if len(tr) < 5:
                continue
            x1, y1, x2, y2, tid_raw = tr[:5]
            tid = int(tid_raw)
            if tid < 0:
                continue

            x1i, y1i, x2i, y2i = map(int, [x1, y1, x2, y2])
            x1f, x2f = x1i + roi_x0, x2i + roi_x0
            y1f, y2f = y1i + roi_y0, y2i + roi_y0

            anchor_y = int(y2i)
            hist = th_map.setdefault(tid, [])
            hist.append((infer_n, anchor_y))
            if len(hist) > _MAX_TRACK_POSITIONS:
                hist.pop(0)
            tid_infer[tid] = infer_n

            boxes.append(
                {
                    "track_id": tid,
                    "x1": x1f,
                    "y1": y1f,
                    "x2": x2f,
                    "y2": y2f,
                }
            )

            if tid in counted:
                continue
            decision = _hybrid_cross_decision(
                hist,
                line_y,
                effective_min_move,
                soft_enable=hybrid_soft_enable,
                soft_min_dy=flow_soft_min_dy,
                soft_margin=line_soft_margin,
            )
            if decision is None:
                continue

            uh = up_hard_per_cctv.get(cctv_name, 0)
            dh = down_hard_per_cctv.get(cctv_name, 0)
            us = up_soft_per_cctv.get(cctv_name, 0)
            ds = down_soft_per_cctv.get(cctv_name, 0)
            ts = datetime.now().isoformat(timespec="seconds")
            tier = "hard" if decision.startswith("hard") else "soft"
            direction = "하행" if decision.endswith("down") else "상행"

            if decision == "hard_down":
                down_hard_per_cctv[cctv_name] = dh + 1
                dh += 1
            elif decision == "hard_up":
                up_hard_per_cctv[cctv_name] = uh + 1
                uh += 1
            elif decision == "soft_down":
                down_soft_per_cctv[cctv_name] = ds + 1
                ds += 1
            elif decision == "soft_up":
                up_soft_per_cctv[cctv_name] = us + 1
                us += 1
            else:
                continue

            counted.add(tid)
            site_total = uh + dh + us + ds
            global_count_per_cctv[cctv_name] = site_total
            add_to_batch(cctv_name, uh, dh, us, ds)
            with counter_lock:
                count_status["cctv_name"] = cctv_name
                count_status["count"] = site_total
                count_status["logs"].append(
                    f"[{ts}] {tier} {direction} ↑h{uh}↓h{dh} ↑s{us}↓s{ds} "
                    f"SITE={site_total} tid={tid} line_y={line_y} @ {cctv_name}"
                )
                if len(count_status["logs"]) > 50:
                    count_status["logs"] = count_status["logs"][-50:]

        for tid_old in list(tid_infer.keys()):
            if infer_n - tid_infer[tid_old] > stale_infer_gap:
                counted.discard(tid_old)
                tid_infer.pop(tid_old, None)
                th_map.pop(tid_old, None)

        # 장시간 실행 시 CCTV별 트랙 상태 상한 가드
        if len(tid_infer) > 1000:
            stale_cutoff = infer_n - max(1, stale_infer_gap // 2)
            for tid_old in list(tid_infer.keys()):
                if tid_infer[tid_old] < stale_cutoff:
                    tid_infer.pop(tid_old, None)
                    counted.discard(tid_old)
                    th_map.pop(tid_old, None)

        debug_b64 = ""
        if DEBUG_IMAGE:
            debug_infer_count_per_cctv[cctv_name] = (
                debug_infer_count_per_cctv.get(cctv_name, 0) + 1
            )
            if debug_infer_count_per_cctv[cctv_name] % DEBUG_IMAGE_EVERY == 0:
                try:
                    debug_b64 = _debug_image_roi_jpeg_b64(
                        roi,
                        roi_y0=roi_y0,
                        boxes=boxes,
                    )
                except Exception as exc:
                    logger.debug("debug_image: %s", exc)

        uh = up_hard_per_cctv.get(cctv_name, 0)
        dh = down_hard_per_cctv.get(cctv_name, 0)
        us = up_soft_per_cctv.get(cctv_name, 0)
        ds = down_soft_per_cctv.get(cctv_name, 0)
        u_tot = uh + us
        d_tot = dh + ds
        site_total = u_tot + d_tot
        global_count_per_cctv[cctv_name] = site_total
        payload = {
            "cctv": cctv_name,
            "count": site_total,
            "site_count": site_total,
            "up_count": u_tot,
            "down_count": d_tot,
            "up_count_hard": uh,
            "down_count_hard": dh,
            "up_count_soft": us,
            "down_count_soft": ds,
            "frame_width": w,
            "frame_height": h,
            "roi_x0": roi_x0,
            "roi_y0": roi_y0,
            "line_y": line_y,
            "zone_top": line_y - 2,
            "zone_bottom": line_y + 2,
            "boxes": boxes,
            "timestamp": datetime.now().isoformat(),
            "debug_image": debug_b64,
        }

        with status_lock:
            detection_status.update(payload)

        if main_loop is not None:
            _schedule_broadcast(main_loop, _ws_detection_message(payload))

    try:
        flush_batch()
    except Exception as exc:
        logger.debug("yolo_worker flush_batch: %s", exc)


def _sequential_cctv_loop(main_loop: asyncio.AbstractEventLoop) -> None:
    """FFmpeg 스레드는 스트림 유지·큐 적재, 본 루프는 CCTV 로테이션·세그먼트 WS만 처리."""
    global frame_queues_revision, rotation_active_cctv
    from yolo_mysql_counter import flush_batch

    try:
        from app.config import (
            CCTV_ROTATION_SEC,
            YOLO_INGEST_URL,
            get_effective_rotation_sites,
        )
    except ImportError:
        return

    ordered = get_effective_rotation_sites()
    ingest_url = (YOLO_INGEST_URL or "").strip()
    # 공유 1스트림 제거: YOLO_INGEST_URL만 있을 때는 지점명별로 동일 URL을 복제해 CCTV별 라벨·ffmpeg 유지
    if not ordered and ingest_url:
        try:
            from app.config import CCTV_MULTICAST_SITES

            ordered = [(n, ingest_url) for n, _ in CCTV_MULTICAST_SITES]
        except Exception:
            ordered = []
    if not ordered:
        return

    with frame_queues_lock:
        frame_queues.clear()
        frame_queues_revision += 1

    with trackers_lock:
        trackers.clear()

    with rotation_telemetry_lock:
        rotation_telemetry["ingest_mode"] = "per_site_ffmpeg"
        rotation_telemetry["yolo_ingest_url_set"] = bool(ingest_url)

    with rotation_tag_lock:
        rotation_active_cctv = ordered[0][0]

    try:
        from app.config import FRAME_HEIGHT, FRAME_WIDTH

        dec_w = max(320, int(FRAME_WIDTH))
        dec_h = max(180, int(FRAME_HEIGHT))
    except Exception:
        dec_w, dec_h = 640, 360

    stream_threads: list[threading.Thread] = []

    def ffmpeg_worker(
        cctv_name: str,
        url: str,
        *,
        out_w: int = dec_w,
        out_h: int = dec_h,
    ) -> None:
        """HLS/RTSP → ffmpeg → raw BGR → 해당 CCTV 전용 큐(최신 1프레임)."""
        frame_size = int(out_w) * int(out_h) * 3
        backoff = 0.2

        while not sequencer_stop.is_set():
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
                f"fps=5,scale={int(out_w)}:{int(out_h)}",
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
                while not sequencer_stop.is_set():
                    raw = process.stdout.read(frame_size)
                    if not raw or len(raw) != frame_size:
                        break

                    frame = np.frombuffer(raw, dtype=np.uint8).reshape((int(out_h), int(out_w), 3))
                    cap_ts = time.time()
                    q = get_or_create_queue(cctv_name)
                    put_latest(
                        q,
                        {
                            "cctv": cctv_name,
                            "frame": frame.copy(),
                            "ts": cap_ts,
                        },
                    )
            finally:
                if process is not None:
                    try:
                        if process.poll() is None:
                            process.kill()
                            process.wait(timeout=1)
                    except Exception:
                        pass

            if sequencer_stop.is_set():
                break
            time.sleep(backoff)
            backoff = min(2.0, backoff * 1.5)

    for cctv_name, cctv_url in ordered:
        if not (cctv_url or "").strip():
            continue
        t = threading.Thread(
            target=ffmpeg_worker,
            args=(cctv_name, cctv_url.strip()),
            daemon=True,
        )
        t.start()
        stream_threads.append(t)

    cam_idx = 0
    dwell_start = time.monotonic()
    last_seg_cctv: str | None = None
    current_dwell_sec = float(CCTV_ROTATION_SEC)

    try:
        while not sequencer_stop.is_set():
            if not ordered:
                time.sleep(0.5)
                continue

            if time.monotonic() - dwell_start >= float(current_dwell_sec):
                cam_idx = (cam_idx + 1) % len(ordered)
                dwell_start = time.monotonic()

            cctv_name, _ = ordered[cam_idx]

            with rotation_tag_lock:
                rotation_active_cctv = cctv_name

            if cctv_name != last_seg_cctv:
                _schedule_broadcast(
                    main_loop,
                    _ws_segment_message(cctv=cctv_name),
                )
                last_seg_cctv = cctv_name

            time.sleep(0.05)
    finally:
        flush_batch()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """기동 시 로테이션·YOLO 캡처 자동 시작(지점 URL 없으면 스킵). 종료 시 워커 정리."""
    global sequencer_thread, yolo_thread

    loop = asyncio.get_running_loop()
    app.state.main_loop = loop

    boot = _rotation_start_impl(app)
    if boot.get("ok"):
        logger.info("서버 기동 시 로테이션 자동 시작: %s", boot.get("message"))
    else:
        err = boot.get("error")
        if err == "no_sites":
            logger.warning(
                "서버 기동 시 로테이션 미시작: 유효한 스트림 URL이 없습니다(.env·ITS API)."
            )
        elif err == "no_loop":
            logger.warning("서버 기동 시 로테이션 미시작: main_loop 없음")
        else:
            logger.warning("서버 기동 시 로테이션 미시작: %s", boot)

    yield

    with sequencer_lock:
        sequencer_stop.set()
        with _segment_lock:
            s = _active_segment_stop[0]
        if s is not None:
            s.set()
        try:
            from app.config import CCTV_ROTATION_SEC as _rot_sec
        except ImportError:
            _rot_sec = 30
        if sequencer_thread is not None and sequencer_thread.is_alive():
            sequencer_thread.join(timeout=float(_rot_sec) + 15.0)
        sequencer_thread = None
        if yolo_thread is not None and yolo_thread.is_alive():
            yolo_thread.join(timeout=15.0)
        yolo_thread = None


app = FastAPI(title="Traffic AI CCTV Viewer", lifespan=lifespan)
app.include_router(hls_proxy_router)


@app.get("/preview-sites")
def preview_sites():
    """하단 미리보기 드롭다운용: 로테이션과 동일한 5지점 이름·스트림 URL."""
    sites = _rotation_sites_configured()
    try:
        from app.config import CCTV_URL as _def

        raw = (_def or "").strip()
        if raw == "여기에 m3u8 URL":
            raw = ""
    except Exception:
        raw = ""
    return {
        "sites": [{"name": n, "url": u} for n, u in sites],
        "default_preview_url": raw,
    }


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/cctv-list")
def cctv_list():
    return get_cctv_list()


@app.get("/count-status")
def get_count_status():
    with status_lock:
        return {
            "cctv_name": count_status["cctv_name"],
            "count": count_status["count"],
            "logs": list(count_status["logs"]),
        }


@app.post("/start-count")
def start_count(request: Request, id: int = Query(..., ge=0)):
    try:
        from yolo_mysql_counter import run_counter_stream  # 지연 import
    except ModuleNotFoundError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"counter dependency missing: {exc}. run `pip install -r requirements.txt`",
        )
    global counter_thread, counter_stop_event

    cctvs = get_cctv_list()
    if id >= len(cctvs):
        raise HTTPException(status_code=404, detail="CCTV id not found")
    row = cctvs[id]
    cctv_url = row.get("cctvurl", "")
    cctv_name = row.get("cctvname", f"cctv-{id}")
    if not cctv_url:
        raise HTTPException(status_code=404, detail="CCTV stream url not found")

    with status_lock:
        count_status["cctv_name"] = cctv_name
        count_status["count"] = 0
        count_status["logs"] = []
        detection_status["count"] = 0
        detection_status["boxes"] = []
        detection_status["timestamp"] = ""

    def on_counter_event(evt: dict):
        with status_lock:
            if evt.get("type") == "count":
                count_status["count"] = int(evt.get("count", count_status["count"]))
                direction = str(evt.get("direction", "")).lower()
                direction_kr = "하행" if direction == "down" else "상행" if direction == "up" else "-"
                count_status["logs"].append(
                    f'[{evt.get("timestamp")}] {direction_kr} '
                    f'SITE={evt.get("count", 0)} TOTAL={evt.get("count")} track={evt.get("track_id")}'
                )
            elif evt.get("type") == "db":
                if evt.get("status") == "ok":
                    count_status["logs"].append(
                        f'[{evt.get("timestamp")}] DB 배치 저장 완료'
                    )
                else:
                    count_status["logs"].append(
                        f'[{evt.get("timestamp")}] DB 저장 실패: {evt.get("message")}'
                    )
            if len(count_status["logs"]) > 50:
                count_status["logs"] = count_status["logs"][-50:]

    loop = getattr(request.app.state, "main_loop", None)

    def on_detections(payload: dict):
        with status_lock:
            detection_status.update(payload)
        if loop is not None:
            _schedule_broadcast(loop, _ws_detection_message(payload))

    with counter_lock:
        if counter_stop_event is not None:
            counter_stop_event.set()
        if counter_thread is not None and counter_thread.is_alive():
            counter_thread.join(timeout=1.0)
        counter_stop_event = threading.Event()
        counter_thread = threading.Thread(
            target=run_counter_stream,
            kwargs={
                "cctv_url": cctv_url,
                "cctv_name": cctv_name,
                "on_event": on_counter_event,
                "on_detections": on_detections,
                "stop_event": counter_stop_event,
            },
            daemon=True,
        )
        counter_thread.start()

    return {"ok": True, "message": "counter started", "cctv_name": cctv_name}


@app.post("/stop-count")
def stop_count():
    """선택 CCTV 카운터 스레드 정지(SSE 오버레이·백엔드 추론 중단)."""
    global counter_thread, counter_stop_event

    with counter_lock:
        if counter_stop_event is not None:
            counter_stop_event.set()
        if counter_thread is not None and counter_thread.is_alive():
            counter_thread.join(timeout=5.0)
        counter_thread = None
        counter_stop_event = None

    with status_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        count_status["logs"].append(f"[{ts}] 카운터 정지 (수동)")
        if len(count_status["logs"]) > 50:
            count_status["logs"] = count_status["logs"][-50:]
        detection_status["boxes"] = []

    return {"ok": True, "message": "counter stopped"}


def _rotation_sites_configured() -> list[tuple[str, str]]:
    try:
        from app.config import get_effective_rotation_sites
    except ImportError:
        return []
    return get_effective_rotation_sites()


def _rotation_start_impl(app: FastAPI) -> dict[str, Any]:
    """5지점 순차 캡처·YOLO 워커 기동. HTTP 예외 없이 dict 반환 (lifespan·API 공용)."""
    global sequencer_thread, yolo_thread

    loop = getattr(app.state, "main_loop", None)
    if loop is None:
        return {"ok": False, "error": "no_loop"}

    sites = _rotation_sites_configured()
    if not sites:
        return {"ok": False, "error": "no_sites"}

    with sequencer_lock:
        if sequencer_thread is not None and sequencer_thread.is_alive():
            return {
                "ok": True,
                "message": "already running",
                "sites": [s[0] for s in sites],
            }
        sequencer_stop.clear()
        sequencer_thread = threading.Thread(
            target=_sequential_cctv_loop,
            args=(loop,),
            daemon=True,
            name="cctv-sequencer",
        )
        _telemetry_reset()
        sequencer_thread.start()

        # stop 후 join으로 죽은 스레드는 None 처리되므로, 재시작 시 반드시 새 스레드 생성
        if yolo_thread is None or not yolo_thread.is_alive():
            yolo_thread = threading.Thread(
                target=yolo_worker,
                args=(loop,),
                daemon=True,
                name="yolo-worker",
            )
            yolo_thread.start()

    with status_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        count_status["logs"].append(
            f"[{ts}] 5지점 로테이션 시작 → " + " → ".join(x[0] for x in sites)
        )
        if len(count_status["logs"]) > 50:
            count_status["logs"] = count_status["logs"][-50:]

    return {"ok": True, "message": "rotation started", "sites": [s[0] for s in sites]}


@app.post("/rotation/start")
def rotation_start(request: Request):
    """5지점(판교→하남→서창→김포→광명) 순차 캡처 시작. URL은 환경변수로 설정."""
    res = _rotation_start_impl(request.app)
    if res.get("ok"):
        return res
    err = res.get("error")
    if err == "no_loop":
        raise HTTPException(status_code=503, detail="서버 초기화 중입니다.")
    if err == "no_sites":
        raise HTTPException(
            status_code=400,
            detail=(
                "유효한 스트림 URL이 없습니다. .env 에 CCTV_URL 또는 지점별 URL을 넣거나, "
                "ITS API(네트워크)로 5지점 자동 조회가 되도록 서버에서 openapi.its.go.kr 접근을 확인하세요."
            ),
        )
    raise HTTPException(status_code=500, detail=str(res))


@app.post("/rotation/stop")
def rotation_stop():
    """5지점 순차 캡처 정지."""
    global sequencer_thread, yolo_thread

    with sequencer_lock:
        sequencer_stop.set()
        with _segment_lock:
            s = _active_segment_stop[0]
        if s is not None:
            s.set()
        th = sequencer_thread
        if th is not None and th.is_alive():
            try:
                from app.config import CCTV_ROTATION_SEC as _rot_sec
            except ImportError:
                _rot_sec = 30
            th.join(timeout=float(_rot_sec) + 15.0)
        sequencer_thread = None
        if yolo_thread is not None and yolo_thread.is_alive():
            yolo_thread.join(timeout=15.0)
        yolo_thread = None
    sequencer_stop.clear()

    with status_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        count_status["logs"].append(f"[{ts}] 5지점 로테이션 정지")
        if len(count_status["logs"]) > 50:
            count_status["logs"] = count_status["logs"][-50:]
        detection_status["boxes"] = []
        detection_status["cctv"] = ""

    return {"ok": True, "message": "rotation stopped"}


@app.get("/rotation/status")
def rotation_status():
    """브라우저 CCTV와 무관하게, 서버 YOLO가 프레임을 받고 추론 중인지 확인."""
    with rotation_telemetry_lock:
        tel = dict(rotation_telemetry)
    with sequencer_lock:
        running = sequencer_thread is not None and sequencer_thread.is_alive()
    with status_lock:
        cnt = int(count_status.get("count") or 0)
        cname = count_status.get("cctv_name") or ""
    return {
        "sequencer_running": running,
        "telemetry": tel,
        "count_status_summary": {"cctv_name": cname, "count": cnt},
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """검출·세그먼트 전환만 push (영상 스트림 없음). 클라이언트는 ping으로 연결 유지 권장."""
    await event_bus.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        event_bus.disconnect(ws)


@app.get("/live", response_class=HTMLResponse)
def traffic_live_page():
    return """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>Traffic AI</title>
</head>
<body>
  <h2>실시간 차량 카운트 (WebSocket)</h2>
  <div id="data"></div>
  <script>
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(proto + "//" + location.host + "/ws");
    ws.onopen = () => {
      setInterval(() => {
        try {
          if (ws.readyState === WebSocket.OPEN) ws.send("ping");
        } catch (_) {}
      }, 20000);
    };
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === "segment") {
        document.getElementById("data").innerHTML =
          "<p>지점 전환: " + (data.cctv ?? "") + "</p>";
        return;
      }
      if (data.type !== "detection") return;
      document.getElementById("data").innerHTML =
        "<p>CCTV: " + (data.cctv ?? "") + "<br>" +
        "Count: " + (data.count ?? 0) + "<br>" +
        "시간: " + (data.timestamp ?? "") + "</p>";
    };
    ws.onerror = () => {
      document.getElementById("data").textContent = "WebSocket 연결 오류";
    };
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    html = """
    <html>
    <head><title>Traffic AI — 5지점 로테이션</title></head>
    <body>
      <h2>5지점 순차 캡처 (YOLO)</h2>
      <p style="color:#555;max-width:720px;">
        상단 영상은 <strong>하단에서 지점 선택 후 [보기]</strong>로 재생합니다. KT/ITS m3u8은 동일 출처 <code>/hls</code> 프록시를 거칩니다. YOLO·카운트는 서버 OpenCV·<code>WebSocket</code>과 별개입니다.
      </p>
      <details style="max-width:920px;margin:12px 0 20px;padding:14px 16px;border:1px solid #ccc;border-radius:8px;background:#f8f9fa;">
        <summary style="cursor:pointer;font-weight:600;color:#222;">서버 캡처·YOLO 파이프라인 (현재 아키텍처 상세)</summary>
        <div style="margin-top:14px;font-size:13px;line-height:1.65;color:#333;max-width:900px;">
          <p><strong>1) 로테이션 시작 시</strong><br/>
          지점 목록(<code>get_effective_rotation_sites</code>)마다 <strong>FFmpeg 전용 스레드</strong>를 띄웁니다. CCTV별 <code>frame_queues</code>·<code>trackers</code>(YOLO 인스턴스)는 세션마다 비운 뒤 다시 채워집니다. 별도 <strong>시퀀서 스레드</strong>는 <code>CCTV_ROTATION_SEC</code>마다 현재 지점만 바며 <code>type: segment</code> WebSocket만 보냅니다(브라우저 HLS 미리보기와 무관).</p>
          <p><strong>2) FFmpeg 인입 (지점당 1스레드)</strong><br/>
          HLS/RTSP URL을 읽어 <code>rawvideo bgr24</code>로 디코드합니다. 필터: <code>fps=5,scale=FRAME_WIDTH×FRAME_HEIGHT</code>(<code>app.config</code>). 끊기면 지수 백오프 후 프로세스 재시작합니다.</p>
          <p><strong>3) CCTV별 프레임 큐 (짧은 버퍼)</strong><br/>
          각 지점 이름으로 <code>Queue(maxsize=YOLO_FRAME_QUEUE_SIZE)</code>(기본 5)를 두고, 디코드된 프레임·캡처 시각 <code>ts</code>·CCTV명을 넣습니다. 큐가 가득 차면 가장 오래된 프레임을 제거해 최신 프레임을 유지합니다.</p>
          <p><strong>4) 단일 <code>yolo_worker</code> 스레드 (GPU 1개 공유)</strong><br/>
          • <code>frame_queues</code> 키 목록은 리비전이 바뀔 때만 갱신합니다.<br/>
          • 큐에 프레임이 있는 CCTV만 <code>ready_cctvs</code>로 모읍니다. 비어 있으면 짧게 sleep합니다.<br/>
          • <code>YOLO_SAMPLE_INTERVAL</code>이 지난 CCTV만 <code>eligible</code>로 남깁니다(지점별 샘플링, 빈 <code>get(timeout)</code> 낭비 감소).<br/>
          • 라운드로빈으로 한 지점을 고른 뒤 큐에서 프레임 1개를 꺼내 순차 처리합니다(중간 프레임 보존).<br/>
          • <code>YOLO_FRAME_MAX_AGE_SEC</code>보다 오래된 <code>ts</code>면 추론을 건너뜁니다.</p>
          <p><strong>5) ROI → YOLO 추론</strong><br/>
          전체 프레임 높이 <code>h</code>에서 <code>roi_y0 = int(h × YOLO_ROI_TOP_RATIO)</code>를 기본으로 사용하고, CCTV 이름에 「하남」이 포함되면 <code>YOLO_ROI_TOP_RATIO_HANAM</code>(기본 0.2)을 적용합니다. 가상선은 <code>line_y_global - roi_y0</code>로 ROI 좌표계에 맞춰 계산합니다.</p>
          <p><strong>6) YOLO detect + CCTV별 ByteTrack</strong><br/>
          YOLO는 전역 단일 모델(<code>model = YOLO(MODEL_PATH)</code>)로 detect만 수행하고, 지점별 상태는 <code>get_tracker(cctv_name)</code>의 ByteTrack 인스턴스가 유지합니다. 검출 결과(<code>xyxy/conf/cls</code>)를 트래커 입력으로 변환해 <code>tracker.update(...)</code>를 호출하며, 트랙 id는 ByteTrack이 생성합니다. <code>imgsz</code>는 <code>YOLO_IMGSZ</code>(기본 960), 「하남」은 <code>YOLO_IMGSZ_HANAM</code>(기본 1280)을 사용합니다.</p>
          <p><strong>7) 검출/트랙 처리</strong><br/>
          <code>boxes_obj</code>가 없거나 <code>xyxy</code>가 없으면 빈 검출로 트래커를 업데이트해 상태를 유지합니다. 검출이 있으면 차량 클래스(<code>vehicle_classes</code>)만 필터링해 ByteTrack 결과를 기준으로 카운트 루프를 진행합니다.</p>
          <p><strong>8) 카운트 규칙 (하이브리드: Line-cross + Flow)</strong><br/>
          트랙별로 <code>(infer_seq, bbox 하단 y)</code> 시계열을 최대 30개 유지합니다. <strong>Primary(hard)</strong>: 이전·현재 하단이 ROI 기준 가상선(<code>LINE_Y_RATIO</code>)을 교차하고 <code>|dy|≥MIN_MOVE</code>이면 상행/하행 hard로 집계합니다. <strong>Secondary(soft)</strong>: 연속 프레임에서 가상선 교차가 없어도, <code>|dy|≥FLOW_SOFT_MIN_DY</code>·가상선 근접(<code>LINE_SOFT_MARGIN</code>)·(3프레임 이상일 때) 방향 반전 없음이면 soft로 집계합니다(<code>HYBRID_SOFT_ENABLE</code>). 동일 <code>track_id</code>는 한 번 카운트되면 stale까지 재사용하지 않습니다. DB에는 <code>up_count_hard/down_count_hard/up_count_soft/down_count_soft</code> 및 합계 <code>up_count/down_count</code>가 저장됩니다.</p>
          <p><strong>9) WebSocket <code>type: detection</code></strong><br/>
          카운트·박스·프레임 메타·타임스탬프를 보냅니다. <code>DEBUG_IMAGE=1</code>이면 N회마다(<code>DEBUG_IMAGE_EVERY</code>) YOLO 입력 <code>roi</code>에 박스를 그린 JPEG를 base64로 넣습니다(전체 원본 프레임은 미포함).</p>
          <p style="font-size:12px;color:#666;margin-bottom:0;">주요 환경변수 예: <code>YOLO_FRAME_QUEUE_SIZE</code>, <code>HYBRID_SOFT_ENABLE</code>, <code>FLOW_SOFT_MIN_DY</code>, <code>LINE_SOFT_MARGIN</code>, <code>LINE_Y_RATIO</code>, <code>MIN_MOVE</code>, <code>YOLO_STALE_INFER_GAP</code>, <code>YOLO_SAMPLE_INTERVAL</code>, <code>YOLO_FRAME_MAX_AGE_SEC</code>, <code>YOLO_ROI_TOP_RATIO</code>, <code>YOLO_ROI_TOP_RATIO_HANAM</code>, <code>YOLO_IMGSZ</code>, <code>YOLO_IMGSZ_HANAM</code>, <code>FRAME_WIDTH</code>/<code>FRAME_HEIGHT</code>, <code>DEBUG_IMAGE</code>, <code>DEBUG_IMAGE_EVERY</code>, <code>MODEL_PATH</code>, <code>CCTV_ROTATION_SEC</code>.</p>
        </div>
      </details>
      <button type="button" id="btnStart" onclick="startRotation()">시작</button>
      <button type="button" id="btnStop" onclick="stopRotation()" disabled>정지</button>
      <span id="runStatus" style="margin-left:12px;color:#444;">대기 중</span>
      <br><br>
      <div style="margin-top:8px;">
        <div style="font-size:12px;color:#666;margin-bottom:6px;">
          YOLO 디버그(5대): 모델 입력 roi 크롭 + 박스. <code>DEBUG_IMAGE=1</code>
        </div>
        <div id="debugGrid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px;max-width:1360px;">
          <div class="dbg-card" data-cctv="판교분기점" style="border:1px solid #ccc;padding:6px;background:#f9f9f9;">
            <div style="font-size:12px;font-weight:600;margin-bottom:4px;">판교분기점</div>
            <img id="debugImage-판교분기점" alt="debug 판교분기점" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;" />
            <div id="debugMeta-판교분기점" style="font-size:11px;color:#555;margin-top:4px;">대기중</div>
          </div>
          <div class="dbg-card" data-cctv="하남분기점" style="border:1px solid #ccc;padding:6px;background:#f9f9f9;">
            <div style="font-size:12px;font-weight:600;margin-bottom:4px;">하남분기점</div>
            <img id="debugImage-하남분기점" alt="debug 하남분기점" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;" />
            <div id="debugMeta-하남분기점" style="font-size:11px;color:#555;margin-top:4px;">대기중</div>
          </div>
          <div class="dbg-card" data-cctv="서창분기점" style="border:1px solid #ccc;padding:6px;background:#f9f9f9;">
            <div style="font-size:12px;font-weight:600;margin-bottom:4px;">서창분기점</div>
            <img id="debugImage-서창분기점" alt="debug 서창분기점" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;" />
            <div id="debugMeta-서창분기점" style="font-size:11px;color:#555;margin-top:4px;">대기중</div>
          </div>
          <div class="dbg-card" data-cctv="김포" style="border:1px solid #ccc;padding:6px;background:#f9f9f9;">
            <div style="font-size:12px;font-weight:600;margin-bottom:4px;">김포</div>
            <img id="debugImage-김포" alt="debug 김포" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;" />
            <div id="debugMeta-김포" style="font-size:11px;color:#555;margin-top:4px;">대기중</div>
          </div>
          <div class="dbg-card" data-cctv="광명" style="border:1px solid #ccc;padding:6px;background:#f9f9f9;">
            <div style="font-size:12px;font-weight:600;margin-bottom:4px;">광명</div>
            <img id="debugImage-광명" alt="debug 광명" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;" />
            <div id="debugMeta-광명" style="font-size:11px;color:#555;margin-top:4px;">대기중</div>
          </div>
        </div>
      </div>
      <pre id="yoloTelemetry" style="font-size:12px;color:#444;max-width:720px;white-space:pre-wrap;border-left:3px solid #ccc;padding-left:8px;">서버 YOLO: 로테이션 시작 후 여기에 추론 여부가 표시됩니다 (브라우저 영상과 무관).</pre>
      <h3>실시간 카운트: <span id="countValue">0</span> <small id="currentCctv" style="color:#666;"></small></h3>
      <div id="debugBox" style="
        position:fixed;
        right:10px;
        bottom:10px;
        background:rgba(0,0,0,0.7);
        color:#0f0;
        padding:10px;
        font-size:12px;
        font-family:monospace;
        z-index:9999;
      ">YOLO: 대기중</div>
      <div>
        <h4>저장 로그</h4>
        <pre id="logBox" style="height:220px; overflow:auto; border:1px solid #ccc; padding:8px;"></pre>
      </div>
      <script>
        let wsInfer = null;
        let wsPingTimer = null;
        let isRunning = false;
        const debug = document.getElementById("debugBox");

        function updateDebugCard(msg) {
          const cctv = msg.cctv || "";
          if (!cctv) return;
          const img = document.getElementById("debugImage-" + cctv);
          const meta = document.getElementById("debugMeta-" + cctv);
          if (img && msg.debug_image) {
            img.src = "data:image/jpeg;base64," + msg.debug_image;
          }
          if (meta) {
            meta.textContent =
              "count=" + (msg.site_count ?? 0) +
              " | roi_y0=" + (msg.roi_y0 ?? 0) +
              " | line_y=" + (msg.line_y ?? 0) +
              " | t=" + (msg.timestamp || "");
          }
        }

        function setRunUi(running) {
          isRunning = running;
          document.getElementById("btnStart").disabled = running;
          document.getElementById("btnStop").disabled = !running;
          document.getElementById("runStatus").textContent = running ? "실행 중" : "대기 중";
        }

        let statusTimer = null;
        function updateYoloTelemetry() {
          const el = document.getElementById("yoloTelemetry");
          if (!el) return;
          fetch("/rotation/status")
            .then(r => r.json())
            .then((d) => {
              const t = d.telemetry || {};
              const seq = d.sequencer_running ? "실행 중" : "대기";
              const inf = t.infer_total ?? 0;
              const last = t.last_infer_at || "—";
              const mode = t.ingest_mode || "—";
              const yset = t.yolo_ingest_url_set ? "YOLO_INGEST_URL 설정됨(단일 URL 폴백 가능)" : "YOLO_INGEST_URL 없음(지점별 URL만 사용)";
              el.textContent = [
                "서버 YOLO (프론트 CCTV와 무관): 시퀀서 " + seq + " | " + yset,
                "모드: " + mode + " | 추론 누적 " + inf + "회 | 마지막 추론 시각 " + last,
                "마지막 처리 지점: " + (t.last_cctv_processed || "—"),
                t.line_cross_note || "",
              ].join("\\n");
            })
            .catch(() => {
              el.textContent = "/rotation/status 조회 실패 (서버 확인)";
            });
        }
        function startStatusPolling() {
          if (statusTimer) clearInterval(statusTimer);
          const update = () => {
            fetch("/count-status")
              .then(r => r.json())
              .then(s => {
                document.getElementById("countValue").textContent = s.count ?? 0;
                const name = s.cctv_name || "";
                document.getElementById("currentCctv").textContent =
                  name ? "(" + name + ")" : "";
                const logs = Array.isArray(s.logs) ? s.logs : [];
                document.getElementById("logBox").textContent = logs.join("\\n");
              })
              .catch(() => null);
            updateYoloTelemetry();
          };
          update();
          statusTimer = setInterval(update, 1000);
        }

        function startDetectionWebSocket() {
          if (wsInfer && wsInfer.readyState <= 1) {
            wsInfer.close();
          }
          if (wsPingTimer) {
            clearInterval(wsPingTimer);
            wsPingTimer = null;
          }
          const proto = location.protocol === "https:" ? "wss:" : "ws:";
          wsInfer = new WebSocket(proto + "//" + location.host + "/ws");
          wsInfer.onmessage = (evt) => {
            try {
              const msg = JSON.parse(evt.data);
              if (msg.type === "segment") {
                const cctvEl = document.getElementById("currentCctv");
                if (cctvEl && msg.cctv)
                  cctvEl.textContent = "(" + msg.cctv + ")";
              } else if (msg.type === "detection") {
                // 🔥 화면 표시만 (디버깅용)
                const el = document.getElementById("countValue");
                if (el && typeof msg.count === "number")
                  el.textContent = msg.count;
                const cctvEl = document.getElementById("currentCctv");
                if (cctvEl && msg.cctv)
                  cctvEl.textContent = "(" + msg.cctv + ")";
                updateDebugCard(msg);

                // 👉 디버깅용 로그
                const tracks = (msg.boxes || []).map((b) => ({
                  id: b.track_id,
                  cx: b.cx,
                  cy: b.cy
                }));
                console.log("YOLO:", {
                  cctv: msg.cctv,
                  count: msg.count,
                  site: msg.site_count,
                  roi_y0: msg.roi_y0,
                  line_y: msg.line_y,
                  boxes: (msg.boxes || []).length,
                  tracks: tracks,
                });

                // 🔥 화면 디버그 표시
                if (debug) {
                  const trackIds = tracks.map(t => t.id).join(",");
                  debug.innerHTML =
                    "CCTV: " + (msg.cctv || "") + "<br>" +
                    "COUNT: " + (msg.count ?? 0) + "<br>" +
                    "SITE COUNT: " + (msg.site_count ?? 0) + "<br>" +
                    "ROI Y0: " + (msg.roi_y0 ?? 0) + "<br>" +
                    "LINE Y: " + (msg.line_y ?? 0) + "<br>" +
                    "BOXES: " + ((msg.boxes && msg.boxes.length) ? msg.boxes.length : 0) + "<br>" +
                    "TRACKS: " + trackIds + "<br>" +
                    "TIME: " + (msg.timestamp || "");
                }
              }
            } catch (_) {}
          };
          wsInfer.onopen = () => {
            wsPingTimer = setInterval(() => {
              try {
                if (wsInfer && wsInfer.readyState === WebSocket.OPEN) wsInfer.send("ping");
              } catch (_) {}
            }, 20000);
          };
        }

        function startRotation() {
          if (isRunning) return;
          fetch("/rotation/start", { method: "POST" })
            .then(res => {
              if (res.status === 400) {
                return res.json().then(j => {
                  const d = j.detail;
                  throw new Error(typeof d === "string" ? d : JSON.stringify(d));
                });
              }
              if (!res.ok) throw new Error("start_failed");
              return res.json();
            })
            .then(() => {
              startStatusPolling();
              startDetectionWebSocket();
              setRunUi(true);
            })
            .catch(e => alert("로테이션 시작 실패: " + (e.message || e)));
        }

        function stopRotation() {
          if (!isRunning) return;
          fetch("/rotation/stop", { method: "POST" })
            .then(res => {
              if (!res.ok) throw new Error("stop_failed");
              if (wsPingTimer) {
                clearInterval(wsPingTimer);
                wsPingTimer = null;
              }
              if (wsInfer) {
                try {
                  wsInfer.close();
                } catch (_) {}
                wsInfer = null;
              }
              if (statusTimer) {
                clearInterval(statusTimer);
                statusTimer = null;
              }
              setRunUi(false);
            })
            .catch(() => alert("정지 요청 실패"));
        }

        function initFromServer() {
          // 서버가 이미 로테이션 중이면, 프론트도 즉시 "실행 중"으로 동기화
          fetch("/rotation/status")
            .then(r => r.json())
            .then((d) => {
              const running = !!d.sequencer_running;
              setRunUi(running);
              startStatusPolling();
              startDetectionWebSocket();
            })
            .catch(() => {
              // 서버 상태를 못 읽어도, WS/폴링은 붙여서 사용자가 진단할 수 있게 둔다
              startStatusPolling();
              startDetectionWebSocket();
            });
        }

        // 최초 진입 시 서버 상태 반영
        initFromServer();
      </script>
    </body>
    </html>
    """
    return html
