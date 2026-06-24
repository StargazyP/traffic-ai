if True:
    import asyncio
    import json
    import logging
    import os
    from queue import Empty, Full, Queue
    import subprocess
    import threading
    import time
    from contextlib import asynccontextmanager
    from datetime import datetime
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

    from app.boot_rotation_retry import halt_rotation_boot_retry, spawn_rotation_boot_retry
    from app.hls_proxy import router as hls_proxy_router
    from app.inference_visuals import debug_image_roi_jpeg_b64 as _debug_image_roi_jpeg_b64
    from app.its_client import get_cctv_list

    from event_bus import event_bus
    from db_mysql import run_hourly_compression_loop

    logger = logging.getLogger(__name__)

    # ultralytics → lap 은 pip 설치본 필요. `.vendor/lap` 전체를 path에 넣으면 다른 OS용 바이너리가 없어 깨짐(Docker 로그의 lap._lapjv).
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
    def _ws_segment_message(*, cctv: str, cctvs: list[str] | None = None) -> dict:
        group = [x for x in (cctvs or []) if x]
        if not group and cctv:
            group = [cctv]
        primary = group[0] if group else cctv
        return {
            "type": "segment",
            "cctv": primary,
            "cctvs": group,
            "timestamp": datetime.now().isoformat(),
        }


    def _ws_detection_message(payload: dict) -> dict:
        from app.ws_broadcast import ws_detection_message

        return ws_detection_message(payload)


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
        "frame_id": 0,
        "capture_ts": 0.0,
        "capture_time": "",
        "processed_ts": 0.0,
        "processed_time": "",
        "frame_age_ms": 0.0,
        "infer_latency_ms": 0.0,
        "roi_x0": 0,
        "timestamp": "",
        "cctv": "",
        "line_y_global": 0,
        "active_tracks": 0,
        "persisted_tracks": 0,
        "counted_tracks": 0,
    }
    counter_thread: threading.Thread | None = None
    counter_stop_event: threading.Event | None = None
    rollup_thread: threading.Thread | None = None
    rollup_stop_event: threading.Event | None = None

    # 순차 CCTV 라운드로빈 (메인 프로세스 내 단일 워커 스레드, lifespan 또는 API로 기동·정지)
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
    last_frames: dict[str, np.ndarray] = {}
    last_frames_lock = threading.Lock()
    yolo_threads: list[threading.Thread] = []
    # CCTV별 ByteTracker (워커 간 공유, trackers_lock)
    MODEL_PATH = os.getenv("MODEL_PATH", "models/yolov8n.pt")
    trackers: dict[str, Any] = {}
    trackers_lock = threading.Lock()
    # YOLO_INGEST_URL(공유 스트림) 시 큐에 붙일 현재 로테이션 CCTV 라벨
    rotation_tag_lock = threading.Lock()
    rotation_active_cctv: str = ""
    rotation_active_group: list[str] = []
    rotation_stream_urls: dict[str, str] = {}
    rotation_stream_urls_lock = threading.Lock()

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
    last_infer_at_per_cctv: dict[str, str] = {}
    debug_cache_lock = threading.Lock()
    last_debug_image_per_cctv: dict[str, str] = {}
    last_debug_frame_id_per_cctv: dict[str, int] = {}


    def _telemetry_reset() -> None:
        with rotation_telemetry_lock:
            rotation_telemetry["last_infer_at"] = None
            rotation_telemetry["infer_total"] = 0
            rotation_telemetry["last_cctv_processed"] = ""
            last_infer_at_per_cctv.clear()
        with debug_cache_lock:
            last_debug_image_per_cctv.clear()
            last_debug_frame_id_per_cctv.clear()


    def _telemetry_infer(cctv_name: str) -> None:
        now_iso = datetime.now().isoformat(timespec="seconds")
        with rotation_telemetry_lock:
            rotation_telemetry["last_infer_at"] = now_iso
            rotation_telemetry["infer_total"] = int(rotation_telemetry.get("infer_total") or 0) + 1
            rotation_telemetry["last_cctv_processed"] = cctv_name
            if cctv_name:
                last_infer_at_per_cctv[cctv_name] = now_iso


    def _store_debug_image(cctv_name: str, frame_id: int, b64: str) -> str:
        """워커 스레드 → 전역 ROI JPEG 캐시 (HTTP 폴링용)."""
        with debug_cache_lock:
            if b64:
                last_debug_image_per_cctv[cctv_name] = b64
            elif cctv_name in last_debug_image_per_cctv:
                b64 = last_debug_image_per_cctv[cctv_name]
            last_debug_frame_id_per_cctv[cctv_name] = int(frame_id)
            return b64


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
                args = _make_bytetrack_args()
                try:
                    trackers[cctv_name] = BYTETracker(args, frame_rate=30)
                except TypeError:
                    trackers[cctv_name] = BYTETracker(args)
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


    def _yolo_workers_alive() -> int:
        return sum(1 for t in yolo_threads if t.is_alive())

    def _start_yolo_workers(main_loop: asyncio.AbstractEventLoop | None) -> None:
        global yolo_threads
        from app.yolo_runtime import get_yolo_worker_count

        target = get_yolo_worker_count()
        yolo_threads = [t for t in yolo_threads if t.is_alive()]
        while len(yolo_threads) < target:
            worker_id = len(yolo_threads)
            t = threading.Thread(
                target=yolo_worker,
                args=(main_loop, worker_id, target),
                daemon=True,
                name=f"yolo-worker-{worker_id}",
            )
            t.start()
            yolo_threads.append(t)
        logger.info("YOLO workers started: %s/%s alive", _yolo_workers_alive(), target)

    def _join_yolo_workers(timeout: float = 15.0) -> None:
        global yolo_threads
        for t in list(yolo_threads):
            if t.is_alive():
                t.join(timeout=timeout)
        yolo_threads = []

    def yolo_worker(
        main_loop: asyncio.AbstractEventLoop | None,
        worker_id: int = 0,
        worker_count: int = 1,
    ) -> None:
        """CCTV별 큐 라운드로빈. worker_id로 지점 고정 분할 → N워커 병렬 추론."""
        from app.yolo_runtime import cctv_yolo_worker_slot, load_yolo_model

        worker_model = load_yolo_model(worker_id)
        worker_count = max(1, int(worker_count))
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
                get_camera_config,
                get_yolo_imgsz_for_cctv,
            )
        except ImportError:
            YOLO_TRACK_CONF = 0.4
            DEBUG_IMAGE = os.getenv("DEBUG_IMAGE", "0").strip().lower() in {"1", "true", "yes"}
            try:
                DEBUG_IMAGE_EVERY = max(1, int(os.getenv("DEBUG_IMAGE_EVERY", "5")))
            except ValueError:
                DEBUG_IMAGE_EVERY = 5

            def get_camera_config(_name: str) -> dict:
                return {}

            def get_yolo_imgsz_for_cctv(_name: str) -> int:
                try:
                    return max(320, min(1536, int(os.getenv("YOLO_IMGSZ", "960"))))
                except ValueError:
                    return 960

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
        default_roi_cfg = dict(top=ROI_TOP_RATIO, left=0.0, width=1.0)
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
        try:
            counted_stale_infer_gap = int(
                os.getenv("YOLO_COUNTED_STALE_INFER_GAP", str(max(stale_infer_gap * 6, 300)))
            )
        except Exception:
            counted_stale_infer_gap = max(stale_infer_gap * 6, 300)
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
        counted_last_seen_infer: dict[str, dict[int, int]] = {}
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
                if cctv_yolo_worker_slot(name, worker_count) == worker_id
                and now - last_infer_time_per_cctv.get(name, 0.0) >= SAMPLE_INTERVAL
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

            cam = get_camera_config(cctv_name)
            from app.roi_geometry import compute_line_y, compute_roi_rect, crossing_params_for_camera

            roi_y0, roi_x0, roi_x1, roi_h, _roi_w = compute_roi_rect(
                h, w, cam, default_top=ROI_TOP_RATIO
            )
            roi = frame[roi_y0:h, roi_x0:roi_x1]
            line_y, line_y_global = compute_line_y(
                cam,
                frame_h=h,
                roi_y0=roi_y0,
                roi_h=roi_h,
                default_line_y_ratio=line_y_ratio,
            )
            decision_min_move, decision_soft_min_dy, decision_soft_margin = crossing_params_for_camera(
                cam,
                roi_h=roi_h,
                default_min_move=effective_min_move,
                default_soft_min_dy=flow_soft_min_dy,
                default_soft_margin=line_soft_margin,
            )
            infer_seq_per_cctv[cctv_name] = infer_seq_per_cctv.get(cctv_name, 0) + 1
            infer_n = infer_seq_per_cctv[cctv_name]

            _telemetry_infer(cctv_name)

            yolo_imgsz = get_yolo_imgsz_for_cctv(cctv_name)
            tracker_model = get_tracker(cctv_name)
            infer_started_at = time.time()
            results = worker_model(
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
            counted_seen = counted_last_seen_infer.setdefault(cctv_name, {})
            active_track_ids: set[int] = set()

            for tr in tracks:
                if len(tr) < 5:
                    continue
                x1, y1, x2, y2, tid_raw = tr[:5]
                tid = int(tid_raw)
                if tid < 0:
                    continue
                active_track_ids.add(tid)

                x1i, y1i, x2i, y2i = map(int, [x1, y1, x2, y2])
                x1f, x2f = x1i + roi_x0, x2i + roi_x0
                y1f, y2f = y1i + roi_y0, y2i + roi_y0
                cx = int((x1f + x2f) / 2)
                cy = int((y1f + y2f) / 2)

                anchor_y = int(y2i)
                hist = th_map.setdefault(tid, [])
                hist.append((infer_n, anchor_y))
                if len(hist) > _MAX_TRACK_POSITIONS:
                    hist.pop(0)
                tid_infer[tid] = infer_n
                if tid in counted:
                    counted_seen[tid] = infer_n

                boxes.append(
                    {
                        "track_id": tid,
                        "x1": x1f,
                        "y1": y1f,
                        "x2": x2f,
                        "y2": y2f,
                        "cx": cx,
                        "cy": cy,
                        "width": max(0, x2f - x1f),
                        "height": max(0, y2f - y1f),
                        "roi_x1": x1i,
                        "roi_y1": y1i,
                        "roi_x2": x2i,
                        "roi_y2": y2i,
                        "anchor_y": anchor_y,
                        "history_len": len(hist),
                        "last_seen_frame_id": infer_n,
                        "counted": tid in counted,
                    }
                )

                if tid in counted:
                    continue
                decision = _hybrid_cross_decision(
                    hist,
                    line_y,
                    decision_min_move,
                    soft_enable=hybrid_soft_enable,
                    soft_min_dy=decision_soft_min_dy,
                    soft_margin=decision_soft_margin,
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
                counted_seen[tid] = infer_n
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
                    tid_infer.pop(tid_old, None)
                    th_map.pop(tid_old, None)
            for tid_old in list(counted_seen.keys()):
                if infer_n - counted_seen[tid_old] > counted_stale_infer_gap:
                    counted.discard(tid_old)
                    counted_seen.pop(tid_old, None)

            # 장시간 실행 시 CCTV별 트랙 상태 상한 가드
            if len(tid_infer) > 1000:
                stale_cutoff = infer_n - max(1, stale_infer_gap // 2)
                for tid_old in list(tid_infer.keys()):
                    if tid_infer[tid_old] < stale_cutoff:
                        tid_infer.pop(tid_old, None)
                        th_map.pop(tid_old, None)
            if len(counted_seen) > 2000:
                keep_after = infer_n - max(1, counted_stale_infer_gap // 2)
                for tid_old in list(counted_seen.keys()):
                    if counted_seen[tid_old] < keep_after:
                        counted.discard(tid_old)
                        counted_seen.pop(tid_old, None)

            debug_b64 = ""
            if DEBUG_IMAGE:
                debug_infer_count_per_cctv[cctv_name] = (
                    debug_infer_count_per_cctv.get(cctv_name, 0) + 1
                )
                if debug_infer_count_per_cctv[cctv_name] % DEBUG_IMAGE_EVERY == 0:
                    try:
                        debug_b64 = _debug_image_roi_jpeg_b64(
                            roi,
                            roi_x0=roi_x0,
                            roi_y0=roi_y0,
                            boxes=boxes,
                            line_y=line_y,
                        )
                    except Exception as exc:
                        logger.debug("debug_image: %s", exc)
            debug_b64 = _store_debug_image(cctv_name, infer_n, debug_b64)

            uh = up_hard_per_cctv.get(cctv_name, 0)
            dh = down_hard_per_cctv.get(cctv_name, 0)
            us = up_soft_per_cctv.get(cctv_name, 0)
            ds = down_soft_per_cctv.get(cctv_name, 0)
            u_tot = uh + us
            d_tot = dh + ds
            site_total = u_tot + d_tot
            global_count_per_cctv[cctv_name] = site_total
            processed_ts = time.time()
            capture_time = datetime.fromtimestamp(cap_ts).isoformat() if cap_ts else ""
            processed_time = datetime.fromtimestamp(processed_ts).isoformat()
            frame_age_ms = max(0.0, (processed_ts - cap_ts) * 1000.0) if cap_ts else 0.0
            infer_latency_ms = max(0.0, (processed_ts - infer_started_at) * 1000.0)
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
                "frame_id": infer_n,
                "capture_ts": cap_ts,
                "capture_time": capture_time,
                "processed_ts": processed_ts,
                "processed_time": processed_time,
                "frame_age_ms": frame_age_ms,
                "infer_latency_ms": infer_latency_ms,
                "roi_x0": roi_x0,
                "roi_y0": roi_y0,
                "roi_x1": roi_x1,
                "roi_y1": h,
                "roi_width": max(0, roi_x1 - roi_x0),
                "roi_height": max(0, h - roi_y0),
                "line_y": line_y,
                "line_y_global": line_y_global,
                "zone_top": line_y - 2,
                "zone_bottom": line_y + 2,
                "zone_top_global": roi_y0 + line_y - 2,
                "zone_bottom_global": roi_y0 + line_y + 2,
                "boxes": boxes,
                "active_tracks": len(active_track_ids),
                "persisted_tracks": len(tid_infer),
                "counted_tracks": len(counted),
                "timestamp": processed_time,
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
        global frame_queues_revision, rotation_active_cctv, rotation_active_group
        from yolo_mysql_counter import flush_batch

        try:
            from app.config import (
                CCTV_ROTATION_SEC,
                YOLO_INGEST_URL,
                get_effective_rotation_sites,
                get_rotation_parallel_slots,
                rotation_active_names,
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

        try:
            from app.rotation_sites_cache import save_rotation_sites_cache

            save_rotation_sites_cache(ordered, source="runtime")
        except Exception as exc:
            logger.debug("rotation stream cache save skipped: %s", exc)

        with rotation_stream_urls_lock:
            rotation_stream_urls.clear()
            rotation_stream_urls.update(
                {name: url.strip() for name, url in ordered if (url or "").strip()}
            )

        with frame_queues_lock:
            frame_queues.clear()
            frame_queues_revision += 1
        with last_frames_lock:
            last_frames.clear()

        with trackers_lock:
            trackers.clear()

        with rotation_telemetry_lock:
            rotation_telemetry["ingest_mode"] = "per_site_ffmpeg"
            rotation_telemetry["yolo_ingest_url_set"] = bool(ingest_url)

        parallel_slots = get_rotation_parallel_slots()
        site_names = [n for n, _ in ordered]
        window_start = 0
        n_sites = len(ordered)

        def _set_active_group(start: int) -> list[str]:
            global rotation_active_cctv, rotation_active_group
            group = rotation_active_names(site_names, start, slots=parallel_slots)
            with rotation_tag_lock:
                rotation_active_cctv = group[0] if group else ""
                rotation_active_group = list(group)
            return group

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

        window_start = 0
        dwell_start = time.monotonic()
        last_group: tuple[str, ...] = ()
        current_dwell_sec = float(CCTV_ROTATION_SEC)

        def _broadcast_group(group: list[str]) -> None:
            nonlocal last_group
            key = tuple(group)
            if key == last_group:
                return
            last_group = key
            _schedule_broadcast(
                main_loop,
                _ws_segment_message(cctv=group[0] if group else "", cctvs=group),
            )

        _broadcast_group(_set_active_group(0))

        try:
            while not sequencer_stop.is_set():
                if not ordered:
                    time.sleep(0.5)
                    continue

                if time.monotonic() - dwell_start >= float(current_dwell_sec):
                    window_start = (window_start + parallel_slots) % max(n_sites, 1)
                    dwell_start = time.monotonic()
                    _broadcast_group(_set_active_group(window_start))

                time.sleep(0.05)
        finally:
            flush_batch()


    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """기동 시 로테이션·YOLO 캡처 자동 시작(지점 URL 없으면 스킵). 종료 시 워커 정리."""
        global sequencer_thread, yolo_threads, rollup_thread, rollup_stop_event

        loop = asyncio.get_running_loop()
        app.state.main_loop = loop

        boot = _rotation_start_impl(app)
        if boot.get("ok"):
            logger.info("서버 기동 시 로테이션 자동 시작: %s", boot.get("message"))
        else:
            err = boot.get("error")
            if err == "no_sites":
                logger.warning(
                    "서버 기동 시 로테이션 미시작: 유효한 스트림 URL이 없습니다(.env·ITS API). "
                    "백그라운드에서 재시도합니다(%s 초 간격).",
                    os.getenv("ROTATION_BOOT_RETRY_INTERVAL_SEC", "15"),
                )
                spawn_rotation_boot_retry(app, _rotation_start_impl)
            elif err == "no_loop":
                logger.warning("서버 기동 시 로테이션 미시작: main_loop 없음")
            elif err in {"its_quota_exceeded", "its_unauthorized", "its_forbidden", "its_autofetch_disabled"}:
                logger.warning(
                    "서버 기동 시 로테이션 미시작(%s): ITS 호출이 영구 차단 상태이므로 자동 재시도하지 않습니다. "
                    ".env 에 CCTV_URL/CCTV_URL_* 를 지정하거나, ITS 한도 회복 후 /rotation/start 로 수동 재시작하세요.",
                    err,
                )
            else:
                logger.warning("서버 기동 시 로테이션 미시작: %s", boot)

        compression_enabled = os.getenv("DB_HOURLY_COMPRESSION_ENABLED", "1").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        if compression_enabled:
            interval_seconds = int(os.getenv("DB_HOURLY_COMPRESSION_INTERVAL_SEC", "3600"))
            retention_hours = int(os.getenv("DB_RAW_RETENTION_HOURS", "72"))
            rollup_stop_event = threading.Event()
            rollup_thread = threading.Thread(
                target=run_hourly_compression_loop,
                kwargs={
                    "stop_event": rollup_stop_event,
                    "interval_seconds": interval_seconds,
                    "retention_hours": retention_hours,
                },
                daemon=True,
            )
            rollup_thread.start()
            logger.info(
                "DB 시간 단위 통합(vehicle_count_hourly) 스케줄러 시작(interval=%ss, retention=%sh)",
                interval_seconds,
                retention_hours,
            )
            # Docker 로그에는 print가 바로 보임(logger.info는 기본 레벨에서 누락될 수 있음).
            print(
                f"[DB ROLLUP] 시간 단위 통합 스케줄러 시작 interval={interval_seconds}s retention={retention_hours}h",
                flush=True,
            )

        yield

        halt_rotation_boot_retry()

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
            _join_yolo_workers(timeout=15.0)

        if rollup_stop_event is not None:
            rollup_stop_event.set()
        if rollup_thread is not None and rollup_thread.is_alive():
            rollup_thread.join(timeout=3.0)
        rollup_thread = None
        rollup_stop_event = None


    app = FastAPI(
        title="Traffic AI CCTV Viewer",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.include_router(hls_proxy_router)


    @app.get("/preview-sites")
    def preview_sites():
        """Public site list for the dashboard. Do not expose source stream URLs."""
        from app.config import peek_idle_rotation_diagnose
        from app.rotation_health import build_rotation_health

        sites = _rotation_sites_configured()
        idle = peek_idle_rotation_diagnose() if not sites else {}
        try:
            rotation_health = build_rotation_health(sites=sites, infer_total=0, sequencer_running=False)
        except Exception:
            rotation_health = {"system_ok": bool(sites), "alert": None}
        idle_out = idle or None
        alert = rotation_health.get("alert") if isinstance(rotation_health, dict) else None
        if alert and isinstance(alert, dict):
            idle_out = {
                "code": alert.get("its_error_code") or alert.get("its_api_result_code") or "error",
                "message": alert.get("message") or "",
                "title": alert.get("title") or "",
                "its_api_result_code": alert.get("its_api_result_code") or "",
            }
        return {
            "sites": [{"name": n} for n, _ in sites],
            "default_preview_url": "",
            "effective_site_count": len(sites),
            "idle_reason": idle_out,
            "rotation_health": rotation_health,
        }


    @app.get("/its/health")
    def its_health():
        """ITS OpenAPI 사용 가능 여부 (CCTV_URL 무관)."""
        from app.rotation_health import probe_its_api_health

        return probe_its_api_health()


    @app.get("/favicon.ico")
    def favicon():
        return Response(status_code=204)


    @app.get("/count-status")
    def get_count_status():
        with status_lock:
            ds = detection_status
            boxes = ds.get("boxes") or []
            box_n = len(boxes) if isinstance(boxes, list) else 0
            cctv_live = (str(ds.get("cctv") or "").strip()) or (
                count_status.get("cctv_name") or ""
            )
            return {
                "cctv_name": count_status.get("cctv_name", ""),
                "cctv": cctv_live,
                "count": int(count_status.get("count") or 0),
                "up_count": int(ds.get("up_count") or 0),
                "down_count": int(ds.get("down_count") or 0),
                "site_count": int(ds.get("site_count") or ds.get("count") or 0),
                "up_count_hard": int(ds.get("up_count_hard") or 0),
                "down_count_hard": int(ds.get("down_count_hard") or 0),
                "up_count_soft": int(ds.get("up_count_soft") or 0),
                "down_count_soft": int(ds.get("down_count_soft") or 0),
                "frame_id": int(ds.get("frame_id") or 0),
                "capture_time": ds.get("capture_time", ""),
                "processed_time": ds.get("processed_time", ""),
                "timestamp": ds.get("timestamp", ""),
                "flow_per_sec": float(ds.get("flow_per_sec") or 0.0),
                "direction_score": float(ds.get("direction_score") or 0.0),
                "main_flow_count": float(ds.get("main_flow_count") or 0.0),
                "is_valid": bool(ds.get("is_valid", True)),
                "invalid_reason": str(ds.get("invalid_reason") or ""),
                "roi_x0": int(ds.get("roi_x0") or 0),
                "roi_y0": int(ds.get("roi_y0") or 0),
                "roi_x1": int(ds.get("roi_x1") or 0),
                "roi_y1": int(ds.get("roi_y1") or 0),
                "line_y": int(ds.get("line_y") or 0),
                "line_y_global": int(ds.get("line_y_global") or ds.get("line_y") or 0),
                "active_tracks": int(ds.get("active_tracks") or 0),
                "persisted_tracks": int(ds.get("persisted_tracks") or 0),
                "frame_age_ms": float(ds.get("frame_age_ms") or 0.0),
                "infer_latency_ms": float(ds.get("infer_latency_ms") or 0.0),
                "box_count": box_n,
                "debug_image": str(ds.get("debug_image") or ""),
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

        from app.its_guard import its_autofetch_disabled

        if its_autofetch_disabled():
            raise HTTPException(
                status_code=503,
                detail=(
                    "ITS_AUTOFETCH_DISABLED=1 로 /start-count 가 ITS 목록을 조회하지 않습니다. "
                    "CCTV_URL 로 직접 스트림을 지정하거나 환경변수를 해제하세요."
                ),
            )
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
        """서울 유입 CCTV 순차 캡처·YOLO 워커 기동. HTTP 예외 없이 dict 반환 (lifespan·API 공용).

        sites 가 비어 있는 사유가 ITS 한도 초과/인증 실패 같은 영구 오류면 별도 에러 코드를 반환해
        boot_rotation_retry 가 무한 재시도로 한도를 더 소진하지 않도록 한다.
        """
        global sequencer_thread, yolo_threads

        from app.yolo_runtime import get_yolo_worker_count

        loop = getattr(app.state, "main_loop", None)
        if loop is None:
            return {"ok": False, "error": "no_loop"}

        sites = _rotation_sites_configured()
        if not sites:
            try:
                from app.config import last_rotation_sites_error_code
                code = last_rotation_sites_error_code()
            except Exception:
                code = ""
            if code in {"its_quota_exceeded", "its_unauthorized", "its_forbidden", "its_autofetch_disabled"}:
                return {"ok": False, "error": code}
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
            if _yolo_workers_alive() < get_yolo_worker_count():
                _start_yolo_workers(loop)

        with status_lock:
            ts = datetime.now().isoformat(timespec="seconds")
            count_status["logs"].append(
                f"[{ts}] 서울 유입 CCTV 로테이션 시작 → " + " → ".join(x[0] for x in sites)
            )
            if len(count_status["logs"]) > 50:
                count_status["logs"] = count_status["logs"][-50:]

        try:
            from app.config import invalidate_idle_rotation_diagnose

            invalidate_idle_rotation_diagnose()
        except ImportError:
            pass

        return {"ok": True, "message": "rotation started", "sites": [s[0] for s in sites]}


    @app.post("/rotation/refresh-sites-cache")
    def rotation_refresh_sites_cache(force_its: bool = Query(False)):
        """ITS에서 CCTV URL을 다시 받아 data/rotation_sites_cache.json 에 저장.

        .env 에 CCTV_URL 이 있으면 기본적으로 ITS 호출을 건너뜀(skipped=true).
        한도 회복 후 강제 갱신: POST ...?force_its=true
        """
        from app.config import refresh_rotation_sites_from_its

        result = refresh_rotation_sites_from_its(force=True, force_its=force_its)
        if result.get("skipped"):
            return result
        if not result.get("ok"):
            raise HTTPException(
                status_code=502,
                detail=result.get("error") or "ITS에서 스트림 URL을 가져오지 못했습니다.",
            )
        return result

    @app.post("/rotation/start")
    def rotation_start(request: Request):
        """서울 유입 로테이션 지점 순차 캡처 시작. URL은 환경변수·ITS 조회로 설정."""
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
                    "ITS API(네트워크)로 로테이션 지점 자동 조회가 되도록 서버에서 openapi.its.go.kr 접근을 확인하세요."
                ),
            )
        raise HTTPException(status_code=500, detail=str(res))


    @app.post("/rotation/stop")
    def rotation_stop():
        """서울 유입 CCTV 순차 캡처 정지."""
        global sequencer_thread, yolo_threads

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
            _join_yolo_workers(timeout=15.0)
        sequencer_stop.clear()

        with status_lock:
            ts = datetime.now().isoformat(timespec="seconds")
            count_status["logs"].append(f"[{ts}] 서울 유입 CCTV 로테이션 정지")
            if len(count_status["logs"]) > 50:
                count_status["logs"] = count_status["logs"][-50:]
            detection_status["boxes"] = []
            detection_status["cctv"] = ""

        return {"ok": True, "message": "rotation stopped"}


    @app.get("/rotation/status")
    def rotation_status():
        """로테이션 상태·YOLO 텔레메트리(디버그 화면용)."""
        from app.config import CCTV_ROTATION_SEC, get_rotation_parallel_slots, peek_idle_rotation_diagnose
        from app.rotation_service import rotation_sites_configured

        from app.yolo_runtime import cuda_status, get_yolo_worker_count

        sites = rotation_sites_configured()
        idle = peek_idle_rotation_diagnose() if not sites else {}
        with rotation_telemetry_lock:
            tel = dict(rotation_telemetry)
            tel["last_infer_at_per_cctv"] = dict(last_infer_at_per_cctv)
        with rotation_tag_lock:
            active = (rotation_active_cctv or "").strip()
            active_group = list(rotation_active_group or [])
        with sequencer_lock:
            seq_running = sequencer_thread is not None and sequencer_thread.is_alive()
        yolo_alive = _yolo_workers_alive()
        yolo_running = yolo_alive > 0
        cuda_info = cuda_status()
        with status_lock:
            cnt = int(count_status.get("count") or 0)
            cname = (count_status.get("cctv_name") or "").strip()
        last_processed = (tel.get("last_cctv_processed") or "").strip()
        infer_total = int(tel.get("infer_total") or 0)
        try:
            from app.rotation_health import build_rotation_health

            rotation_health = build_rotation_health(
                sites=sites,
                infer_total=infer_total,
                sequencer_running=seq_running,
            )
        except Exception as exc:
            logger.debug("rotation_health: %s", exc)
            rotation_health = {"system_ok": bool(sites), "alert": None}

        idle_out = idle or None
        alert = rotation_health.get("alert") if isinstance(rotation_health, dict) else None
        if alert and isinstance(alert, dict):
            idle_out = {
                "code": alert.get("its_error_code") or alert.get("its_api_result_code") or "error",
                "message": alert.get("message") or "",
                "title": alert.get("title") or "",
                "its_api_result_code": alert.get("its_api_result_code") or "",
            }

        return {
            "sequencer_running": seq_running,
            "yolo_running": yolo_running,
            "yolo_worker_count": get_yolo_worker_count(),
            "yolo_workers_alive": yolo_alive,
            "cuda_available": cuda_info.get("cuda_available", False),
            "cuda_device": cuda_info.get("cuda_device", ""),
            "rotation_running": seq_running or yolo_running,
            "active_cctv": active,
            "active_cctv_group": active_group,
            "rotation_parallel_slots": get_rotation_parallel_slots(),
            "last_cctv_processed": last_processed,
            "rotation_sites": [n for n, _ in sites],
            "streams_effective_count": len(sites),
            "idle_reason": idle_out,
            "rotation_health": rotation_health,
            "rotation_sec": int(CCTV_ROTATION_SEC),
            "telemetry": tel,
            "last_infer_at_per_cctv": dict(last_infer_at_per_cctv),
            "count_status_summary": {"cctv_name": cname, "count": cnt},
        }


    @app.get("/rotation/debug-image/{cctv_key}")
    def rotation_debug_image(cctv_key: str, frame_id: int | None = None):
        """지점별 ROI 디버그 JPEG (WS와 분리 — frame_id 로 304 유사 스킵)."""
        key = _resolve_camera_key(cctv_key)
        with debug_cache_lock:
            b64 = last_debug_image_per_cctv.get(key, "")
            fid = int(last_debug_frame_id_per_cctv.get(key, 0))
        if frame_id is not None and fid > 0 and fid <= int(frame_id):
            return {"cctv": key, "frame_id": fid, "debug_image": "", "unchanged": True}
        return {"cctv": key, "frame_id": fid, "debug_image": b64, "unchanged": False}


    def _latest_frame_for_cctv(cctv_name: str):
        from app.config import FRAME_HEIGHT, FRAME_WIDTH, get_effective_rotation_sites
        from app.frame_snapshot import grab_single_frame

        with last_frames_lock:
            cached = last_frames.get(cctv_name)
        if cached is not None:
            return cached

        url_by_name = {n: u for n, u in get_effective_rotation_sites()}
        url = url_by_name.get(cctv_name) or ""
        return grab_single_frame(url, width=int(FRAME_WIDTH), height=int(FRAME_HEIGHT))

    def _resolve_camera_key(cctv_key: str) -> str:
        from app.config import list_camera_config_keys

        key = (cctv_key or "").strip()
        keys = list_camera_config_keys()
        if key in keys:
            return key
        for candidate in keys:
            if candidate in key or key in candidate:
                return candidate
        return key

    @app.get("/rotation/camera-config")
    def list_camera_config():
        from app.config import CAMERA_CONFIG, list_camera_config_keys

        keys = list_camera_config_keys()
        return {
            "sites": [{"key": k, "config": dict(CAMERA_CONFIG.get(k) or {})} for k in keys],
        }

    @app.get("/rotation/camera-config/{cctv_key}")
    def get_camera_config_route(cctv_key: str):
        from app.config import get_camera_config

        key = _resolve_camera_key(cctv_key)
        return {"key": key, "config": get_camera_config(key)}

    @app.get("/rotation/camera-config/{cctv_key}/preview")
    def preview_camera_config(
        cctv_key: str,
        top: float | None = None,
        left: float | None = None,
        width: float | None = None,
        line_y_ratio: float | None = None,
        min_move: float | None = None,
        soft_margin_ratio: float | None = None,
    ):
        from app.config import LINE_Y_RATIO, get_camera_config
        from app.roi_calibration import calibration_summary, overlay_roi_preview_jpeg_b64, suggest_line_y_ratio
        from app.roi_geometry import compute_roi_rect

        key = _resolve_camera_key(cctv_key)
        cam_cfg = get_camera_config(key)
        draft = dict(cam_cfg)
        for field, val in (
            ("top", top),
            ("left", left),
            ("width", width),
            ("line_y_ratio", line_y_ratio),
            ("min_move", min_move),
            ("soft_margin_ratio", soft_margin_ratio),
        ):
            if val is not None:
                draft[field] = float(val)

        frame = _latest_frame_for_cctv(key)
        frame_available = frame is not None
        default_top = float(os.getenv("YOLO_ROI_TOP_RATIO", "0.4"))
        suggested = None
        preview_b64 = ""
        if frame is not None:
            h, w = frame.shape[:2]
            roi_y0, roi_x0, roi_x1, roi_h, _ = compute_roi_rect(h, w, draft, default_top=default_top)
            roi_crop = frame[roi_y0:h, roi_x0:roi_x1]
            suggested = suggest_line_y_ratio(roi_crop)
            preview_b64 = overlay_roi_preview_jpeg_b64(
                frame,
                draft,
                default_top=default_top,
                default_line_y_ratio=float(LINE_Y_RATIO),
                suggested_line_y_ratio=suggested,
            )
            summary = calibration_summary(
                key,
                draft,
                frame_h=h,
                default_top=default_top,
                default_line_y_ratio=float(LINE_Y_RATIO),
                suggested_line_y_ratio=suggested,
            )
        else:
            summary = {"cctv": key, "config": draft, "roi": {}}

        return {
            "key": key,
            "frame_available": frame_available,
            "preview_jpeg_b64": preview_b64,
            "suggested_line_y_ratio": suggested,
            "delta_ratio": summary.get("delta_ratio"),
            "roi": summary.get("roi") or {},
            "config": draft,
        }

    @app.post("/rotation/camera-config/{cctv_key}")
    async def save_camera_config_route(cctv_key: str, request: Request):
        from app.config import save_camera_config_override

        key = _resolve_camera_key(cctv_key)
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            return {"ok": False, "message": "JSON body required"}
        allowed = ("top", "left", "width", "line_y_ratio", "min_move", "soft_margin_ratio", "soft_margin")
        patch = {k: float(body[k]) for k in allowed if k in body}
        if not patch:
            return {"ok": False, "message": "no tunable fields in body"}
        merged = save_camera_config_override(key, patch)
        return {"ok": True, "key": key, "config": merged}

    @app.get("/tuning/roi", response_class=HTMLResponse)
    def roi_tuning_page():
        from app import html_pages

        return html_pages.roi_tuning_html()


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
        const PATH_TRAFFIC = location.pathname.match(/^(\/traffic)(?=\\/|$)/);
        const TRAFFIC_BASE = PATH_TRAFFIC ? PATH_TRAFFIC[1] : "";
        const proto = location.protocol === "https:" ? "wss:" : "ws:";
        const ws = new WebSocket(proto + "//" + location.host + TRAFFIC_BASE + "/ws");
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
        from app import html_pages

        return html_pages.index_html()
