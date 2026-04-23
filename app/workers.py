"""YOLO inference worker thread and CCTV sequencer / FFmpeg ingest."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from queue import Empty

import numpy as np

from app import state as st
from app.inference_visuals import debug_image_roi_jpeg_b64
from app.tracking import (
    MAX_TRACK_POSITIONS,
    TrackDetections,
    get_tracker,
    hybrid_cross_decision,
)
from app.ws_broadcast import schedule_broadcast, ws_detection_message, ws_segment_message

logger = logging.getLogger(__name__)

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
        ROI_TOP_RATIO_HANAM = float(os.getenv("YOLO_ROI_TOP_RATIO_HANAM", "0.10"))
    except Exception:
        ROI_TOP_RATIO_HANAM = 0.10
    try:
        ROI_LEFT_RATIO_HANAM = float(os.getenv("YOLO_ROI_LEFT_RATIO_HANAM", "0.02"))
    except Exception:
        ROI_LEFT_RATIO_HANAM = 0.02
    try:
        ROI_WIDTH_RATIO_HANAM = float(os.getenv("YOLO_ROI_WIDTH_RATIO_HANAM", "0.98"))
    except Exception:
        ROI_WIDTH_RATIO_HANAM = 0.98
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
        from app.config import get_line_y_ratio_for_cctv
    except Exception:
        def get_line_y_ratio_for_cctv(name: str) -> float:
            raw = os.getenv("LINE_Y_RATIO", "0.6")
            if "하남" in (name or ""):
                raw = os.getenv("LINE_Y_RATIO_HANAM", raw)
            try:
                val = float(raw)
            except ValueError:
                val = 0.6
            return min(0.95, max(0.05, val))
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

    while not st.sequencer_stop.is_set():
        now = time.time()

        if st.frame_queues_revision != last_cctv_revision:
            with st.frame_queues_lock:
                cctv_list = sorted(st.frame_queues.keys())
            last_cctv_revision = st.frame_queues_revision

        if not cctv_list:
            time.sleep(0.05)
            continue

        ready_cctvs: list[str] = []
        for name in cctv_list:
            q0 = st.frame_queues.get(name)
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

        q = st.frame_queues.get(cctv_name)
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

        line_y_ratio = float(get_line_y_ratio_for_cctv(cctv_name))
        line_y_global = max(2, min(h - 3, int(line_y_ratio * h)))
        line_y = max(2, min((h - roi_y0) - 3, int(line_y_global - roi_y0)))
        is_hanam = "하남" in (cctv_name or "")
        decision_min_move = float(effective_min_move)
        decision_soft_min_dy = float(flow_soft_min_dy)
        decision_soft_margin = int(line_soft_margin)
        if is_hanam:
            # 하남 안정형: 미세 흔들림으로 인한 중복 카운트를 줄이기 위해 판정을 더 엄격하게 적용.
            decision_min_move = max(decision_min_move, 3.0)
            decision_soft_min_dy = max(decision_soft_min_dy, 10.0)
            decision_soft_margin = min(decision_soft_margin, 55)
        infer_seq_per_cctv[cctv_name] = infer_seq_per_cctv.get(cctv_name, 0) + 1
        infer_n = infer_seq_per_cctv[cctv_name]

        st.telemetry_infer(cctv_name)

        yolo_imgsz = get_yolo_imgsz_for_cctv(cctv_name)
        tracker_model = get_tracker(cctv_name)
        results = st.model(
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
        detections = TrackDetections(
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
            if len(hist) > MAX_TRACK_POSITIONS:
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
            decision = hybrid_cross_decision(
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
            site_total = uh + dh + us + ds
            global_count_per_cctv[cctv_name] = site_total
            add_to_batch(cctv_name, uh, dh, us, ds)
            with st.counter_lock:
                st.count_status["cctv_name"] = cctv_name
                st.count_status["count"] = site_total
                st.count_status["logs"].append(
                    f"[{ts}] {tier} {direction} ↑h{uh}↓h{dh} ↑s{us}↓s{ds} "
                    f"SITE={site_total} tid={tid} line_y={line_y} @ {cctv_name}"
                )
                if len(st.count_status["logs"]) > 50:
                    st.count_status["logs"] = st.count_status["logs"][-50:]

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
                    debug_b64 = debug_image_roi_jpeg_b64(
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

        with st.status_lock:
            st.detection_status.update(payload)

        if main_loop is not None:
            schedule_broadcast(main_loop, ws_detection_message(payload))

    try:
        flush_batch()
    except Exception as exc:
        logger.debug("yolo_worker flush_batch: %s", exc)


def sequential_cctv_loop(main_loop: asyncio.AbstractEventLoop) -> None:
    """FFmpeg 스레드는 스트림 유지·큐 적재, 본 루프는 CCTV 로테이션·세그먼트 WS만 처리."""
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

    with st.frame_queues_lock:
        st.frame_queues.clear()
        st.frame_queues_revision += 1

    with st.trackers_lock:
        st.trackers.clear()

    with st.rotation_telemetry_lock:
        st.rotation_telemetry["ingest_mode"] = "per_site_ffmpeg"
        st.rotation_telemetry["yolo_ingest_url_set"] = bool(ingest_url)

    with st.rotation_tag_lock:
        st.rotation_active_cctv = ordered[0][0]

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

        while not st.sequencer_stop.is_set():
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
                while not st.sequencer_stop.is_set():
                    raw = process.stdout.read(frame_size)
                    if not raw or len(raw) != frame_size:
                        break

                    frame = np.frombuffer(raw, dtype=np.uint8).reshape((int(out_h), int(out_w), 3))
                    cap_ts = time.time()
                    q = st.get_or_create_queue(cctv_name)
                    st.put_latest(
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

            if st.sequencer_stop.is_set():
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
        while not st.sequencer_stop.is_set():
            if not ordered:
                time.sleep(0.5)
                continue

            if time.monotonic() - dwell_start >= float(current_dwell_sec):
                cam_idx = (cam_idx + 1) % len(ordered)
                dwell_start = time.monotonic()

            cctv_name, _ = ordered[cam_idx]

            with st.rotation_tag_lock:
                st.rotation_active_cctv = cctv_name

            if cctv_name != last_seg_cctv:
                schedule_broadcast(
                    main_loop,
                    ws_segment_message(cctv=cctv_name),
                )
                last_seg_cctv = cctv_name

            time.sleep(0.05)
    finally:
        flush_batch()
