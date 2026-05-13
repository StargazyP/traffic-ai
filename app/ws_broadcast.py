"""WebSocket payload builders and thread-safe broadcast to event_bus."""

from __future__ import annotations

import asyncio

from event_bus import event_bus


def schedule_broadcast(loop: asyncio.AbstractEventLoop | None, data: dict) -> None:
    """백그라운드 스레드에서 WS 브로드캐스트. run_coroutine_threadsafe 실패 시 coroutine 누수 방지."""
    if loop is None:
        return
    coro = event_bus.broadcast(data)
    try:
        asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        coro.close()


def ws_segment_message(*, cctv: str) -> dict:
    from datetime import datetime

    return {
        "type": "segment",
        "cctv": cctv,
        "timestamp": datetime.now().isoformat(),
    }


def ws_detection_message(payload: dict) -> dict:
    """YOLO 결과만 (bbox·카운트·스케일용 메타). 영상 바이너리·stream_url 없음."""
    return {
        "type": "detection",
        "cctv": payload.get("cctv", ""),
        "boxes": payload.get("boxes") or [],
        "count": payload.get("count", 0),
        "site_count": payload.get("site_count", payload.get("count", 0)),
        "up_count": payload.get("up_count", 0),
        "down_count": payload.get("down_count", 0),
        "main_flow_count": payload.get("main_flow_count", 0),
        "direction_score": payload.get("direction_score", 0.0),
        "duration_sec": payload.get("duration_sec", 0.0),
        "flow_per_sec": payload.get("flow_per_sec", 0.0),
        "time_bucket": payload.get("time_bucket", ""),
        "time_bucket_epoch": payload.get("time_bucket_epoch", 0),
        "bucket_lag_sec": payload.get("bucket_lag_sec", 0.0),
        "is_valid": payload.get("is_valid", True),
        "invalid_reason": payload.get("invalid_reason", ""),
        "prev_main_flow_count": payload.get("prev_main_flow_count"),
        "up_count_hard": payload.get("up_count_hard", 0),
        "down_count_hard": payload.get("down_count_hard", 0),
        "up_count_soft": payload.get("up_count_soft", 0),
        "down_count_soft": payload.get("down_count_soft", 0),
        "frame_width": payload.get("frame_width", 640),
        "frame_height": payload.get("frame_height", 360),
        "frame_id": payload.get("frame_id", 0),
        "capture_ts": payload.get("capture_ts", 0.0),
        "capture_time": payload.get("capture_time", ""),
        "processed_ts": payload.get("processed_ts", 0.0),
        "processed_time": payload.get("processed_time", ""),
        "frame_age_ms": payload.get("frame_age_ms", 0.0),
        "infer_latency_ms": payload.get("infer_latency_ms", 0.0),
        "roi_x0": payload.get("roi_x0", 0),
        "roi_y0": payload.get("roi_y0", 0),
        "roi_x1": payload.get("roi_x1", payload.get("frame_width", 640)),
        "roi_y1": payload.get("roi_y1", payload.get("frame_height", 360)),
        "roi_width": payload.get("roi_width", payload.get("frame_width", 640)),
        "roi_height": payload.get("roi_height", payload.get("frame_height", 360)),
        "line_y": payload.get("line_y", 0),
        "line_y_global": payload.get("line_y_global", payload.get("line_y", 0)),
        "zone_top": payload.get("zone_top", 280),
        "zone_bottom": payload.get("zone_bottom", 320),
        "zone_top_global": payload.get("zone_top_global", payload.get("zone_top", 280)),
        "zone_bottom_global": payload.get("zone_bottom_global", payload.get("zone_bottom", 320)),
        "active_tracks": payload.get("active_tracks", 0),
        "persisted_tracks": payload.get("persisted_tracks", 0),
        "counted_tracks": payload.get("counted_tracks", 0),
        "timestamp": payload.get("timestamp", ""),
        "debug_image": payload.get("debug_image", ""),
    }
