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
    return {"type": "segment", "cctv": cctv}


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
