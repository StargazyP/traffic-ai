"""HTTP API, WebSocket, and thin HTML handlers."""
# [2026-04-28] rotation/start 설명을 서울 유입 핵심 CCTV 로테이션 기준으로 갱신.

from __future__ import annotations

import threading
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response

from app import html_pages
from app import state as st
from app.its_guard import its_autofetch_disabled
from app.its_client import get_cctv_list
from app.rotation_service import (
    rotation_sites_configured,
    rotation_start_impl,
    rotation_status_payload,
)
from app.ws_broadcast import schedule_broadcast, ws_detection_message
from event_bus import event_bus

router = APIRouter()



@router.get("/preview-sites")
def preview_sites():
    """Public site list for the dashboard. Do not expose source stream URLs."""
    from app.config import peek_idle_rotation_diagnose
    from app.rotation_health import build_rotation_health

    sites = rotation_sites_configured()
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


@router.get("/its/health")
def its_health():
    from app.rotation_health import probe_its_api_health

    return probe_its_api_health()


@router.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@router.get("/count-status")
def get_count_status():
    with st.status_lock:
        return {
            "cctv_name": st.count_status["cctv_name"],
            "count": st.count_status["count"],
            "up_count": st.count_status.get(
                "up_count",
                st.detection_status.get("up_count", 0),
            ),
            "down_count": st.count_status.get(
                "down_count",
                st.detection_status.get("down_count", 0),
            ),
        }


@router.post("/start-count")
def start_count(request: Request, id: int = Query(..., ge=0)):
    try:
        from yolo_mysql_counter import run_counter_stream  # 지연 import
    except ModuleNotFoundError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"counter dependency missing: {exc}. run `pip install -r requirements.txt`",
        )
    
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

    with st.status_lock:
        st.count_status["cctv_name"] = cctv_name
        st.count_status["count"] = 0
        st.count_status["up_count"] = 0
        st.count_status["down_count"] = 0
        st.count_status["main_flow_count"] = 0
        st.count_status["direction_score"] = 0.0
        st.count_status["duration_sec"] = 0.0
        st.count_status["flow_per_sec"] = 0.0
        st.count_status["time_bucket"] = ""
        st.count_status["bucket_lag_sec"] = 0.0
        st.count_status["is_valid"] = True
        st.count_status["invalid_reason"] = ""
        st.count_status["logs"] = []
        st.detection_status["count"] = 0
        st.detection_status["up_count"] = 0
        st.detection_status["down_count"] = 0
        st.detection_status["main_flow_count"] = 0
        st.detection_status["direction_score"] = 0.0
        st.detection_status["duration_sec"] = 0.0
        st.detection_status["flow_per_sec"] = 0.0
        st.detection_status["time_bucket"] = ""
        st.detection_status["bucket_lag_sec"] = 0.0
        st.detection_status["is_valid"] = True
        st.detection_status["invalid_reason"] = ""
        st.detection_status["boxes"] = []
        st.detection_status["timestamp"] = ""

    def on_counter_event(evt: dict):
        with st.status_lock:
            if evt.get("type") == "count":
                st.count_status["count"] = int(evt.get("count", st.count_status["count"]))
                st.count_status["up_count"] = int(
                    evt.get("up_count", st.count_status.get("up_count", 0)),
                )
                st.count_status["down_count"] = int(
                    evt.get("down_count", st.count_status.get("down_count", 0)),
                )
                direction = str(evt.get("direction", "")).lower()
                direction_kr = "하행" if direction == "down" else "상행" if direction == "up" else "-"
                st.count_status["logs"].append(
                    f'[{evt.get("timestamp")}] {direction_kr} '
                    f'SITE={evt.get("count", 0)} TOTAL={evt.get("count")} track={evt.get("track_id")}'
                )
            elif evt.get("type") == "db":
                if evt.get("status") == "ok":
                    st.count_status["logs"].append(
                        f'[{evt.get("timestamp")}] DB 배치 저장 완료'
                    )
                else:
                    st.count_status["logs"].append(
                        f'[{evt.get("timestamp")}] DB 저장 실패: {evt.get("message")}'
                    )
            if len(st.count_status["logs"]) > 50:
                st.count_status["logs"] = st.count_status["logs"][-50:]

    loop = getattr(request.app.state, "main_loop", None)

    def on_detections(payload: dict):
        with st.status_lock:
            st.detection_status.update(payload)
        if loop is not None:
            schedule_broadcast(loop, ws_detection_message(payload))

    with st.counter_lock:
        if st.counter_stop_event is not None:
            st.counter_stop_event.set()
        if st.counter_thread is not None and st.counter_thread.is_alive():
            st.counter_thread.join(timeout=1.0)
        st.counter_stop_event = threading.Event()
        st.counter_thread = threading.Thread(
            target=run_counter_stream,
            kwargs={
                "cctv_url": cctv_url,
                "cctv_name": cctv_name,
                "on_event": on_counter_event,
                "on_detections": on_detections,
                "stop_event": st.counter_stop_event,
            },
            daemon=True,
        )
        st.counter_thread.start()

    return {"ok": True, "message": "counter started", "cctv_name": cctv_name}


@router.post("/stop-count")
def stop_count():
    """선택 CCTV 카운터 스레드 정지(SSE 오버레이·백엔드 추론 중단)."""
    
    with st.counter_lock:
        if st.counter_stop_event is not None:
            st.counter_stop_event.set()
        if st.counter_thread is not None and st.counter_thread.is_alive():
            st.counter_thread.join(timeout=5.0)
        st.counter_thread = None
        st.counter_stop_event = None

    with st.status_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        st.count_status["logs"].append(f"[{ts}] 카운터 정지 (수동)")
        st.count_status["count"] = 0
        st.count_status["up_count"] = 0
        st.count_status["down_count"] = 0
        st.count_status["main_flow_count"] = 0
        st.count_status["direction_score"] = 0.0
        st.count_status["duration_sec"] = 0.0
        st.count_status["flow_per_sec"] = 0.0
        st.count_status["time_bucket"] = ""
        st.count_status["bucket_lag_sec"] = 0.0
        st.count_status["is_valid"] = True
        st.count_status["invalid_reason"] = ""
        if len(st.count_status["logs"]) > 50:
            st.count_status["logs"] = st.count_status["logs"][-50:]
        st.detection_status["count"] = 0
        st.detection_status["up_count"] = 0
        st.detection_status["down_count"] = 0
        st.detection_status["boxes"] = []

    return {"ok": True, "message": "counter stopped"}


@router.post("/rotation/refresh-sites-cache")
def rotation_refresh_sites_cache(force_its: bool = Query(False)):
    """ITS에서 CCTV URL을 다시 받아 data/rotation_sites_cache.json 에 저장."""
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


@router.post("/rotation/start")
def rotation_start(request: Request):
    """서울 유입 핵심 CCTV 순차 캡처 시작. URL은 환경변수 또는 ITS API에서 조회."""
    res = rotation_start_impl(request.app)
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


@router.post("/rotation/stop")
def rotation_stop():
    """서울 유입 핵심 CCTV 순차 캡처 정지."""
    
    with st.sequencer_lock:
        st.sequencer_stop.set()
        with st._segment_lock:
            s = st._active_segment_stop[0]
        if s is not None:
            s.set()
        th = st.sequencer_thread
        if th is not None and th.is_alive():
            try:
                from app.config import CCTV_ROTATION_SEC as _rot_sec
            except ImportError:
                _rot_sec = 30
            th.join(timeout=float(_rot_sec) + 15.0)
        st.sequencer_thread = None
        if st.yolo_thread is not None and st.yolo_thread.is_alive():
            st.yolo_thread.join(timeout=15.0)
        st.yolo_thread = None
    st.sequencer_stop.clear()

    with st.status_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        st.count_status["logs"].append(f"[{ts}] 서울 유입 CCTV 로테이션 정지")
        if len(st.count_status["logs"]) > 50:
            st.count_status["logs"] = st.count_status["logs"][-50:]
        st.detection_status["boxes"] = []
        st.detection_status["cctv"] = ""

    return {"ok": True, "message": "rotation stopped"}


@router.get("/rotation/status")
def rotation_status():
    """로테이션 상태·YOLO 텔레메트리(디버그 화면용)."""
    return rotation_status_payload()

@router.websocket("/ws")
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


@router.get("/live", response_class=HTMLResponse)
def traffic_live_page():
    return html_pages.traffic_live_html()


@router.get("/", response_class=HTMLResponse)
def index():
    return html_pages.index_html()
