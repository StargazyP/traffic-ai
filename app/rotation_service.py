"""CCTV rotation orchestration (sequencer + YOLO worker threads)."""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from fastapi import FastAPI

from app import state as st
from app.workers import sequential_cctv_loop, yolo_worker


def rotation_sites_configured() -> list[tuple[str, str]]:
    try:
        from app.config import get_effective_rotation_sites
    except ImportError:
        return []
    return get_effective_rotation_sites()


def rotation_status_payload() -> dict[str, Any]:
    """디버그 프론트용 로테이션·YOLO 상태."""
    from app.config import CCTV_ROTATION_SEC, get_rotation_parallel_slots, peek_idle_rotation_diagnose

    sites = rotation_sites_configured()
    idle = peek_idle_rotation_diagnose() if not sites else {}
    with st.rotation_telemetry_lock:
        tel = dict(st.rotation_telemetry)
    with st.rotation_tag_lock:
        active = (st.rotation_active_cctv or "").strip()
        active_group = list(st.rotation_active_group or [])
    with st.sequencer_lock:
        seq_running = st.sequencer_thread is not None and st.sequencer_thread.is_alive()
    yolo_running = st.yolo_thread is not None and st.yolo_thread.is_alive()
    with st.status_lock:
        cnt = int(st.count_status.get("count") or 0)
        cname = (st.count_status.get("cctv_name") or "").strip()
    last_processed = (tel.get("last_cctv_processed") or "").strip()
    return {
        "sequencer_running": seq_running,
        "yolo_running": yolo_running,
        "rotation_running": seq_running or yolo_running,
        "active_cctv": active,
        "active_cctv_group": active_group,
        "last_cctv_processed": last_processed,
        "rotation_sites": [n for n, _ in sites],
        "streams_effective_count": len(sites),
        "idle_reason": idle or None,
        "rotation_sec": int(CCTV_ROTATION_SEC),
        "rotation_parallel_slots": get_rotation_parallel_slots(),
        "telemetry": tel,
        "count_status_summary": {"cctv_name": cname, "count": cnt},
    }


def rotation_start_impl(app: FastAPI) -> dict[str, Any]:
    """서울 유입 CCTV 순차 캡처·YOLO 워커 기동. HTTP 예외 없이 dict 반환 (lifespan·API 공용).

    sites 가 비어 있는 사유가 ITS 한도 초과/인증 실패 등 영구 오류면 별도 에러 코드를 반환해
    부팅 자동 재시도가 무한히 ITS 한도를 더 소진하지 않도록 한다.
    """

    loop = getattr(app.state, "main_loop", None)
    if loop is None:
        return {"ok": False, "error": "no_loop"}

    sites = rotation_sites_configured()
    if not sites:
        try:
            from app.config import last_rotation_sites_error_code
            code = last_rotation_sites_error_code()
        except Exception:
            code = ""
        if code in {"its_quota_exceeded", "its_unauthorized", "its_forbidden", "its_autofetch_disabled"}:
            return {"ok": False, "error": code}
        return {"ok": False, "error": "no_sites"}

    with st.sequencer_lock:
        if st.sequencer_thread is not None and st.sequencer_thread.is_alive():
            return {
                "ok": True,
                "message": "already running",
                "sites": [s[0] for s in sites],
            }
        st.sequencer_stop.clear()
        st.sequencer_thread = threading.Thread(
            target=sequential_cctv_loop,
            args=(loop,),
            daemon=True,
            name="cctv-sequencer",
        )
        st.telemetry_reset()
        st.sequencer_thread.start()

        # stop 후 join으로 죽은 스레드는 None 처리되므로, 재시작 시 반드시 새 스레드 생성
        if st.yolo_thread is None or not st.yolo_thread.is_alive():
            st.yolo_thread = threading.Thread(
                target=yolo_worker,
                args=(loop,),
                daemon=True,
                name="yolo-worker",
            )
            st.yolo_thread.start()

    with st.status_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        st.count_status["logs"].append(
            f"[{ts}] 서울 유입 CCTV 로테이션 시작 → " + " → ".join(x[0] for x in sites)
        )
        if len(st.count_status["logs"]) > 50:
            st.count_status["logs"] = st.count_status["logs"][-50:]

    try:
        from app.config import invalidate_idle_rotation_diagnose

        invalidate_idle_rotation_diagnose()
    except ImportError:
        pass

    return {"ok": True, "message": "rotation started", "sites": [s[0] for s in sites]}
