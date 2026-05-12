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



def rotation_start_impl(app: FastAPI) -> dict[str, Any]:
    """서울 유입 CCTV 순차 캡처·YOLO 워커 기동. HTTP 예외 없이 dict 반환 (lifespan·API 공용)."""
    

    loop = getattr(app.state, "main_loop", None)
    if loop is None:
        return {"ok": False, "error": "no_loop"}

    sites = rotation_sites_configured()
    if not sites:
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

    return {"ok": True, "message": "rotation started", "sites": [s[0] for s in sites]}
