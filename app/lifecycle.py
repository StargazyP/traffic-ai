# 2026-04-28: DB 시간 단위 압축 스케줄러 기동/종료 로직 추가.
"""FastAPI lifespan: start rotation workers on boot, join threads on shutdown."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import state as st
from app.rotation_service import rotation_start_impl
from db_mysql import run_hourly_compression_loop

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """기동 시 로테이션·YOLO 캡처 자동 시작(지점 URL 없으면 스킵). 종료 시 워커 정리."""
    loop = asyncio.get_running_loop()
    app.state.main_loop = loop

    boot = rotation_start_impl(app)
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

    compression_enabled = os.getenv("DB_HOURLY_COMPRESSION_ENABLED", "1").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if compression_enabled:
        interval_seconds = int(os.getenv("DB_HOURLY_COMPRESSION_INTERVAL_SEC", "3600"))
        retention_hours = int(os.getenv("DB_RAW_RETENTION_HOURS", "72"))
        st.rollup_stop_event = threading.Event()
        st.rollup_thread = threading.Thread(
            target=run_hourly_compression_loop,
            kwargs={
                "stop_event": st.rollup_stop_event,
                "interval_seconds": interval_seconds,
                "retention_hours": retention_hours,
            },
            daemon=True,
        )
        st.rollup_thread.start()
        logger.info(
            "DB 시간 압축 스케줄러 시작(interval=%ss, retention=%sh)",
            interval_seconds,
            retention_hours,
        )

    yield

    with st.sequencer_lock:
        st.sequencer_stop.set()
        with st._segment_lock:
            s = st._active_segment_stop[0]
        if s is not None:
            s.set()
        try:
            from app.config import CCTV_ROTATION_SEC as _rot_sec
        except ImportError:
            _rot_sec = 30
        if st.sequencer_thread is not None and st.sequencer_thread.is_alive():
            st.sequencer_thread.join(timeout=float(_rot_sec) + 15.0)
        st.sequencer_thread = None
        if st.yolo_thread is not None and st.yolo_thread.is_alive():
            st.yolo_thread.join(timeout=15.0)
        st.yolo_thread = None

    if st.rollup_stop_event is not None:
        st.rollup_stop_event.set()
    if st.rollup_thread is not None and st.rollup_thread.is_alive():
        st.rollup_thread.join(timeout=3.0)
    st.rollup_thread = None
    st.rollup_stop_event = None
