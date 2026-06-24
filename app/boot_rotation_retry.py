"""Docker/ITS 지연 등으로 부팅 직후 로테이션 지점(no_sites)이 비어 있을 때 자동 재시도."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Callable

from fastapi import FastAPI

logger = logging.getLogger(__name__)

_holder: dict[str, Any] = {"stop": threading.Event(), "thread": None}


def halt_rotation_boot_retry(*, join_timeout: float = 5.0) -> None:
    _holder["stop"].set()
    th = _holder.get("thread")
    if isinstance(th, threading.Thread) and th.is_alive():
        th.join(timeout=join_timeout)
    _holder["thread"] = None


def spawn_rotation_boot_retry(
    app: FastAPI,
    starter: Callable[[FastAPI], dict[str, Any]],
    *,
    log_label: str = "로테이션",
) -> None:
    existing = _holder["thread"]
    if isinstance(existing, threading.Thread) and existing.is_alive():
        return

    _holder["stop"].clear()

    def _retry_interval_sec() -> float:
        try:
            return max(3.0, float(os.getenv("ROTATION_BOOT_RETRY_INTERVAL_SEC", "15")))
        except ValueError:
            return 15.0

    try:
        max_attempts = int(os.getenv("ROTATION_BOOT_RETRY_MAX", "120"))
    except ValueError:
        max_attempts = 120

    iv = _retry_interval_sec()

    def _runner():
        attempts = 0
        interval = iv
        while not _holder["stop"].wait(timeout=interval):
            rb = starter(app)
            if rb.get("ok"):
                logger.info(
                    "%s 자동 재시도 후 시작: %s",
                    log_label,
                    rb.get("message") or rb,
                )
                print(
                    f"[ROTATION] 자동 재시도 성공: {rb.get('sites', [])}",
                    flush=True,
                )
                return
            attempts += 1
            err = rb.get("error")
            if err != "no_sites":
                logger.warning(
                    "%s 자동 재시도 중단(%s): %s",
                    log_label,
                    err,
                    rb,
                )
                return
            if max_attempts > 0 and attempts >= max_attempts:
                logger.warning(
                    "%s 자동 재시도 횟수 상한(%s회, ITS·.env 확인)",
                    log_label,
                    max_attempts,
                )
                return
            logger.info(
                "%s 자동 재시도 %s회차: 지점 없음, %ss 대기",
                log_label,
                attempts,
                int(interval),
            )

    th = threading.Thread(
        target=_runner,
        daemon=True,
        name="rotation-boot-retry",
    )
    _holder["thread"] = th
    th.start()
    logger.info(
        "로테이션 부팅 재시도 스레드 시작(interval=%ss, max_attempts=%s)",
        iv,
        max_attempts if max_attempts > 0 else "unlimited",
    )
