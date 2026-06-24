"""로테이션·스트림 상태 진단 (프론트 알림용)."""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_probe_lock = threading.Lock()
_probe_cache: dict[str, Any] = {
    "checked_at": 0.0,
    "url_key": "",
    "ok": True,
    "error_code": "",
    "message": "",
}

_its_health_lock = threading.Lock()
_its_health_cache: dict[str, Any] = {
    "expires_at": 0.0,
    "ok": False,
    "error_code": "",
    "message": "",
    "its_api_result_code": "",
    "row_count": 0,
}

# 내부 분류 코드 → ITS OpenAPI resultCode(표시용)
_ITS_API_RESULT_CODE: dict[str, str] = {
    "its_quota_exceeded": "4001",
    "its_unauthorized": "4005",
    "its_forbidden": "403",
    "its_autofetch_disabled": "—",
    "its_no_match": "—",
    "missing_its_key": "—",
    "its_error": "—",
    "disk_cache_fallback": "—",
}


def its_api_result_code_for(error_code: str, *, raw_message: str = "") -> str:
    code = (error_code or "").strip()
    raw = raw_message or ""
    if not code and not raw:
        return ""
    if code in _ITS_API_RESULT_CODE:
        mapped = _ITS_API_RESULT_CODE[code]
        return mapped if mapped != "—" else ""
    if "4001" in code or "4001" in raw:
        return "4001"
    if "4005" in code or "4005" in raw:
        return "4005"
    if "401" in code or "401" in raw:
        return "401"
    if "403" in code:
        return "403"
    return ""


def _its_health_ttl_ok_sec() -> float:
    try:
        return max(60.0, float(os.getenv("ITS_HEALTH_PROBE_TTL_OK_SEC", "600")))
    except ValueError:
        return 600.0


def _its_health_ttl_error_sec(code: str) -> float:
    from app.config import _sites_error_cache_ttl_sec

    return _sites_error_cache_ttl_sec(code)


def probe_its_api_health(*, force: bool = False) -> dict[str, Any]:
    """ITS OpenAPI 사용 가능 여부 — CCTV_URL 과 무관하게 검사(캐시)."""
    from app.config import _its_autofetch_disabled
    from app.its_client import ItsAutofetchDisabledError, _classify_its_error, get_cctv_list_for_rotation

    try:
        now = time.monotonic()
    except Exception:
        now = 0.0

    with _its_health_lock:
        if not force and now < float(_its_health_cache.get("expires_at") or 0.0):
            return {
                "ok": bool(_its_health_cache.get("ok")),
                "error_code": str(_its_health_cache.get("error_code") or ""),
                "its_api_result_code": str(_its_health_cache.get("its_api_result_code") or ""),
                "message": str(_its_health_cache.get("message") or ""),
                "row_count": int(_its_health_cache.get("row_count") or 0),
                "cached": True,
            }

    def _store(
        *,
        ok: bool,
        error_code: str,
        message: str,
        row_count: int = 0,
        ttl_sec: float,
    ) -> dict[str, Any]:
        api_code = its_api_result_code_for(error_code, raw_message=message)
        payload = {
            "ok": ok,
            "error_code": error_code,
            "its_api_result_code": api_code,
            "message": message,
            "row_count": row_count,
            "cached": False,
        }
        with _its_health_lock:
            _its_health_cache.update(
                {
                    "expires_at": now + ttl_sec,
                    "ok": ok,
                    "error_code": error_code,
                    "its_api_result_code": api_code,
                    "message": message,
                    "row_count": row_count,
                }
            )
        return payload

    if _its_autofetch_disabled():
        return _store(
            ok=False,
            error_code="its_autofetch_disabled",
            message=(
                "ITS 자동 조회가 꺼져 있습니다(ITS_AUTOFETCH_DISABLED=1). "
                "ITS를 쓰려면 해제 후 ITS_API_KEY·한도를 확인하세요."
            ),
            ttl_sec=_its_health_ttl_error_sec("its_autofetch_disabled"),
        )

    key = (os.getenv("ITS_API_KEY") or "").strip()
    if not key:
        return _store(
            ok=False,
            error_code="missing_its_key",
            message=".env 에 ITS_API_KEY 가 없습니다. ITS 포털에서 키를 발급해 설정하세요.",
            ttl_sec=_its_health_ttl_error_sec("missing_its_key"),
        )

    try:
        rows = get_cctv_list_for_rotation(bypass_cache=force)
        return _store(
            ok=True,
            error_code="",
            message="ITS OpenAPI 응답 정상",
            row_count=len(rows or []),
            ttl_sec=_its_health_ttl_ok_sec(),
        )
    except ItsAutofetchDisabledError as exc:
        return _store(
            ok=False,
            error_code="its_autofetch_disabled",
            message=str(exc),
            ttl_sec=_its_health_ttl_error_sec("its_autofetch_disabled"),
        )
    except RuntimeError as exc:
        raw = str(exc)
        code = _classify_its_error(raw) or "its_error"
        return _store(
            ok=False,
            error_code=code,
            message=raw,
            ttl_sec=_its_health_ttl_error_sec(code),
        )
    except Exception as exc:
        logger.debug("probe_its_api_health: %s", exc)
        return _store(
            ok=False,
            error_code="its_error",
            message=str(exc),
            ttl_sec=_its_health_ttl_error_sec("its_error"),
        )


def _idle_message_for_code(code: str, *, raw: str = "") -> str:
    from app.config import peek_idle_rotation_diagnose

    if code:
        idle = peek_idle_rotation_diagnose(ttl_sec=0.0)
        if idle.get("code") == code and idle.get("message"):
            return str(idle["message"])
    if raw:
        return raw
    if code == "its_quota_exceeded":
        return (
            "ITS OpenAPI 호출 한도 초과(4001): 일일/월간 제한량을 초과했습니다. "
            "ITS 포털에서 잔여량을 확인하거나 한도 갱신 후 다시 시도하세요."
        )
    if code == "its_unauthorized":
        return (
            "ITS OpenAPI 인증 실패(4005/401): ITS_API_KEY 또는 ITS_REGISTERED_SITE_URL 을 확인하세요."
        )
    if code == "stream_decode_failed":
        return (
            "ffmpeg 가 CCTV 스트림을 열지 못했습니다. URL 만료·401·네트워크 오류일 수 있습니다. "
            ".env 의 CCTV_URL 을 갱신하거나 ITS 한도 회복 후 지점별 URL을 받으세요."
        )
    return "CCTV 스트림을 사용할 수 없습니다."


def _probe_stream_url(url: str, *, ttl_sec: float = 45.0) -> tuple[bool, str, str]:
    """ffmpeg 1프레임 디코드 시도. (ok, error_code, message)"""
    url = (url or "").strip()
    if not url:
        return False, "stream_url_missing", "CCTV 스트림 URL 이 비어 있습니다."

    try:
        now = time.monotonic()
    except Exception:
        now = 0.0

    with _probe_lock:
        if (
            _probe_cache.get("url_key") == url
            and now - float(_probe_cache.get("checked_at") or 0.0) < ttl_sec
        ):
            return (
                bool(_probe_cache.get("ok")),
                str(_probe_cache.get("error_code") or ""),
                str(_probe_cache.get("message") or ""),
            )

    cmd = [
        "ffmpeg",
        "-loglevel",
        "error",
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "2",
        "-i",
        url,
        "-frames:v",
        "1",
        "-f",
        "null",
        "-",
    ]
    if url.lower().startswith("rtsp"):
        cmd = [
            "ffmpeg",
            "-loglevel",
            "error",
            "-rtsp_transport",
            "tcp",
            "-i",
            url,
            "-frames:v",
            "1",
            "-f",
            "null",
            "-",
        ]

    ok = False
    error_code = "stream_decode_failed"
    message = ""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        ok = r.returncode == 0
        stderr = (r.stderr or "").strip()
        if ok:
            error_code = ""
            message = ""
        elif "401" in stderr or "Unauthorized" in stderr:
            error_code = "stream_401"
            message = "스트림 서버가 401 Unauthorized 를 반환했습니다. URL·토큰이 만료되었을 수 있습니다."
        elif "403" in stderr or "Forbidden" in stderr:
            error_code = "stream_403"
            message = "스트림 서버가 403 Forbidden 을 반환했습니다."
        else:
            message = stderr[-280:] if stderr else "ffmpeg 디코드 실패"
    except subprocess.TimeoutExpired:
        error_code = "stream_timeout"
        message = "ffmpeg 스트림 연결 시간 초과"
    except FileNotFoundError:
        error_code = "ffmpeg_missing"
        message = "컨테이너에 ffmpeg 가 없습니다."
    except Exception as exc:
        message = str(exc)

    with _probe_lock:
        _probe_cache.update(
            {
                "checked_at": now,
                "url_key": url,
                "ok": ok,
                "error_code": error_code,
                "message": message,
            }
        )
    return ok, error_code, message


def _its_failure_alert(its_health: dict[str, Any], *, stream_source: str) -> dict[str, Any]:
    code = (its_health.get("error_code") or "its_error").strip()
    api_code = (its_health.get("its_api_result_code") or "").strip() or its_api_result_code_for(
        code, raw_message=str(its_health.get("message") or "")
    )
    msg = str(its_health.get("message") or "").strip() or _idle_message_for_code(code)
    return {
        "severity": "error",
        "title": "CCTV 동작 불가 — ITS API 사용 불가",
        "its_api_result_code": api_code,
        "its_error_code": code,
        "message": msg + " (CCTV_URL 설정과 무관하게 ITS 상태를 기준으로 표시합니다.)",
        "stream_source": stream_source,
    }


def build_rotation_health(
    *,
    sites: list[tuple[str, str]],
    infer_total: int = 0,
    sequencer_running: bool = False,
    skip_its_probe: bool = False,
) -> dict[str, Any]:
    """프론트 상단 알림·배지용 상태."""
    from app.config import (
        _disk_fallback_active,
        list_env_rotation_stream_sites,
        peek_idle_rotation_diagnose,
    )

    env_sites = list_env_rotation_stream_sites()
    its_health = (
        {"ok": True, "error_code": "", "its_api_result_code": "", "message": "", "row_count": 0}
        if skip_its_probe
        else probe_its_api_health()
    )

    if env_sites:
        stream_source = "env"
        its_preferred = False
        sites_ok = len(env_sites) > 0
    elif sites:
        stream_source = "disk_cache" if _disk_fallback_active else "its"
        its_preferred = True
        sites_ok = True
    else:
        stream_source = "none"
        its_preferred = True
        sites_ok = False

    its_error = (its_health.get("error_code") or "").strip()
    its_api_code = (its_health.get("its_api_result_code") or "").strip()

    alert: dict[str, Any] | None = None

    # 0) ITS 유효성 — CCTV_URL 과 무관 (실패 시 즉시 동작 불가)
    if not its_health.get("ok"):
        alert = _its_failure_alert(its_health, stream_source=stream_source)
    # 1) ITS 우선 경로에서 지점 URL 을 못 얻은 경우
    elif its_preferred and not sites_ok:
        idle = peek_idle_rotation_diagnose(ttl_sec=0.0)
        code = (idle.get("code") or its_error or "its_error").strip()
        api_code = its_api_result_code_for(code) or its_api_code
        alert = {
            "severity": "error",
            "title": "CCTV 동작 불가 — ITS API 조회 실패",
            "its_api_result_code": api_code,
            "its_error_code": code,
            "message": idle.get("message") or _idle_message_for_code(code),
            "stream_source": stream_source,
        }
    # 2) ITS 정상 + env URL ffmpeg 실패 (보조 경고만)
    elif env_sites and infer_total <= 0:
        sample_url = env_sites[0][1]
        ok, err_code, msg = _probe_stream_url(sample_url, ttl_sec=120.0)
        if not ok:
            alert = {
                "severity": "warning",
                "title": "참고: .env CCTV_URL 스트림 오류",
                "its_api_result_code": "",
                "its_error_code": err_code,
                "message": (
                    msg
                    + " ITS 는 사용 가능하나 현재 CCTV_URL 로는 캡처가 되지 않습니다. "
                    "URL 갱신 또는 ITS 지점별 URL 조회를 권장합니다."
                ),
                "stream_source": "env",
            }

    system_ok = alert is None or alert.get("severity") != "error"

    return {
        "system_ok": system_ok,
        "stream_source": stream_source,
        "its_preferred": its_preferred,
        "its_health": its_health,
        "its_error_code": its_error or None,
        "its_api_result_code": its_api_code or None,
        "streams_configured": len(sites) if sites else len(env_sites),
        "alert": alert,
    }
