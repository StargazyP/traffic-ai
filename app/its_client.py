"""국가교통정보센터 ITS CCTV OpenAPI 호출.

`ITS_API_KEY`는 `.env` 또는 실행 환경에서 주입합니다. 공개 저장소에 API 키가
올라가지 않도록 코드에는 기본 키를 두지 않습니다.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

ITS_CCTV_URL = "https://openapi.its.go.kr:9443/cctvInfo"

_ITS_HTTP_HEADERS = {
    "User-Agent": os.getenv(
        "ITS_HTTP_USER_AGENT",
        "Mozilla/5.0 (compatible; traffic-ai/1.0; ITS OpenAPI client)",
    ),
}

_list_cache_lock = threading.Lock()
# cache_key -> (expires_monotonic, rows_or_none, error_message)
_list_cache: dict[str, tuple[float, list[dict] | None, str]] = {}


class ItsAutofetchDisabledError(RuntimeError):
    """ITS_AUTOFETCH_DISABLED=1 일 때 API 호출 차단."""


def _its_autofetch_disabled() -> bool:
    raw = (os.getenv("ITS_AUTOFETCH_DISABLED") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def invalidate_cctv_list_cache() -> None:
    """로테이션 캐시 무효화 시 목록 캐시도 비움."""
    with _list_cache_lock:
        _list_cache.clear()


def _its_request_headers() -> dict[str, str]:
    """ITS 포털에 등록한 사용처(사이트)와 맞추려면 ITS_REGISTERED_SITE_URL 로 Referer/Origin 전달."""
    h: dict[str, str] = dict(_ITS_HTTP_HEADERS)
    base = (os.getenv("ITS_REGISTERED_SITE_URL") or "").strip().rstrip("/")
    if base:
        h.setdefault("Referer", base if base.endswith("/") else base + "/")
        h.setdefault("Origin", base)
    return h


def _classify_its_error(raw: str) -> str:
    if not raw:
        return ""
    if "4001" in raw or "제한량" in raw or "호출 한도" in raw:
        return "its_quota_exceeded"
    if "4005" in raw or "인증키" in raw or "401" in raw or "인증 실패" in raw:
        return "its_unauthorized"
    if "403" in raw or "접근 거부" in raw:
        return "its_forbidden"
    return "its_error"


def _its_http_error_message(res: requests.Response) -> str:
    """로그·프론트용: URL·apiKey 노출 없이 ITS HTTP 오류 설명."""
    code = int(res.status_code or 0)
    result_code = None
    result_msg = ""
    try:
        payload = res.json()
        header = payload.get("header") if isinstance(payload, dict) else None
        if isinstance(header, dict):
            result_code = header.get("resultCode")
            result_msg = str(header.get("resultMsg") or "").strip()
    except ValueError:
        pass

    if result_code == 4001 or "제한량" in result_msg:
        return (
            "ITS OpenAPI 호출 한도 초과(4001): 개인 일일/월간 제한량을 넘었습니다. "
            "ITS 포털에서 잔여량을 확인하거나 내일(또는 한도 갱신 후) 다시 시도하세요. "
            "당장 테스트하려면 .env 에 CCTV_URL(직접 m3u8/rtsp)을 넣으면 ITS 없이 로테이션할 수 있습니다."
        )
    if result_code == 4005 or "존재하지 않는 인증키" in result_msg:
        return (
            "ITS OpenAPI 인증 실패(4005): ITS_API_KEY 가 올바르지 않습니다. "
            ".env 의 키를 ITS 마이페이지에서 재발급·확인하세요."
        )
    if result_msg:
        return f"ITS OpenAPI 오류 ({result_code or code}): {result_msg}"

    if code == 401:
        return (
            "ITS OpenAPI 인증 실패(401): ITS_API_KEY 거부 또는 사용처 불일치일 수 있습니다. "
            "마이페이지의 사용처(사이트)가 실제 서비스 URL(예: HTTPS·호스트명)과 같은지 확인하고, "
            "필요 시 .env 에 ITS_REGISTERED_SITE_URL 로 등록 주소와 동일한 값을 넣어보세요. "
            "ITS 없이 쓰려면 CCTV_URL(직접 m3u8/rtsp)을 설정할 수 있습니다."
        )
    if code == 403:
        return (
            "ITS 접근 거부(403): IP 차단 또는 호출 한도 초과 가능성이 있습니다. ITS 안내 또는 CCTV_URL 직접 지정을 검토하세요."
        )
    snippet = ""
    try:
        txt = (res.text or "").strip()
        if txt:
            snippet = " (" + txt[:200].replace("\n", " ") + ")"
    except Exception:
        pass
    return f"ITS HTTP 오류 ({code}).{snippet}"


def _api_key() -> str:
    key = (os.getenv("ITS_API_KEY") or "").strip()
    if not key:
        raise RuntimeError(
            "ITS_API_KEY가 설정되지 않았습니다. .env에 ITS_API_KEY를 넣거나 "
            "CCTV_URL/지점별 CCTV_URL_* 환경변수로 스트림을 직접 지정하세요."
        )
    return key


def _default_params() -> dict[str, Any]:
    return {
        "apiKey": _api_key(),
        "type": "all",
        "cctvType": "1",
        "minX": os.getenv("ITS_MINX", "126.7"),
        "maxX": os.getenv("ITS_MAXX", "127.2"),
        "minY": os.getenv("ITS_MINY", "37.4"),
        "maxY": os.getenv("ITS_MAXY", "37.7"),
        "getType": "json",
    }


def _rotation_wide_params() -> dict[str, Any]:
    """서울 유입 로테이션 지점을 포함하도록 검색 범위 확대."""
    return {
        "apiKey": _api_key(),
        "type": "all",
        "cctvType": "1",
        "minX": os.getenv("ITS_ROTATION_MINX", "125.5"),
        "maxX": os.getenv("ITS_ROTATION_MAXX", "128.5"),
        "minY": os.getenv("ITS_ROTATION_MINY", "37.0"),
        "maxY": os.getenv("ITS_ROTATION_MAXY", "38.2"),
        "getType": "json",
    }


def _list_cache_key(params: dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _list_cache_ttl_success_sec() -> float:
    try:
        return max(5.0, float(os.getenv("ITS_CCTV_LIST_CACHE_TTL_SEC", "300")))
    except ValueError:
        return 300.0


def _list_cache_ttl_error_sec(code: str) -> float:
    perm = {"its_quota_exceeded", "its_unauthorized", "its_forbidden"}
    if code in perm:
        try:
            return max(60.0, float(os.getenv("ROTATION_SITES_PERM_ERROR_TTL_SEC", "1800")))
        except ValueError:
            return 1800.0
    try:
        return max(5.0, float(os.getenv("ROTATION_SITES_ERROR_TTL_SEC", "60")))
    except ValueError:
        return 60.0


def _fetch_cctv_list_uncached(params: dict[str, Any]) -> list[dict]:
    try:
        res = requests.get(
            ITS_CCTV_URL,
            params=params,
            headers=_its_request_headers(),
            timeout=25,
        )
    except requests.RequestException as exc:
        raise RuntimeError(
            f"ITS 네트워크 오류: {type(exc).__name__}. openapi.its.go.kr 연결 및 방화벽을 확인하세요."
        ) from exc
    if not res.ok:
        raise RuntimeError(_its_http_error_message(res))
    try:
        data = res.json()
    except ValueError as exc:
        raise RuntimeError("ITS 응답이 JSON 이 아닙니다. 서버 점검 또는 파라미터를 확인하세요.") from exc
    return data.get("response", {}).get("data", []) or []


def get_cctv_list(
    params: dict[str, Any] | None = None,
    *,
    bypass_cache: bool = False,
) -> list[dict]:
    """ITS cctvInfo 1회. 동일 파라미터는 TTL 동안 메모리 캐시(한도·중복 호출 방지)."""
    if _its_autofetch_disabled():
        raise ItsAutofetchDisabledError(
            "ITS_AUTOFETCH_DISABLED=1 로 ITS OpenAPI 자동 조회가 꺼져 있습니다. "
            "CCTV_URL/CCTV_URL_* 로 스트림을 지정하거나 환경변수를 해제하세요."
        )

    p = _default_params()
    if params is not None:
        p.update({k: v for k, v in params.items() if v is not None})

    key = _list_cache_key(p)
    now = time.monotonic()

    if not bypass_cache:
        with _list_cache_lock:
            entry = _list_cache.get(key)
            if entry is not None:
                expires, rows, err = entry
                if now < expires:
                    if err:
                        raise RuntimeError(err)
                    return list(rows or [])

    try:
        rows = _fetch_cctv_list_uncached(p)
    except RuntimeError as exc:
        raw = str(exc)
        code = _classify_its_error(raw)
        ttl = _list_cache_ttl_error_sec(code)
        if not bypass_cache:
            with _list_cache_lock:
                _list_cache[key] = (now + ttl, None, raw)
        raise

    ttl = _list_cache_ttl_success_sec()
    if not bypass_cache:
        with _list_cache_lock:
            _list_cache[key] = (now + ttl, list(rows), "")
    return rows


def get_cctv_list_for_rotation(*, bypass_cache: bool = False) -> list[dict]:
    """로테이션 URL 자동 매칭용 — 넓은 bbox."""
    return get_cctv_list(_rotation_wide_params(), bypass_cache=bypass_cache)
