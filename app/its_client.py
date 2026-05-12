"""국가교통정보센터 ITS CCTV OpenAPI 호출.

`ITS_API_KEY`는 `.env` 또는 실행 환경에서 주입합니다. 공개 저장소에 API 키가
올라가지 않도록 코드에는 기본 키를 두지 않습니다.
"""

from __future__ import annotations

import os
from typing import Any

import requests

ITS_CCTV_URL = "https://openapi.its.go.kr:9443/cctvInfo"


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


def get_cctv_list(params: dict[str, Any] | None = None) -> list[dict]:
    p = _default_params()
    if params is not None:
        p.update({k: v for k, v in params.items() if v is not None})
    res = requests.get(ITS_CCTV_URL, params=p, timeout=25)
    res.raise_for_status()
    data = res.json()
    return data.get("response", {}).get("data", []) or []


def get_cctv_list_for_rotation() -> list[dict]:
    """로테이션 URL 자동 매칭용 — 넓은 bbox."""
    return get_cctv_list(_rotation_wide_params())
