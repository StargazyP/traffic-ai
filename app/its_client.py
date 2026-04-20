"""국가교통정보센터 ITS CCTV OpenAPI 호출.

`ITS_API_KEY`(미설정 시 코드 내 기본값)로 `cctvInfo`를 조회하면, 응답 `cctvurl`의
서명 파라미터(wmsAuthSign 등)에 동일 키가 연동되는 경우가 많습니다.
운영 시에는 반드시 발급받은 키를 .env 의 ITS_API_KEY 로 지정하세요.
"""

from __future__ import annotations

import os
from typing import Any

import requests

ITS_CCTV_URL = "https://openapi.its.go.kr:9443/cctvInfo"


def _default_params() -> dict[str, Any]:
    return {
        "apiKey": os.getenv("ITS_API_KEY", "127cdf2be84b4a618fa7ac95bd695d5f"),
        "type": "all",
        "cctvType": "1",
        "minX": os.getenv("ITS_MINX", "126.7"),
        "maxX": os.getenv("ITS_MAXX", "127.2"),
        "minY": os.getenv("ITS_MINY", "37.4"),
        "maxY": os.getenv("ITS_MAXY", "37.7"),
        "getType": "json",
    }


def _rotation_wide_params() -> dict[str, Any]:
    """로테이션 5지점(수도권·김포·광명 등) 포함하도록 검색 범위 확대."""
    return {
        "apiKey": os.getenv("ITS_API_KEY", "127cdf2be84b4a618fa7ac95bd695d5f"),
        "type": "all",
        "cctvType": "1",
        "minX": os.getenv("ITS_ROTATION_MINX", "125.5"),
        "maxX": os.getenv("ITS_ROTATION_MAXX", "128.5"),
        "minY": os.getenv("ITS_ROTATION_MINY", "37.0"),
        "maxY": os.getenv("ITS_ROTATION_MAXY", "38.2"),
        "getType": "json",
    }


def get_cctv_list(params: dict[str, Any] | None = None) -> list[dict]:
    p = params if params is not None else _default_params()
    res = requests.get(ITS_CCTV_URL, params=p, timeout=25)
    res.raise_for_status()
    data = res.json()
    return data.get("response", {}).get("data", []) or []


def get_cctv_list_for_rotation() -> list[dict]:
    """5지점 URL 자동 매칭용 — 넓은 bbox."""
    return get_cctv_list(_rotation_wide_params())
