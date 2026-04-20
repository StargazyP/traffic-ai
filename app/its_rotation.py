"""
ITS CCTV 목록에서 판교·하남·서창·김포·광명 에 해당하는 스트림 URL을 찾는다.
cctvname 부분 문자열 매칭(긴 패턴 우선).
"""

from __future__ import annotations

from app.its_client import get_cctv_list_for_rotation

# (DB에 쓸 고정 이름, cctvname 에서 찾을 패턴들 — 앞쪽이 우선)
_ROTATION_SITE_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("판교분기점", ("판교분기점", "판교분기", "판교IC")),
    ("하남분기점", ("하남분기점", "하남분기", "하남IC")),
    ("서창분기점", ("서창분기점", "서창분기", "서창IC")),
    ("김포", ("김포분기점", "김포TG", "김포IC", "김포")),
    ("광명", ("광명분기점", "광명TG", "광명IC", "광명")),
]


def _norm(name: str) -> str:
    return (name or "").replace(" ", "").replace("\u3000", "")


def resolve_rotation_sites_from_its() -> list[tuple[str, str]]:
    rows = get_cctv_list_for_rotation()
    out: list[tuple[str, str]] = []
    used_urls: set[str] = set()

    for label, patterns in _ROTATION_SITE_PATTERNS:
        url = ""
        matched_name = ""
        for pat in patterns:
            for row in rows:
                cname = row.get("cctvname", "") or ""
                if _norm(pat) and _norm(pat) in _norm(cname):
                    u = (row.get("cctvurl") or "").strip()
                    if u and u not in used_urls:
                        url = u
                        matched_name = cname
                        break
            if url:
                break
        if url:
            used_urls.add(url)
            out.append((label, url))
        else:
            # 매칭 실패 시 스킵 (로그는 호출 측에서)
            pass

    return out
