"""
ITS CCTV 목록에서 서울 유입 핵심 축별 CCTV를 찾는다.
cctvname 부분 문자열 매칭(긴 패턴 우선).
"""
# [2026-04-28] 경부·영동·서해안·자유로·북부·동서울 축 기준으로 갱신.

from __future__ import annotations

from app.its_client import get_cctv_list_for_rotation

# (DB에 쓸 고정 이름, cctvname 에서 찾을 패턴들 — 앞쪽이 우선)
_ROTATION_SITE_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("수원신갈IC", ("[경부선] 수원신갈", "수원신갈", "[경부선] 수원")),
    ("판교분기점", ("[경부선] 판교분기점", "[수도권제1순환선] 판교분기점", "판교분기점")),
    ("서울TG", ("[경부선] 서울영업소", "[경부선] 서울영업소-광장", "서울영업소")),
    ("용인IC", ("[영동선] 용인", "용인IC", "용인")),
    ("신갈분기점", ("[영동선] 신갈분기점", "[경부선] 신갈분기점", "신갈분기점")),
    ("서평택분기점", ("[서해안선] 서평택분기점", "[평택시흥선] 서평택JCT", "서평택분기점")),
    ("비봉IC", ("[서해안선] 비봉", "비봉IC", "비봉")),
    ("매송나들목", ("[서해안선] 매송", "[서해안선] 매송휴게소", "매송")),
    ("장항IC", ("장항IC", "장항")),
    ("자유로분기점", ("[수도권제1순환선] 자유로분기점", "자유로분기점")),
    ("일산IC", ("[수도권제1순환선] 일산", "일산IC")),
    ("양주IC", ("[구리포천선] 양주IC", "[수도권제1순환선] 양주TG", "양주IC")),
    ("의정부IC", ("[수도권제1순환선] 의정부IC", "의정부IC")),
    ("동의정부IC북측", ("[구리포천선] 동의정부IC북측", "동의정부IC북측")),
    ("남구리IC", ("[구리포천선] 남구리IC", "남구리IC")),
    ("구리IC", ("[수도권제1순환선] 구리", "[수도권제1순환선] 구리2", "구리IC")),
    ("중랑IC교", ("[구리포천선] 중랑IC교", "중랑IC교")),
    ("신월IC", ("[경인선] 신월", "신월IC")),
    ("김포IC", ("[수도권제1순환선] 김포", "김포IC")),
    ("서운분기점", ("[수도권제1순환선] 서운분기점", "[경인선] 서운분기점2", "서운분기점")),
    ("토평IC", ("[수도권제1순환선] 토평", "토평IC")),
    ("서하남IC", ("[수도권제1순환선] 서하남", "서하남IC")),
    ("상일IC", ("[수도권제1순환선] 상일", "상일IC")),
    ("시흥IC", ("[수도권제1순환선] 시흥", "시흥IC")),
    ("안현분기점", ("[수도권제1순환선] 안현분기점", "안현분기점")),
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
