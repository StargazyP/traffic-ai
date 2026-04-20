"""브라우저 HLS 미리보기용 최소 프록시 (KT cctvsec 등 직접 XHR 403 시 서버가 대신 요청)."""

from __future__ import annotations

import hashlib
import re
import threading
import time
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, field_validator

try:
    from app.config import (
        HLS_PROXY_ALLOWED_HOSTS,
        HLS_PROXY_ORIGIN,
        HLS_PROXY_REFERER,
        HLS_PROXY_UA,
    )
except ImportError:
    HLS_PROXY_ALLOWED_HOSTS = ("cctvsec.ktict.co.kr", "cctv.its.go.kr", "openapi.its.go.kr")
    HLS_PROXY_REFERER = "https://openapi.its.go.kr/"
    HLS_PROXY_ORIGIN = "https://openapi.its.go.kr"
    HLS_PROXY_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

router = APIRouter(prefix="/hls", tags=["hls"])

_hls_registry: dict[str, str] = {}
# tid → 이 링크가 등장한 부모 m3u8 URL (KT가 직계 부모 Referer를 요구하는 경우)
_hls_parent: dict[str, str] = {}

# 마스터 응답 최종 URL (리다이렉트 후 r.url). 보조 Referer.
_last_kt_master_url: str | None = None
_last_kt_master_lock = threading.Lock()


def _upstream_headers() -> dict[str, str]:
    return {
        "User-Agent": HLS_PROXY_UA,
        "Referer": HLS_PROXY_REFERER,
        "Origin": HLS_PROXY_ORIGIN,
        "Accept": "*/*",
    }


def _referer_candidates(target_url: str, *, play_tid: str | None = None) -> list[str]:
    """KT 세그먼트 요청마다 허용되는 Referer가 달라 여러 후보를 순서대로 시도."""
    out: list[str] = []
    try:
        sp = urlsplit(target_url)
        host = (sp.hostname or "").lower()
        path = sp.path or "/"
        bare = urlunsplit((sp.scheme, sp.netloc, path, "", ""))

        # -1) 플레이리스트에 기록된 직계 부모 m3u8 (가장 신뢰)
        if play_tid and play_tid in _hls_parent:
            par = _hls_parent[play_tid]
            if par and (urlsplit(par).hostname or "").lower() == host:
                out.append(par)

        # 0) 직전에 성공한 마스터 URL (nimble 세션·wmsAuthSign 쿼리 유지)
        with _last_kt_master_lock:
            lm = _last_kt_master_url
        if lm and (urlsplit(lm).hostname or "").lower() == host:
            out.append(lm)

        # 1) 같은 디렉터리의 main_stream.m3u8 (경로 마지막 파일명 제거 후)
        segs = [x for x in path.split("/") if x]
        if segs:
            last = segs[-1]
            if "." in last:
                segs = segs[:-1]
            if segs:
                base = "/" + "/".join(segs) + "/"
                master = urlunsplit((sp.scheme, sp.netloc, base + "main_stream.m3u8", "", ""))
                out.append(master)

        # 2) 쿼리 없는 자기 URL (일부 세그먼트)
        out.append(bare)

        # 3) openapi (기본)
        if "cctvsec.ktict" in host:
            out.append(HLS_PROXY_REFERER)
    except Exception:
        pass
    if HLS_PROXY_REFERER not in out:
        out.append(HLS_PROXY_REFERER)

    seen: set[str] = set()
    uniq: list[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


# KT CDN: 첫 m3u8의 Set-Cookie가 이후 요청에 필요 → Session. Referer는 요청마다 덮어씀.
_upstream_sess = requests.Session()
_upstream_sess.headers.update(_upstream_headers())
_sess_lock = threading.Lock()


def _session_get(url: str, *, stream: bool, play_tid: str | None = None) -> requests.Response:
    """Referer 후보 순회 + 짧은 재시도."""
    last_err: requests.RequestException | None = None
    last: requests.Response | None = None
    refs = _referer_candidates(url, play_tid=play_tid) or [HLS_PROXY_REFERER]
    for ref in refs:
        hdr = dict(_upstream_headers())
        hdr["Referer"] = ref
        for attempt in range(2):
            try:
                with _sess_lock:
                    last = _upstream_sess.get(url, timeout=120, stream=stream, headers=hdr)
            except requests.RequestException as exc:
                last_err = exc
                if ref == refs[-1] and attempt == 1:
                    raise HTTPException(
                        status_code=502,
                        detail=f"KT CDN 연결 실패: {exc}",
                    ) from exc
                time.sleep(0.12)
                continue
            if last is not None and last.status_code == 200:
                return last
            if last is not None:
                try:
                    last.close()
                except Exception:
                    pass
            time.sleep(0.12)
    if last is None:
        msg = str(last_err) if last_err else "응답 없음"
        raise HTTPException(status_code=502, detail=f"업스트림 요청 실패: {msg}")
    return last


def _host_allowed(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return any(host == h.lower() or host.endswith("." + h.lower()) for h in HLS_PROXY_ALLOWED_HOSTS)


def _ensure_tid(url: str, *, parent_manifest: str | None = None) -> str:
    tid = hashlib.sha256(url.encode("utf-8")).hexdigest()
    _hls_registry[tid] = url
    if parent_manifest:
        _hls_parent[tid] = parent_manifest
    return tid


def _rewrite_m3u8(content: str, manifest_url: str, request: Request) -> str:
    prefix = str(request.base_url).rstrip("/")
    lines_out: list[str] = []
    for line in content.splitlines(keepends=False):
        if "URI=" in line and "#EXT" in line:

            def _sub_uri(m: re.Match[str]) -> str:
                inner = m.group(1)
                if inner.startswith("data:") or inner.startswith("skd://"):
                    return m.group(0)
                abs_u = urljoin(manifest_url, inner)
                if abs_u.startswith("http") and _host_allowed(abs_u):
                    tid = _ensure_tid(abs_u, parent_manifest=manifest_url)
                    return f'URI="{prefix}/hls/play/{tid}"'
                return m.group(0)

            line = re.sub(r'URI="([^"]+)"', _sub_uri, line)
            lines_out.append(line)
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            lines_out.append(line)
            continue
        abs_u = urljoin(manifest_url, stripped)
        if abs_u.startswith("http") and _host_allowed(abs_u):
            tid = _ensure_tid(abs_u, parent_manifest=manifest_url)
            lines_out.append(f"{prefix}/hls/play/{tid}")
        else:
            lines_out.append(line)
    ending = "\n" if content.endswith("\n") else ""
    return "\n".join(lines_out) + ending


class _RegisterBody(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def _must_be_http(cls, v: str) -> str:
        s = (v or "").strip()
        if not (s.startswith("http://") or s.startswith("https://")):
            raise ValueError("http(s) URL 필요")
        return s


@router.post("/register")
def hls_register(body: _RegisterBody, request: Request):
    u = body.url
    if not _host_allowed(u):
        raise HTTPException(
            status_code=400,
            detail="허용되지 않은 호스트입니다. HLS_PROXY_ALLOWED_HOSTS 확인.",
        )
    # 새 스트림마다 이전 KT 세션 쿠키·마스터 URL 제거(다른 카메라·토큰 충돌 방지)
    global _last_kt_master_url
    with _sess_lock:
        _upstream_sess.cookies.clear()
    with _last_kt_master_lock:
        _last_kt_master_url = None
    _hls_parent.clear()
    tid = _ensure_tid(u)
    play_path = f"/hls/play/{tid}"
    return {"play_url": play_path, "play_url_absolute": str(request.base_url).rstrip("/") + play_path}


def _bad_upstream(status: int) -> HTTPException:
    return HTTPException(
        status_code=502,
        detail=(
            f"KT CDN 업스트림 HTTP {status}. "
            "m3u8·wmsAuthSign·nimblesessionid는 수 분 단위로 만료될 수 있습니다. "
            "페이지 새로고침 후 [보기]를 다시 누르세요. "
            "ITS OpenAPI용 ITS_API_KEY와 동일 키를 .env에 두는 것을 권장합니다."
        ),
    )


@router.get("/play/{tid}")
def hls_play(tid: str, request: Request):
    url = _hls_registry.get(tid)
    if not url or not _host_allowed(url):
        raise HTTPException(status_code=404, detail="unknown id")

    try:
        r = _session_get(url, stream=True, play_tid=tid)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"업스트림 연결 실패: {exc}") from exc

    if r.status_code == 403:
        r.close()
        raise HTTPException(status_code=502, detail="업스트림 403 — HLS_PROXY_REFERER·ORIGIN 확인")
    if r.status_code != 200:
        r.close()
        raise _bad_upstream(r.status_code)

    first = next(r.iter_content(chunk_size=65536), b"")
    head = first.lstrip(b"\xef\xbb\xbf")
    if head.startswith(b"#EXTM3U"):
        buf = first
        for chunk in r.iter_content(chunk_size=65536):
            buf += chunk
        text = buf.decode("utf-8", errors="replace")
        global _last_kt_master_url
        effective_manifest = getattr(r, "url", None) or url
        with _last_kt_master_lock:
            _last_kt_master_url = effective_manifest
        rewritten = _rewrite_m3u8(text, effective_manifest, request)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store"},
        )

    ct = r.headers.get("content-type") or "video/mp2t"

    def gen():
        yield first
        for chunk in r.iter_content(chunk_size=65536):
            if chunk:
                yield chunk

    return StreamingResponse(gen(), media_type=ct, headers={"Cache-Control": "no-store"})
