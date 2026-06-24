import logging
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
# [2026-04-28] 서울 유입 핵심 축 로테이션 지점으로 확정.
# 프로젝트 루트의 .env 자동 로드 (없으면 무시)
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

# 예: export CCTV_URL="https://....m3u8"
CCTV_URL = os.getenv("CCTV_URL", "여기에 m3u8 URL")

# 브라우저 HLS 미리보기 프록시(/hls/*) 허용 호스트
def _hls_hosts_csv() -> str:
    return os.getenv(
        "HLS_PROXY_ALLOWED_HOSTS",
        "cctvsec.ktict.co.kr,cctv.its.go.kr,openapi.its.go.kr",
    )


HLS_PROXY_ALLOWED_HOSTS: tuple[str, ...] = tuple(
    h.strip() for h in _hls_hosts_csv().split(",") if h.strip()
)
HLS_PROXY_REFERER = os.getenv("HLS_PROXY_REFERER", "https://openapi.its.go.kr/")
HLS_PROXY_ORIGIN = os.getenv("HLS_PROXY_ORIGIN", "https://openapi.its.go.kr")
HLS_PROXY_UA = os.getenv(
    "HLS_PROXY_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# YOLO ingest 전용 (RTSP / 로컬 mp4 / 안정적인 스트림). 설정 시 OpenCV는 이 URL만 사용.
# 프론트 고정 미리보기 영상은 CCTV_URL을 별도로 둔다 (YOLO·로테이션과 동기 안 됨).
YOLO_INGEST_URL = (os.getenv("YOLO_INGEST_URL") or "").strip()
MODEL_PATH = os.getenv("MODEL_PATH", "models/yolov8n.pt")

# 성능 관련 설정
FRAME_WIDTH = int(os.getenv("FRAME_WIDTH", "640"))
FRAME_HEIGHT = int(os.getenv("FRAME_HEIGHT", "360"))
FRAME_SKIP = int(os.getenv("FRAME_SKIP", "2"))  # 2면 2프레임 중 1프레임만 추론

# YOLO 설정
IMG_SIZE = int(os.getenv("IMG_SIZE", "640"))
# 멀리 있는 CCTV는 차량이 작게 보임 → 추론 입력 해상도 상향 권장 (기본 960)
try:
    YOLO_IMGSZ = max(320, min(1280, int(os.getenv("YOLO_IMGSZ", "960"))))
except ValueError:
    YOLO_IMGSZ = 960
CONF_THRES = float(os.getenv("CONF_THRES", "0.25"))
# 로테이션 루프 model.track() 전용 (검출률↑ 시 카운트 유리)
YOLO_TRACK_CONF = float(os.getenv("YOLO_TRACK_CONF", "0.25"))
USE_CUDA = os.getenv("USE_CUDA", "true").lower() in {"1", "true", "yes"}

# WebSocket 디버그: YOLO 추론 프레임을 JPEG base64로 전송 (대역·CPU 사용↑)
DEBUG_IMAGE = os.getenv("DEBUG_IMAGE", "0").strip().lower() in {"1", "true", "yes"}
try:
    DEBUG_IMAGE_EVERY = max(1, int(os.getenv("DEBUG_IMAGE_EVERY", "5")))
except ValueError:
    DEBUG_IMAGE_EVERY = 5

# 순차 로테이션: 한 번에 한 지점만 YOLO 처리, 각 지점당 아래 초만큼 머문 뒤 다음 지점.
CCTV_ROTATION_SEC = int(os.getenv("CCTV_ROTATION_SEC", "30"))

# UI·세그먼트 병렬 로테이션: 한 주기에 동시에 강조할 지점 수 (YOLO는 전 지점 라운드로빈)
def get_rotation_parallel_slots() -> int:
    try:
        return max(1, int(os.getenv("ROTATION_PARALLEL_SLOTS", "4")))
    except ValueError:
        return 4


def rotation_active_names(
    site_names: list[str],
    window_start: int,
    *,
    slots: int | None = None,
) -> list[str]:
    """ordered 지점명 목록에서 window_start부터 slots개 병렬 활성 그룹."""
    if not site_names:
        return []
    n = len(site_names)
    k = slots if slots is not None else get_rotation_parallel_slots()
    k = max(1, min(k, n))
    return [site_names[(window_start + i) % n] for i in range(k)]

# 가상선 세로 위치 (프레임 높이 × 비율). .env 예: LINE_Y_RATIO=0.6
_line_y = float(os.getenv("LINE_Y_RATIO", "0.6"))
LINE_Y_RATIO = min(0.95, max(0.05, _line_y))

# 서울 유입 25지점 YOLO ROI·가상선 특화.
# - top/left/width: 전 프레임 대비 세로 크롭 시작 비율·가로 시작·가로 폭 (0~1).
# - line_y_ratio: ROI 크롭 **내부** 세로 비율 (0~1). 미설정 시 LINE_Y_RATIO.
# - min_move / soft_margin_ratio: 지점별 라인 크로싱 민감도(선택).
# 매칭은 `get_camera_config`에서 CCTV 이름 부분문자열 중 **가장 긴 키** 우선 (남구리IC vs 구리IC 등).
# 런타임 덮어쓰기: data/camera_config_overrides.json (대시보드 /tuning/roi 또는 API)
_CAMERA_CONFIG_BASE: dict[str, dict[str, float]] = {
    "동의정부IC북측": {"top": 0.36, "left": 0.0, "width": 1.0, "line_y_ratio": 0.748, "min_move": 3, "soft_margin_ratio": 0.34},
    "수원신갈IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y_ratio": 0.726, "min_move": 2, "soft_margin_ratio": 0.36},
    "신갈분기점": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y_ratio": 0.755, "min_move": 2, "soft_margin_ratio": 0.38},
    "서평택분기점": {"top": 0.40, "left": 0.0, "width": 1.0, "line_y_ratio": 0.722, "min_move": 2, "soft_margin_ratio": 0.36},
    "자유로분기점": {"top": 0.37, "left": 0.0, "width": 1.0, "line_y_ratio": 0.714, "min_move": 2, "soft_margin_ratio": 0.36},
    "판교분기점": {"top": 0.37, "left": 0.05, "width": 0.92, "line_y_ratio": 0.784, "min_move": 2, "soft_margin_ratio": 0.40},
    "서울TG": {"top": 0.34, "left": 0.0, "width": 1.0, "line_y_ratio": 0.639, "min_move": 2, "soft_margin_ratio": 0.42},
    "용인IC": {"top": 0.40, "left": 0.0, "width": 1.0, "line_y_ratio": 0.722, "min_move": 2, "soft_margin_ratio": 0.36},
    "비봉IC": {"top": 0.41, "left": 0.0, "width": 1.0, "line_y_ratio": 0.717, "min_move": 3, "soft_margin_ratio": 0.34},
    "매송나들목": {"top": 0.43, "left": 0.0, "width": 1.0, "line_y_ratio": 0.644, "min_move": 3, "soft_margin_ratio": 0.32},
    "장항IC": {"top": 0.40, "left": 0.0, "width": 1.0, "line_y_ratio": 0.685, "min_move": 3, "soft_margin_ratio": 0.34},
    "일산IC": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y_ratio": 0.709, "min_move": 2, "soft_margin_ratio": 0.36},
    "양주IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y_ratio": 0.736, "min_move": 2, "soft_margin_ratio": 0.36},
    "의정부IC": {"top": 0.36, "left": 0.0, "width": 1.0, "line_y_ratio": 0.739, "min_move": 3, "soft_margin_ratio": 0.34},
    "남구리IC": {"top": 0.37, "left": 0.03, "width": 0.94, "line_y_ratio": 0.740, "min_move": 2, "soft_margin_ratio": 0.38},
    "중랑IC교": {"top": 0.39, "left": 0.04, "width": 0.92, "line_y_ratio": 0.736, "min_move": 2, "soft_margin_ratio": 0.38},
    "구리IC": {"top": 0.37, "left": 0.03, "width": 0.94, "line_y_ratio": 0.749, "min_move": 2, "soft_margin_ratio": 0.38},
    "신월IC": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y_ratio": 0.718, "min_move": 2, "soft_margin_ratio": 0.36},
    "김포IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y_ratio": 0.726, "min_move": 2, "soft_margin_ratio": 0.36},
    "서운분기점": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y_ratio": 0.736, "min_move": 2, "soft_margin_ratio": 0.38},
    "토평IC": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y_ratio": 0.727, "min_move": 2, "soft_margin_ratio": 0.36},
    "서하남IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y_ratio": 0.726, "min_move": 2, "soft_margin_ratio": 0.36},
    "상일IC": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y_ratio": 0.718, "min_move": 2, "soft_margin_ratio": 0.36},
    "시흥IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y_ratio": 0.736, "min_move": 2, "soft_margin_ratio": 0.36},
    "안현분기점": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y_ratio": 0.727, "min_move": 2, "soft_margin_ratio": 0.38},
}

CAMERA_CONFIG: dict[str, dict[str, float]] = dict(_CAMERA_CONFIG_BASE)

_config_overrides_mtime: float = 0.0


def camera_config_overrides_path() -> Path:
    return _ROOT / "data" / "camera_config_overrides.json"


def _load_camera_config_overrides() -> None:
    global _config_overrides_mtime, CAMERA_CONFIG
    path = camera_config_overrides_path()
    merged = {k: dict(v) for k, v in _CAMERA_CONFIG_BASE.items()}
    if not path.is_file():
        _config_overrides_mtime = 0.0
        CAMERA_CONFIG = merged
        return
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    if mtime == _config_overrides_mtime:
        return
    try:
        import json

        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for key, cfg in raw.items():
                if isinstance(cfg, dict):
                    patch = {str(k): float(v) for k, v in cfg.items()}
                    merged[str(key)] = {**merged.get(str(key), {}), **patch}
        _config_overrides_mtime = mtime
        CAMERA_CONFIG = merged
    except Exception as exc:
        logger.warning("camera_config_overrides.json 로드 실패: %s", exc)
        CAMERA_CONFIG = merged


def save_camera_config_override(cctv_key: str, patch: dict[str, float]) -> dict[str, float]:
    """지점 키(한글명)에 ROI/라인 패치를 저장하고 병합된 설정을 반환."""
    import json

    path = camera_config_overrides_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, dict] = {}
    if path.is_file():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                existing = {str(k): dict(v) for k, v in raw.items() if isinstance(v, dict)}
        except Exception:
            existing = {}
    base = dict(existing.get(cctv_key) or _CAMERA_CONFIG_BASE.get(cctv_key) or {})
    base.update({k: float(v) for k, v in patch.items()})
    existing[cctv_key] = base
    path.write_text(json.dumps(existing, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    global _config_overrides_mtime
    _config_overrides_mtime = 0.0
    _load_camera_config_overrides()
    return get_camera_config(cctv_key)


def list_camera_config_keys() -> list[str]:
    _load_camera_config_overrides()
    keys = set(_CAMERA_CONFIG_BASE.keys())
    keys.update(CAMERA_CONFIG.keys())
    return sorted(keys)

# 지점별 URL이 비어 있으면 `CCTV_URL` 값으로 채움(테스트용: 한 스트림으로 지점 이름만 바꿔 로테이션).
# DB `vehicle_count.cctv_name`에는 한글 지점명이 저장됩니다.
_FALLBACK_STREAM = (os.getenv("CCTV_URL", "") or "").strip()


def _u(key: str) -> str:
    return ((os.getenv(key, "") or "").strip() or _FALLBACK_STREAM)


CCTV_MULTICAST_SITES: list[tuple[str, str]] = [
    ("수원신갈IC", _u("CCTV_URL_SUWON_SINGAL_IC")),
    ("판교분기점", _u("CCTV_URL_PANGYO_JCT")),
    ("서울TG", _u("CCTV_URL_SEOUL_TG")),
    ("용인IC", _u("CCTV_URL_YONGIN_IC")),
    ("신갈분기점", _u("CCTV_URL_SINGAL_JCT")),
    ("서평택분기점", _u("CCTV_URL_SEOPYEONGTAEK_JCT")),
    ("비봉IC", _u("CCTV_URL_BIBONG_IC")),
    ("매송나들목", _u("CCTV_URL_MAESONG_IC")),
    ("장항IC", _u("CCTV_URL_JANGHANG_IC")),
    ("자유로분기점", _u("CCTV_URL_JAYURO_JCT")),
    ("일산IC", _u("CCTV_URL_ILSAN_IC")),
    ("양주IC", _u("CCTV_URL_YANGJU_IC")),
    ("의정부IC", _u("CCTV_URL_UIJEONGBU_IC")),
    ("동의정부IC북측", _u("CCTV_URL_EAST_UIJEONGBU_IC_NORTH")),
    ("남구리IC", _u("CCTV_URL_NAMGURI_IC")),
    ("구리IC", _u("CCTV_URL_GURI_IC")),
    ("중랑IC교", _u("CCTV_URL_JUNGNANG_IC_BRIDGE")),
    ("신월IC", _u("CCTV_URL_SINWOL_IC")),
    ("김포IC", _u("CCTV_URL_GIMPO_IC")),
    ("서운분기점", _u("CCTV_URL_SEOUN_JCT")),
    ("토평IC", _u("CCTV_URL_TOPYEONG_IC")),
    ("서하남IC", _u("CCTV_URL_SEOHANAM_IC")),
    ("상일IC", _u("CCTV_URL_SANGIL_IC")),
    ("시흥IC", _u("CCTV_URL_SIHEUNG_IC")),
    ("안현분기점", _u("CCTV_URL_ANHYEON_JCT")),
]

# 로테이션 지점명 (프론트 디버그 카드·`/preview-sites` 와 동일 순서 유지 권장)
CCTV_ROTATION_SITE_NAMES: tuple[str, ...] = tuple(n for n, _ in CCTV_MULTICAST_SITES)

logger = logging.getLogger(__name__)

# ITS 진단 결과 캐시(프리뷰·/rotation/status 초당 폴링 시 중복 호출 방지)
_idle_diag_expires_ts: float = 0.0
_idle_diag_cache: dict[str, str] | None = None

# get_effective_rotation_sites() 결과 캐시 — /rotation/status 폴링이
# 매번 ITS OpenAPI 를 호출하여 일일 한도를 더 소진하는 것을 막는다.
# 일반 TTL 은 짧게(60s), ITS 영구 오류(4001 한도 초과 등) 발생 시
# 긴 TTL(기본 1800s)로 한도 회복까지 호출을 차단한다.
_sites_cache_expires_ts: float = 0.0
_sites_cache_value: list[tuple[str, str]] | None = None
_sites_cache_error_code: str = ""
_disk_fallback_active: bool = False


def _load_disk_rotation_sites() -> list[tuple[str, str]]:
    try:
        from app.rotation_sites_cache import load_rotation_sites_cache

        return load_rotation_sites_cache()
    except Exception as exc:
        logger.debug("disk rotation cache unavailable: %s", exc)
        return []


def _fallback_disk_rotation_sites(
    disk_sites: list[tuple[str, str]],
    reason_code: str,
    raw_error: str = "",
) -> list[tuple[str, str]]:
    """ITS 실패 시 마지막으로 저장된 URL 목록으로 로테이션 유지."""
    global _sites_cache_value, _sites_cache_error_code, _sites_cache_expires_ts, _disk_fallback_active

    _sites_cache_value = list(disk_sites)
    _sites_cache_error_code = "disk_cache_fallback"
    _disk_fallback_active = True
    try:
        import time

        _sites_cache_expires_ts = time.monotonic() + _sites_success_cache_ttl_sec()
    except Exception:
        pass

    msg = f"{len(disk_sites)} sites from disk cache (ITS: {reason_code})"
    if raw_error:
        msg += f" — {raw_error[:120]}"
    logger.warning("get_effective_rotation_sites: %s", msg)
    return list(disk_sites)


def _its_autofetch_disabled() -> bool:
    """ITS_AUTOFETCH_DISABLED=1 이면 ITS 자동 조회를 호출하지 않는다(한도 보호용 비상 스위치)."""
    raw = (os.getenv("ITS_AUTOFETCH_DISABLED") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


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


def list_env_rotation_stream_sites() -> list[tuple[str, str]]:
    """환경변수로 유효 URL이 채워진 지점만(ITS 없이도 결정 가능)."""
    return [(n, u.strip()) for n, u in CCTV_MULTICAST_SITES if (u or "").strip()]


def get_yolo_imgsz_for_cctv(cctv_name: str) -> int:
    """지점별 예외 없이 공통 YOLO 입력 크기 사용."""
    return int(YOLO_IMGSZ)


def get_camera_config(cctv_name: str) -> dict[str, float]:
    """CCTV 이름에 매칭되는 ROI/line 튜닝. 부분문자열이 여러 개면 더 긴 지점명 키 우선."""
    _load_camera_config_overrides()
    name = cctv_name or ""
    best_key = ""
    best: dict[str, float] | None = None
    for key, cfg in CAMERA_CONFIG.items():
        if key in name and len(key) > len(best_key):
            best_key = key
            best = cfg
    return dict(best) if best else {}


def get_line_y_ratio_for_cctv(cctv_name: str) -> float:
    """지점별 예외 없이 공통 가상선 비율 사용."""
    return float(LINE_Y_RATIO)


def get_effective_rotation_sites() -> list[tuple[str, str]]:
    """환경변수로 URL이 하나라도 있으면 그 목록만 사용. 전부 비어 있으면 ITS API로 자동 조회.

    /rotation/status 폴링이 매번 ITS 를 두드리지 않도록 호출 결과를 짧게 캐시한다.
    ITS 한도 초과(4001) 등 영구성 오류는 더 긴 TTL 로 차단하여 한도 회복을 방해하지 않는다.
    """
    global _sites_cache_expires_ts, _sites_cache_value, _sites_cache_error_code

    env_sites = list_env_rotation_stream_sites()
    if env_sites:
        # 환경변수에 직접 URL 이 있으면 ITS 와 무관하므로 캐시도 필요 없음.
        return env_sites

    disk_sites = _load_disk_rotation_sites()
    if disk_sites and _its_autofetch_disabled():
        return disk_sites

    try:
        import time

        now = time.monotonic()
    except Exception:
        now = 0.0

    if _sites_cache_value is not None and now < _sites_cache_expires_ts:
        if _sites_cache_value:
            return list(_sites_cache_value)
        if disk_sites:
            return _fallback_disk_rotation_sites(disk_sites, "memory_cache_empty")
        return []

    if _its_autofetch_disabled():
        logger.info(
            "get_effective_rotation_sites: ITS_AUTOFETCH_DISABLED=1 → ITS 자동 조회 생략"
        )
        if disk_sites:
            return _fallback_disk_rotation_sites(disk_sites, "its_autofetch_disabled")
        _sites_cache_value = []
        _sites_cache_error_code = "its_autofetch_disabled"
        _sites_cache_expires_ts = now + _sites_error_cache_ttl_sec("its_autofetch_disabled")
        return []

    try:
        from app.its_rotation import resolve_rotation_sites_from_its
        from app.rotation_sites_cache import save_rotation_sites_cache

        sites = resolve_rotation_sites_from_its()
        if sites:
            save_rotation_sites_cache(sites, source="its")
        _sites_cache_value = list(sites)
        _sites_cache_error_code = "" if sites else "its_no_match"
        ttl = _sites_success_cache_ttl_sec() if sites else _sites_error_cache_ttl_sec("its_no_match")
        _sites_cache_expires_ts = now + ttl
        if sites:
            return list(sites)
        if disk_sites:
            return _fallback_disk_rotation_sites(disk_sites, "its_no_match")
        return []
    except Exception as exc:
        raw = str(exc).strip()
        code = _classify_its_error(raw)
        if disk_sites:
            return _fallback_disk_rotation_sites(disk_sites, code or "its_error", raw)
        _sites_cache_value = []
        _sites_cache_error_code = code or "its_error"
        _sites_cache_expires_ts = now + _sites_error_cache_ttl_sec(_sites_cache_error_code)
        logger.warning(
            "get_effective_rotation_sites: ITS/자동 조회 실패(%s, %ss 캐시): %s",
            _sites_cache_error_code,
            int(_sites_error_cache_ttl_sec(_sites_cache_error_code)),
            raw,
        )
        return []


def _sites_success_cache_ttl_sec() -> float:
    try:
        return max(5.0, float(os.getenv("ROTATION_SITES_CACHE_TTL_SEC", "60")))
    except ValueError:
        return 60.0


def _sites_error_cache_ttl_sec(code: str) -> float:
    """ITS 호출이 실패한 경우 캐시 TTL. 한도 초과·인증 오류는 길게 차단."""
    perm_codes = {"its_quota_exceeded", "its_unauthorized", "its_forbidden", "its_autofetch_disabled"}
    if code in perm_codes:
        try:
            return max(60.0, float(os.getenv("ROTATION_SITES_PERM_ERROR_TTL_SEC", "1800")))
        except ValueError:
            return 1800.0
    try:
        return max(5.0, float(os.getenv("ROTATION_SITES_ERROR_TTL_SEC", "60")))
    except ValueError:
        return 60.0


def last_rotation_sites_error_code() -> str:
    """최근 ITS 자동 조회 시 분류된 오류 코드(없으면 "")."""
    if _disk_fallback_active:
        return ""
    return _sites_cache_error_code or ""


def peek_idle_rotation_diagnose(*, ttl_sec: float = 90.0) -> dict[str, str]:
    """로테이션 URL이 없을 때 키 미설정·ITS 오류 등 원인 문자열 제공. 짧게 캐시.

    get_effective_rotation_sites() 의 캐시를 우선 참조하여 ITS 추가 호출을 만들지 않는다.
    """
    global _idle_diag_expires_ts, _idle_diag_cache

    try:
        import time

        now = time.monotonic()
    except Exception:
        now = 0.0

    if _idle_diag_cache is not None and now < _idle_diag_expires_ts:
        return dict(_idle_diag_cache)

    if list_env_rotation_stream_sites():
        _idle_diag_cache = {}
        _idle_diag_expires_ts = now + max(30.0, float(ttl_sec))
        return {}

    if _load_disk_rotation_sites():
        _idle_diag_cache = {}
        _idle_diag_expires_ts = now + max(30.0, float(ttl_sec))
        return {}

    key = (os.getenv("ITS_API_KEY") or "").strip()
    if _its_autofetch_disabled():
        _idle_diag_cache = {
            "code": "its_autofetch_disabled",
            "message": (
                "ITS_AUTOFETCH_DISABLED=1 로 ITS 자동 조회가 꺼져 있습니다. "
                ".env 에 CCTV_URL 또는 CCTV_URL_* 로 직접 스트림을 지정하거나, "
                "환경변수를 해제하고 ITS 한도가 회복된 뒤 재시도하세요."
            ),
        }
    elif not key:
        _idle_diag_cache = {
            "code": "missing_its_key",
            "message": (
                ".env 에 ITS_API_KEY 가 없거나, CCTV_URL 또는 CCTV_URL_* 변수에 실제 m3u8/rtsp 주소가 "
                "하나도 없습니다. 키를 넣거나 최소 CCTV_URL 하나로 테스트 로테이션을 켜 주세요."
            ),
        }
    else:
        # get_effective_rotation_sites() 의 캐시에 이미 분류된 오류가 있으면 그것을 사용 (ITS 재호출 X).
        cached_code = last_rotation_sites_error_code()
        if cached_code == "its_quota_exceeded":
            _idle_diag_cache = {
                "code": "its_quota_exceeded",
                "message": (
                    "ITS OpenAPI 호출 한도 초과(4001): 개인 일일/월간 제한량을 넘었습니다. "
                    "ITS 포털에서 잔여량을 확인하거나 한도 갱신 후 다시 시도하세요. "
                    "즉시 동작이 필요하면 .env 에 CCTV_URL(직접 m3u8/rtsp)을 넣거나 "
                    "ITS_AUTOFETCH_DISABLED=1 로 자동 조회를 잠시 끄세요."
                ),
            }
        elif cached_code == "its_unauthorized":
            _idle_diag_cache = {
                "code": "its_unauthorized",
                "message": (
                    "ITS OpenAPI 인증 실패: ITS_API_KEY 가 거부됐거나 사용처(사이트) 불일치입니다. "
                    "ITS 마이페이지에서 키와 ITS_REGISTERED_SITE_URL 을 확인하세요."
                ),
            }
        elif cached_code == "its_forbidden":
            _idle_diag_cache = {
                "code": "its_forbidden",
                "message": (
                    "ITS 접근 거부(403): IP 차단·호출 한도 초과 가능성이 있습니다. "
                    "ITS 안내 또는 CCTV_URL 직접 지정을 검토하세요."
                ),
            }
        else:
            try:
                matched = get_effective_rotation_sites()
                if not matched:
                    code_after = last_rotation_sites_error_code()
                    if code_after == "its_quota_exceeded":
                        _idle_diag_cache = {
                            "code": "its_quota_exceeded",
                            "message": (
                                "ITS OpenAPI 호출 한도 초과(4001): 개인 일일/월간 제한량을 넘었습니다. "
                                "ITS 포털에서 잔여량을 확인하거나 한도 갱신 후 다시 시도하세요. "
                                "즉시 동작이 필요하면 .env 에 CCTV_URL(직접 m3u8/rtsp)을 넣거나 "
                                "ITS_AUTOFETCH_DISABLED=1 로 자동 조회를 잠시 끄세요."
                            ),
                        }
                    elif code_after == "its_unauthorized":
                        _idle_diag_cache = {
                            "code": "its_unauthorized",
                            "message": (
                                "ITS OpenAPI 인증 실패: ITS_API_KEY 가 거부됐거나 사용처(사이트) 불일치입니다. "
                                "ITS 마이페이지에서 키와 ITS_REGISTERED_SITE_URL 을 확인하세요."
                            ),
                        }
                    elif code_after == "its_forbidden":
                        _idle_diag_cache = {
                            "code": "its_forbidden",
                            "message": (
                                "ITS 접근 거부(403): IP 차단·호출 한도 초과 가능성이 있습니다. "
                                "ITS 안내 또는 CCTV_URL 직접 지정을 검토하세요."
                            ),
                        }
                    else:
                        _idle_diag_cache = {
                            "code": "its_no_match",
                            "message": (
                                "ITS API 는 응답했으나 패턴에 맞는 CCTV URL 이 없습니다. "
                                "좌표(ITS_ROTATION_MINX 등)·its_rotation 패턴을 확인하세요."
                            ),
                        }
                else:
                    _idle_diag_cache = {}
            except Exception as exc:
                raw = str(exc).strip()
                code = _classify_its_error(raw) or "its_error"
                prefix = "" if raw.startswith("ITS ") else "ITS 자동 조회 실패: "
                _idle_diag_cache = {"code": code, "message": prefix + raw}

    _idle_diag_expires_ts = now + max(30.0, float(ttl_sec))
    return dict(_idle_diag_cache) if _idle_diag_cache else {}


def invalidate_idle_rotation_diagnose() -> None:
    """진단·로테이션 사이트 캐시를 모두 무효화 (로테이션 수동 시작 후 즉시 갱신용)."""
    global _idle_diag_expires_ts, _idle_diag_cache
    global _sites_cache_expires_ts, _sites_cache_value, _sites_cache_error_code, _disk_fallback_active
    _idle_diag_expires_ts = 0.0
    _idle_diag_cache = None
    _sites_cache_expires_ts = 0.0
    _sites_cache_value = None
    _sites_cache_error_code = ""
    _disk_fallback_active = False
    try:
        from app.its_client import invalidate_cctv_list_cache

        invalidate_cctv_list_cache()
    except ImportError:
        pass


def _refresh_skip_when_env_urls_enabled() -> bool:
    """CCTV_URL/CCTV_URL_* 가 있으면 refresh 가 ITS 를 호출하지 않도록(기본 on)."""
    raw = (os.getenv("ITS_REFRESH_SKIP_WHEN_ENV_URLS") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def refresh_rotation_sites_from_its(
    *,
    force: bool = False,
    force_its: bool = False,
) -> dict[str, object]:
    """ITS에서 지점 URL을 다시 받아 디스크 캐시에 저장. force=True면 메모리 캐시 무시.

    force_its=False 이고 .env 에 스트림 URL 이 있으면 ITS 호출을 건너뜀(한도 보호).
    """
    if force:
        invalidate_idle_rotation_diagnose()

    env_sites = list_env_rotation_stream_sites()
    if not force_its and _refresh_skip_when_env_urls_enabled() and env_sites:
        cached_code = last_rotation_sites_error_code()
        msg = (
            ".env 에 CCTV_URL 또는 CCTV_URL_* 가 설정되어 있어 ITS refresh 를 건너뛰었습니다. "
            "로테이션은 환경변수 URL 을 사용 중입니다."
        )
        if cached_code == "its_quota_exceeded":
            msg += (
                " ITS OpenAPI 한도 초과(4001) 상태로 보입니다. 한도 회복 전까지 refresh 를 반복하지 마세요. "
                "디스크 캐시만 갱신하려면 force_its=1 로 강제 호출하세요."
            )
        return {
            "ok": True,
            "skipped": True,
            "reason": "env_urls_configured",
            "message": msg,
            "sites": len(env_sites),
            "names": [n for n, _ in env_sites],
            "its_error_code": cached_code or None,
        }

    if _its_autofetch_disabled():
        return {
            "ok": False,
            "skipped": True,
            "reason": "its_autofetch_disabled",
            "error": (
                "ITS_AUTOFETCH_DISABLED=1 로 ITS refresh 가 차단되었습니다. "
                "CCTV_URL 을 사용하거나 환경변수를 해제한 뒤 다시 시도하세요."
            ),
            "sites": 0,
        }

    try:
        from app.its_rotation import resolve_rotation_sites_from_its
        from app.rotation_sites_cache import save_rotation_sites_cache

        sites = resolve_rotation_sites_from_its()
        if sites:
            save_rotation_sites_cache(sites, source="its")
        invalidate_idle_rotation_diagnose()
        return {
            "ok": bool(sites),
            "skipped": False,
            "sites": len(sites),
            "names": [n for n, _ in sites],
        }
    except Exception as exc:
        return {"ok": False, "skipped": False, "error": str(exc), "sites": 0}
