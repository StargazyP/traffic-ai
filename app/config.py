import os
from pathlib import Path

# [2026-04-28] 서울 유입 핵심 축 로테이션 지점으로 확정.
# 프로젝트 루트의 .env 자동 로드 (없으면 무시)
try:
    from dotenv import load_dotenv

    _ROOT = Path(__file__).resolve().parent.parent
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

# 가상선 세로 위치 (프레임 높이 × 비율). .env 예: LINE_Y_RATIO=0.6
_line_y = float(os.getenv("LINE_Y_RATIO", "0.6"))
LINE_Y_RATIO = min(0.95, max(0.05, _line_y))

# 서울 유입 17지점 YOLO ROI·가상선 특화.
# - top/left/width: 전 프레임 대비 세로 크롭 시작 비율·가로 시작·가로 폭 (0~1).
# - line_y: ROI 크롭 **내부** 픽셀 y (가상선). 미설정 시 LINE_Y_RATIO로 산출.
# 매칭은 `get_camera_config`에서 CCTV 이름 부분문자열 중 **가장 긴 키** 우선 (남구리IC vs 구리IC 등).
CAMERA_CONFIG: dict[str, dict[str, float]] = {
    "동의정부IC북측": {"top": 0.36, "left": 0.0, "width": 1.0, "line_y": 172},
    "수원신갈IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y": 162},
    "신갈분기점": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y": 166},
    "서평택분기점": {"top": 0.40, "left": 0.0, "width": 1.0, "line_y": 156},
    "자유로분기점": {"top": 0.37, "left": 0.0, "width": 1.0, "line_y": 162},
    "판교분기점": {"top": 0.37, "left": 0.05, "width": 0.92, "line_y": 178},
    "서울TG": {"top": 0.34, "left": 0.0, "width": 1.0, "line_y": 152},
    "용인IC": {"top": 0.40, "left": 0.0, "width": 1.0, "line_y": 156},
    "비봉IC": {"top": 0.41, "left": 0.0, "width": 1.0, "line_y": 152},
    "매송나들목": {"top": 0.43, "left": 0.0, "width": 1.0, "line_y": 132},
    "장항IC": {"top": 0.40, "left": 0.0, "width": 1.0, "line_y": 148},
    "일산IC": {"top": 0.39, "left": 0.0, "width": 1.0, "line_y": 156},
    "양주IC": {"top": 0.38, "left": 0.0, "width": 1.0, "line_y": 164},
    "의정부IC": {"top": 0.36, "left": 0.0, "width": 1.0, "line_y": 170},
    "남구리IC": {"top": 0.37, "left": 0.03, "width": 0.94, "line_y": 168},
    "중랑IC교": {"top": 0.39, "left": 0.04, "width": 0.92, "line_y": 162},
    "구리IC": {"top": 0.37, "left": 0.03, "width": 0.94, "line_y": 170},
}

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
]

# 로테이션 지점명 (프론트 디버그 카드·`/preview-sites` 와 동일 순서 유지 권장)
CCTV_ROTATION_SITE_NAMES: tuple[str, ...] = tuple(n for n, _ in CCTV_MULTICAST_SITES)


def get_yolo_imgsz_for_cctv(cctv_name: str) -> int:
    """지점별 예외 없이 공통 YOLO 입력 크기 사용."""
    return int(YOLO_IMGSZ)


def get_camera_config(cctv_name: str) -> dict[str, float]:
    """CCTV 이름에 매칭되는 ROI/line 튜닝. 부분문자열이 여러 개면 더 긴 지점명 키 우선."""
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
    """환경변수로 URL이 하나라도 있으면 그 목록만 사용. 전부 비어 있으면 ITS API로 자동 조회."""
    env_sites = [(n, u.strip()) for n, u in CCTV_MULTICAST_SITES if (u or "").strip()]
    if env_sites:
        return env_sites
    try:
        from app.its_rotation import resolve_rotation_sites_from_its

        return resolve_rotation_sites_from_its()
    except Exception:
        return []
