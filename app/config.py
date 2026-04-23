import os
from pathlib import Path

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
# 하남 등 원거리 지점: 별도 확대 (기본 1280). CCTV 이름에 "하남" 포함 시 적용
try:
    YOLO_IMGSZ_HANAM = max(320, min(1280, int(os.getenv("YOLO_IMGSZ_HANAM", "1280"))))
except ValueError:
    YOLO_IMGSZ_HANAM = 1280

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

# 지점별 가상선 위치 오버라이드 (예: 하남분기점은 시야 특성상 별도 튜닝)
_line_y_hanam_raw = (os.getenv("LINE_Y_RATIO_HANAM", "0.341") or "").strip()
try:
    _line_y_hanam = float(_line_y_hanam_raw) if _line_y_hanam_raw else LINE_Y_RATIO
except ValueError:
    _line_y_hanam = LINE_Y_RATIO
LINE_Y_RATIO_HANAM = min(0.95, max(0.05, _line_y_hanam))

# 지점별 URL이 비어 있으면 `CCTV_URL` 값으로 채움(테스트용: 한 스트림으로 5지점 이름만 바꿔 로테이션).
# DB `vehicle_count.cctv_name`에는 한글 지점명이 저장됩니다.
_FALLBACK_STREAM = (os.getenv("CCTV_URL", "") or "").strip()


def _u(key: str) -> str:
    return ((os.getenv(key, "") or "").strip() or _FALLBACK_STREAM)


CCTV_MULTICAST_SITES: list[tuple[str, str]] = [
    ("판교분기점", _u("CCTV_URL_PANGYO")),
    ("하남분기점", _u("CCTV_URL_HANAM")),
    ("서창분기점", _u("CCTV_URL_SEOCHANG")),
    ("김포", _u("CCTV_URL_GIMPO")),
    ("광명", _u("CCTV_URL_GWANGMYEONG")),
]


def get_yolo_imgsz_for_cctv(cctv_name: str) -> int:
    """지점별 추론 입력 크기. 하남(원거리)은 더 큰 imgsz로 소형 차량 검출에 유리."""
    name = (cctv_name or "").strip()
    if "하남" in name:
        return int(YOLO_IMGSZ_HANAM)
    return int(YOLO_IMGSZ)


def get_line_y_ratio_for_cctv(cctv_name: str) -> float:
    """지점별 가상선 비율. 하남은 별도 환경변수로 미세 조정 가능."""
    name = (cctv_name or "").strip()
    if "하남" in name:
        return float(LINE_Y_RATIO_HANAM)
    return float(LINE_Y_RATIO)


def get_effective_rotation_sites() -> list[tuple[str, str]]:
    """환경변수로 URL이 하나라도 있으면 그 목록만 사용. 전부 비어 있으면 ITS API로 5지점 URL 자동 조회."""
    env_sites = [(n, u.strip()) for n, u in CCTV_MULTICAST_SITES if (u or "").strip()]
    if env_sites:
        return env_sites
    try:
        from app.its_rotation import resolve_rotation_sites_from_its

        return resolve_rotation_sites_from_its()
    except Exception:
        return []
