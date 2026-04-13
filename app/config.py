import os

# 예: export CCTV_URL="https://....m3u8"
CCTV_URL = os.getenv("CCTV_URL", "여기에 m3u8 URL")
MODEL_PATH = os.getenv("MODEL_PATH", "models/yolov8n.pt")

# 성능 관련 설정
FRAME_WIDTH = int(os.getenv("FRAME_WIDTH", "640"))
FRAME_HEIGHT = int(os.getenv("FRAME_HEIGHT", "360"))
FRAME_SKIP = int(os.getenv("FRAME_SKIP", "2"))  # 2면 2프레임 중 1프레임만 추론

# YOLO 설정
IMG_SIZE = int(os.getenv("IMG_SIZE", "640"))
CONF_THRES = float(os.getenv("CONF_THRES", "0.25"))
USE_CUDA = os.getenv("USE_CUDA", "true").lower() in {"1", "true", "yes"}
