from ultralytics import YOLO

from app.config import CONF_THRES, IMG_SIZE, MODEL_PATH, USE_CUDA

model = YOLO(MODEL_PATH)
if USE_CUDA:
    try:
        model.to("cuda")
    except Exception:
        # CUDA 사용 불가 환경이면 CPU로 자동 폴백
        pass


def detect(frame):
    """
    차량(car, bus, truck) 탐지 시 첫 번째 박스를 반환.
    반환: (detected: bool, box: tuple | None)
    """
    results = model(frame, imgsz=IMG_SIZE, conf=CONF_THRES, verbose=False)

    for r in results:
        for box in r.boxes:
            cls = int(box.cls[0])
            # COCO: car=2, bus=5, truck=7
            if cls in [2, 5, 7]:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                return True, (x1, y1, x2, y2)

    return False, None
