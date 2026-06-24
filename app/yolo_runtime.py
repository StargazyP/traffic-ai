"""YOLO model load + multi-worker CCTV sharding."""

from __future__ import annotations

import logging
import os
import zlib

logger = logging.getLogger(__name__)


def get_yolo_worker_count() -> int:
    try:
        return max(1, min(4, int(os.getenv("YOLO_WORKER_COUNT", "2"))))
    except ValueError:
        return 2


def cctv_yolo_worker_slot(cctv_name: str, worker_count: int) -> int:
    """지점명을 워커 수로 고정 분배(한 CCTV는 항상 같은 워커만 처리)."""
    n = max(1, int(worker_count))
    return zlib.adler32((cctv_name or "").encode("utf-8")) % n


def load_yolo_model(worker_id: int = 0):
    """워커별 YOLO 인스턴스. GPU 사용 가능 시 cuda:0."""
    from ultralytics import YOLO

    from app.config import MODEL_PATH, USE_CUDA

    m = YOLO(MODEL_PATH)
    device = "cpu"
    if USE_CUDA:
        try:
            import torch

            if torch.cuda.is_available():
                m.to("cuda:0")
                device = f"cuda:0 ({torch.cuda.get_device_name(0)})"
            else:
                logger.warning("YOLO worker %s: USE_CUDA=1 but torch.cuda.is_available()=False", worker_id)
        except Exception as exc:
            logger.warning("YOLO worker %s CUDA init failed: %s", worker_id, exc)
    logger.info("YOLO worker %s ready on %s", worker_id, device)
    return m


def cuda_status() -> dict[str, str | bool]:
    try:
        import torch

        avail = bool(torch.cuda.is_available())
        name = torch.cuda.get_device_name(0) if avail else ""
        return {"cuda_available": avail, "cuda_device": name}
    except Exception:
        return {"cuda_available": False, "cuda_device": ""}
