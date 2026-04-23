"""Traffic AI FastAPI entrypoint: vendor shim, YOLO load, HTTP/WS routes."""

from __future__ import annotations

from fastapi import FastAPI

from app import state
from app.hls_proxy import router as hls_proxy_router
from app.lifecycle import lifespan
from app.routes import router as api_router
from app.vendor_paths import ensure_websockets_vendor_on_path

ensure_websockets_vendor_on_path()
state.load_yolo_model()

app = FastAPI(title="Traffic AI CCTV Viewer", lifespan=lifespan)
app.include_router(hls_proxy_router)
app.include_router(api_router)
