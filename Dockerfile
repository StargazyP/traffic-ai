FROM python:3.11-slim

WORKDIR /workspace

# torch/ultralytics runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /workspace/requirements.txt

# ultralytics + CUDA PyTorch (GTX 1650 등 NVIDIA GPU용)
RUN pip install --no-cache-dir -r /workspace/requirements.txt \
    && pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cu124

COPY . /workspace

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
