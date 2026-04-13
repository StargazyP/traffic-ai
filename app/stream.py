import cv2

from app.config import FRAME_HEIGHT, FRAME_SKIP, FRAME_WIDTH


def get_stream(url: str):
    cap = cv2.VideoCapture(url)
    count = 0

    while True:
        ret, frame = cap.read()

        # CCTV 끊김 시 재연결
        if not ret:
            cap.release()
            cap = cv2.VideoCapture(url)
            continue

        count += 1
        if FRAME_SKIP > 1 and (count % FRAME_SKIP != 0):
            continue

        # 추론/전송 부하 감소
        frame = cv2.resize(frame, (FRAME_WIDTH, FRAME_HEIGHT))
        yield frame
