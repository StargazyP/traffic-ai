import cv2
from ultralytics import YOLO

model = YOLO("yolov8n.pt")

cap = cv2.VideoCapture("test.mp4")  # 영상 넣기

frame_skip = 2
count = 0

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    count += 1
    if count % frame_skip != 0:
        continue

    results = model(frame, imgsz=416)

    annotated = results[0].plot()

    cv2.imshow("result", annotated)

    if cv2.waitKey(1) == 27:
        break

cap.release()
cv2.destroyAllWindows()
