# traffic-ai

국가 ITS·CCTV HLS/RTSP 영상에서 **차량을 실시간 추적·집계**하고, 대시보드와 REST API로 교통량을 제공하는 서비스입니다.

**Live:** [https://stargazyp.com/traffic/](https://stargazyp.com/traffic/)

Repository: [github.com/StargazyP/traffic-ai](https://github.com/StargazyP/traffic-ai)

---

## 주요 기능

- **다중 CCTV 순환 분석** — 서울 inbound 등 여러 지점을 GPU 워커로 순환하며 YOLO 추론
- **차량 추적·카운팅** — YOLOv8 + ByteTrack, ROI 가상선 기준 상행/하행·hard/soft 집계
- **실시간 대시보드** — WebSocket으로 bbox·카운트·세그먼트 메타데이터 전송 (영상 스트림 미전송)
- **HLS 프록시** — ITS/K-T style Referer·쿠키 이슈 완화용 `/hls` 내장 프록시
- **ROI 튜닝 UI** — [https://stargazyp.com/tuning/](https://stargazyp.com/tuning/) 에서 카메라별 ROI·가상선 보정
- **교통량 REST API** — `traffic_data_api` 사이드카 (`/traffic-api/` 경로, portfolio 게이트웨이 연동)
- **MySQL 적재** — 분 단위 배치 insert, `vehicle_count_hourly` 시간대 롤업

## 사용 시나리오

1. 브라우저에서 Live 대시보드 접속 → 현재 순환 중인 CCTV와 실시간 검출·카운트 확인
2. ROI/가상선 조정 후 rotation 재시작 → 집계 정확도 개선
3. REST API로 사이트·방향별 시간대 교통량 조회 (cctv-map 등 연동)

## 서버 구성 (요약)

| 구성 | 기술 |
|------|------|
| 앱 | Python 3.11, FastAPI, Uvicorn |
| AI | YOLOv8, ByteTrack, FFmpeg (HLS/RTSP 디코드) |
| DB | MySQL 8 |
| 배포 | Docker Compose (별도 스택), portfolio nginx `/traffic/` · `/traffic-api/` |

로컬 실행: `.env.example` → `.env` 복사 후 `docker compose up -d --build`. API 키·DB 비밀번호는 저장소에 올리지 마세요.

## Changelog

- **2026-06-24** — README 기능 중심 정리, portfolio webhook CI/CD 연동.
- **2026-05-27** — ROI calibration, rotation health, ITS guard, `traffic_data_api`, env 분리.

## Compliance

ITS OpenAPI 및 제3자 CCTV 스트림 이용 시 각 제공자의 약관·키 정책을 준수하세요.
