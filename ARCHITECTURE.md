# traffic-ai 아키텍처 문서

## 1) 프로젝트 목적
- 다중 CCTV 스트림(주로 ITS HLS)을 순차 처리하며 차량 객체를 추적한다.
- 가상선(line crossing) 기반으로 상행/하행 카운트를 계산한다.
- 카운트 결과를 MySQL에 배치 저장하고, 브라우저에는 WebSocket으로 실시간 전송한다.

---

## 2) 전체 구조(요약)

```text
[Browser]
  ├─ GET /                -> 기본 대시보드(UI + JS)
  ├─ POST /rotation/start -> 다중 CCTV 순차 추론 시작
  ├─ WS   /ws             -> detection/segment 이벤트 수신
  └─ GET /hls/play/{tid}  -> HLS 프록시 스트림 재생

[FastAPI app.main]
  ├─ Rotation Orchestrator (thread)
  │   ├─ ffmpeg ingest threads (CCTV별 혹은 단일 ingest 공유)
  │   └─ YOLO track + line crossing count
  ├─ Single counter mode (thread)
  │   └─ yolo_mysql_counter.run_counter_stream
  ├─ EventBus (WS broadcast)
  ├─ ITS client (CCTV 목록/API)
  └─ HLS Proxy router (/hls/*)

[Persistence]
  └─ MySQL (vehicle_count 테이블, 배치 insert)
```

---

## 3) 핵심 소스코드와 역할

### `app/main.py` (메인 서버 + 오케스트레이션)
- FastAPI 앱 엔트리이며 API/WS/UI를 모두 제공한다.
- 핵심 상태:
  - `count_status`: 현재 카운트 및 로그 상태
  - `detection_status`: 박스/프레임/지점 정보
  - `rotation_telemetry`: 로테이션 추론 진단 정보
- 핵심 동작:
  - `/rotation/start`, `/rotation/stop`, `/rotation/status`
  - `/ws`로 detection/segment 이벤트 push
  - 내부 스레드 `_sequential_cctv_loop()`에서 다중 CCTV 순차 추론 수행
- 특징:
  - 영상 바이너리를 WS로 보내지 않고, 박스/카운트 메타데이터만 전송한다.
  - FastAPI 이벤트 루프와 백그라운드 스레드를 연결하기 위해 `run_coroutine_threadsafe`를 사용한다.

### `yolo_mysql_counter.py` (추론 + 카운팅 + DB 배치 저장)
- `YOLO("yolov8n.pt")` 모델 로드 후 `model.track(..., tracker="bytetrack.yaml")` 기반 추적.
- `ffmpeg` subprocess로 스트림을 디코드하여 numpy frame generator 생성.
- line crossing 조건:
  - 이전 y 좌표와 현재 y 좌표가 가상선을 교차하고, 이동량이 최소치 이상일 때 카운트.
- DB 저장:
  - `batch_buffer`에 누적 후 `BATCH_SIZE` 단위 `insert_batch()` 호출.
- 제공 함수:
  - `run_counter()` : 단일 CCTV 카운터 실행
  - `run_counter_stream()` : 이벤트 콜백(on_event/on_detections) 제공 모드

### `db_mysql.py` (영속화 계층)
- MySQL 연결정보(`MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`)를 env로 관리.
- `insert_batch(rows)`는 스키마 유연성을 가진다.
  - `up_count/down_count` 컬럼이 있으면 분리 저장
  - 없으면 `count=up+down`으로 fallback 저장

### `event_bus.py` (실시간 전달)
- 연결된 WebSocket 클라이언트 리스트를 유지하고 `broadcast()` 수행.
- 예외 발생 소켓을 정리하여 브로드캐스트 안정성을 확보한다.

### `app/hls_proxy.py` (브라우저 재생용 HLS 프록시)
- 브라우저에서 직접 접근 시 발생 가능한 403/Referer 이슈를 서버 프록시로 해결.
- `/hls/register`로 원본 URL을 등록하고 `/hls/play/{tid}`로 재생.
- m3u8 내부 URI를 재작성하여 세그먼트 요청도 프록시 경유.
- Referer 후보 순회, Session 쿠키 유지 등 KT CDN 특성을 처리.

### `app/its_client.py` + `app/its_rotation.py` (CCTV 소스 자동 수집)
- ITS OpenAPI(`cctvInfo`)에서 CCTV 목록/URL 조회.
- 로테이션 목표 지점(판교/하남/서창/김포/광명)을 패턴 매칭으로 자동 선택.
- env에 지점별 URL이 없을 때 자동 조회 fallback으로 동작.

### `app/config.py` (설정 집약)
- `.env` 자동 로드 후 주요 런타임 옵션 노출:
  - 스트림 URL(`CCTV_URL`, 지점별 URL, `YOLO_INGEST_URL`)
  - 추론 성능(`FRAME_SKIP`, `YOLO_TRACK_CONF`, `LINE_Y_RATIO`)
  - 로테이션 주기(`CCTV_ROTATION_SEC`)
  - HLS 프록시 헤더/허용 호스트

---

## 4) 주요 실행 시나리오

### A. 로테이션 모드(주 사용 경로)
1. 브라우저가 `/rotation/start` 호출
2. 서버가 `_sequential_cctv_loop()` 스레드 시작
3. ffmpeg 워커가 각 CCTV(또는 단일 ingest URL) 프레임 버퍼 갱신
4. 현재 로테이션 지점 프레임에 대해 YOLO 추론/추적 수행
5. 가상선 교차 시 up/down 및 global count 갱신
6. 배치 버퍼에 저장 요청 누적 후 MySQL insert
7. `/ws`로 `{type: "segment"}` 또는 `{type: "detection"}` 전송
8. 프론트가 카운트/지점/로그를 실시간 반영

### B. 단일 카운터 모드
1. `/start-count?id=...` 호출
2. 선택 CCTV URL을 `run_counter_stream()`에 전달
3. 이벤트 콜백으로 `count_status`/`detection_status` 업데이트
4. `/stop-count`로 해당 스레드 정지

### C. 브라우저 HLS 미리보기
1. 프론트가 `/preview-sites`로 지점 목록 조회
2. 특정 지점 선택 후 `/hls/register` 호출
3. `play_url` 수신 후 `/hls/play/{tid}` 스트림 재생
4. m3u8 내부 세그먼트도 프록시 경유

---

## 5) 데이터 흐름 상세

### 추론/카운트 파이프라인
`CCTV URL` -> `ffmpeg raw frame` -> `YOLO track(ByteTrack)` -> `track_id별 y 이력` -> `line crossing 판단` -> `up/down/global count 갱신` -> `DB batch insert` + `WS detection 이벤트`

### 프론트 동기화
- polling:
  - `/count-status` (카운트/로그)
  - `/rotation/status` (시퀀서/추론 텔레메트리)
- push:
  - `/ws` (지점 전환, 박스, 카운트, timestamp)

---

## 6) 스레드/동시성 모델
- 메인: FastAPI async loop
- 백그라운드:
  - 로테이션 시퀀서 스레드 1개
  - ffmpeg ingest 스레드 N개(지점 수 또는 1개 공유)
  - 단일 카운터 모드 스레드 1개(옵션)
- 동기화:
  - `threading.Lock`으로 상태 객체 보호
  - stop 이벤트(`threading.Event`)로 안전 종료
  - 스레드 -> async 브로드캐스트는 `run_coroutine_threadsafe` 사용

---

## 7) 설정 포인트(.env)
- 필수/핵심
  - `ITS_API_KEY`
  - `CCTV_URL` 또는 `CCTV_URL_PANGYO` ... `CCTV_URL_GWANGMYEONG`
- 추론/카운트 튜닝
  - `FRAME_SKIP`, `YOLO_TRACK_CONF`, `LINE_Y_RATIO`
  - `CCTV_ROTATION_SEC`, `YOLO_TRACK_IOU`, `MIN_MOVE`
- 스트림/프록시
  - `YOLO_INGEST_URL`
  - `HLS_PROXY_REFERER`, `HLS_PROXY_ORIGIN`, `HLS_PROXY_ALLOWED_HOSTS`
- DB
  - `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`

---

## 8) 배포/런타임
- 컨테이너: `Dockerfile`
  - 베이스 `python:3.11-slim`
  - 필수 런타임 `ffmpeg` 설치
  - `uvicorn app.main:app --host 0.0.0.0 --port 8000`
- 모델 파일:
  - 기본 `yolov8n.pt` 사용 (`yolo_mysql_counter.py`)

---

## 9) 현재 구조의 장점과 주의점

### 장점
- 영상 전송과 추론 이벤트를 분리하여 브라우저 부담이 낮다.
- 로테이션/단일 카운터 두 모드를 모두 지원한다.
- HLS 프록시가 있어 실제 CCTV CDN 제약(403/Referer)에 대응 가능하다.
- DB 스키마 호환(fallback)으로 운영 전환 부담이 낮다.

### 주의점
- 모델 로드 경로가 이원화되어 있음:
  - `app/yolo.py`는 `MODEL_PATH` 기반
  - `yolo_mysql_counter.py`는 `"yolov8n.pt"` 하드코딩
- CPU/GPU/스트림 품질에 따라 추론 FPS 변동이 크므로 `FRAME_SKIP`, 로테이션 주기 튜닝이 필요하다.
- CCTV 소스(ITS API, m3u8 토큰) 만료 특성상 프록시/재등록 흐름을 운영에서 주기적으로 확인해야 한다.

---

## 10) 빠른 코드 진입 순서(읽기 추천)
1. `app/main.py` : 전체 제어 흐름
2. `yolo_mysql_counter.py` : 실제 추론/카운트 로직
3. `app/config.py` : 런타임 설정/환경변수
4. `app/hls_proxy.py` : 스트림 프록시 동작
5. `db_mysql.py` : 저장 스키마 처리
6. `app/its_client.py`, `app/its_rotation.py` : CCTV 자동 매핑

