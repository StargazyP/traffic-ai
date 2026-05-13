"""Large static HTML payloads for dashboard routes."""
# DEFAULT_DEBUG_CCTVS 순서·이름은 app.config.CCTV_MULTICAST_SITES 지점명과 맞출 것.

from __future__ import annotations


def traffic_live_html() -> str:
    return """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>Traffic AI</title>
</head>
<body>
  <h2>실시간 차량 카운트 (WebSocket)</h2>
  <div id="data"></div>
  <script>
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(proto + "//" + location.host + "/ws");
    ws.onopen = () => {
      setInterval(() => {
        try {
          if (ws.readyState === WebSocket.OPEN) ws.send("ping");
        } catch (_) {}
      }, 20000);
    };
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === "segment") {
        document.getElementById("data").innerHTML =
          "<p>지점 전환: " + (data.cctv ?? "") + "</p>";
        return;
      }
      if (data.type !== "detection") return;
      document.getElementById("data").innerHTML =
        "<p>CCTV: " + (data.cctv ?? "") + "<br>" +
        "Count: " + (data.count ?? 0) + "<br>" +
        "시간: " + (data.timestamp ?? "") + "</p>";
    };
    ws.onerror = () => {
      document.getElementById("data").textContent = "WebSocket 연결 오류";
    };
  </script>
</body>
</html>"""


def index_html() -> str:
    return """
    <html>
    <head><title>Traffic AI — 서울 유입 CCTV 로테이션</title></head>
    <body>
      <h2>서울 유입 CCTV 순차 캡처 (YOLO)</h2>
      <p style="color:#555;max-width:720px;">
        상단 영상은 <strong>하단에서 지점 선택 후 [보기]</strong>로 재생합니다. KT/ITS m3u8은 동일 출처 <code>/hls</code> 프록시를 거칩니다. YOLO·카운트는 서버 OpenCV·<code>WebSocket</code>과 별개입니다.
      </p>
      <details style="max-width:920px;margin:12px 0 20px;padding:14px 16px;border:1px solid #ccc;border-radius:8px;background:#f8f9fa;">
        <summary style="cursor:pointer;font-weight:600;color:#222;">서버 캡처·YOLO 파이프라인 (현재 아키텍처 상세)</summary>
        <div style="margin-top:14px;font-size:13px;line-height:1.65;color:#333;max-width:900px;">
          <p><strong>1) 로테이션 시작 시</strong><br/>
          지점 목록(<code>get_effective_rotation_sites</code>)마다 <strong>FFmpeg 전용 스레드</strong>를 띄웁니다. CCTV별 <code>frame_queues</code>·<code>trackers</code>(YOLO 인스턴스)는 세션마다 비운 뒤 다시 채워집니다. 별도 <strong>시퀀서 스레드</strong>는 <code>CCTV_ROTATION_SEC</code>마다 현재 지점만 바며 <code>type: segment</code> WebSocket만 보냅니다(브라우저 HLS 미리보기와 무관).</p>
          <p><strong>2) FFmpeg 인입 (지점당 1스레드)</strong><br/>
          HLS/RTSP URL을 읽어 <code>rawvideo bgr24</code>로 디코드합니다. 필터: <code>fps=5,scale=FRAME_WIDTH×FRAME_HEIGHT</code>(<code>app.config</code>). 끊기면 지수 백오프 후 프로세스 재시작합니다.</p>
          <p><strong>3) CCTV별 프레임 큐 (짧은 버퍼)</strong><br/>
          각 지점 이름으로 <code>Queue(maxsize=YOLO_FRAME_QUEUE_SIZE)</code>(기본 5)를 두고, 디코드된 프레임·캡처 시각 <code>ts</code>·CCTV명을 넣습니다. 큐가 가득 차면 가장 오래된 프레임을 제거해 최신 프레임을 유지합니다.</p>
          <p><strong>4) 단일 <code>yolo_worker</code> 스레드 (GPU 1개 공유)</strong><br/>
          • <code>frame_queues</code> 키 목록은 리비전이 바뀔 때만 갱신합니다.<br/>
          • 큐에 프레임이 있는 CCTV만 <code>ready_cctvs</code>로 모읍니다. 비어 있으면 짧게 sleep합니다.<br/>
          • <code>YOLO_SAMPLE_INTERVAL</code>이 지난 CCTV만 <code>eligible</code>로 남깁니다(지점별 샘플링, 빈 <code>get(timeout)</code> 낭비 감소).<br/>
          • 라운드로빈으로 한 지점을 고른 뒤 큐에서 프레임 1개를 꺼내 순차 처리합니다(중간 프레임 보존).<br/>
          • <code>YOLO_FRAME_MAX_AGE_SEC</code>보다 오래된 <code>ts</code>면 추론을 건너뜁니다.</p>
          <p><strong>5) ROI → YOLO 추론</strong><br/>
          전체 프레임 높이 <code>h</code>에서 <code>roi_y0 = int(h × YOLO_ROI_TOP_RATIO)</code>를 기본으로 사용합니다. 가상선은 <code>line_y_global - roi_y0</code>로 ROI 좌표계에 맞춰 계산합니다.</p>
          <p><strong>6) YOLO detect + CCTV별 ByteTrack</strong><br/>
          YOLO는 전역 단일 모델(<code>model = YOLO(MODEL_PATH)</code>)로 detect만 수행하고, 지점별 상태는 <code>get_tracker(cctv_name)</code>의 ByteTrack 인스턴스가 유지합니다. 검출 결과(<code>xyxy/conf/cls</code>)를 트래커 입력으로 변환해 <code>tracker.update(...)</code>를 호출하며, 트랙 id는 ByteTrack이 생성합니다. <code>imgsz</code>는 <code>YOLO_IMGSZ</code>(기본 960)를 사용합니다.</p>
          <p><strong>7) 검출/트랙 처리</strong><br/>
          <code>boxes_obj</code>가 없거나 <code>xyxy</code>가 없으면 빈 검출로 트래커를 업데이트해 상태를 유지합니다. 검출이 있으면 차량 클래스(<code>vehicle_classes</code>)만 필터링해 ByteTrack 결과를 기준으로 카운트 루프를 진행합니다.</p>
          <p><strong>8) 카운트 규칙 (하이브리드: Line-cross + Flow)</strong><br/>
          트랙별로 <code>(infer_seq, bbox 하단 y)</code> 시계열을 최대 30개 유지합니다. <strong>Primary(hard)</strong>: 이전·현재 하단이 ROI 기준 가상선(<code>LINE_Y_RATIO</code>)을 교차하고 <code>|dy|≥MIN_MOVE</code>이면 상행/하행 hard로 집계합니다. <strong>Secondary(soft)</strong>: 연속 프레임에서 가상선 교차가 없어도, <code>|dy|≥FLOW_SOFT_MIN_DY</code>·가상선 근접(<code>LINE_SOFT_MARGIN</code>)·(3프레임 이상일 때) 방향 반전 없음이면 soft로 집계합니다(<code>HYBRID_SOFT_ENABLE</code>). 동일 <code>track_id</code>는 한 번 카운트되면 stale까지 재사용하지 않습니다. DB에는 <code>up_count_hard/down_count_hard/up_count_soft/down_count_soft</code> 및 합계 <code>up_count/down_count</code>가 저장됩니다.</p>
          <p><strong>9) WebSocket <code>type: detection</code></strong><br/>
          카운트·박스·프레임 메타·타임스탬프를 보냅니다. <code>DEBUG_IMAGE=1</code>이면 N회마다(<code>DEBUG_IMAGE_EVERY</code>) YOLO 입력 <code>roi</code>에 박스를 그린 JPEG를 base64로 넣습니다(전체 원본 프레임은 미포함).</p>
          <p style="font-size:12px;color:#666;margin-bottom:0;">주요 환경변수 예: <code>YOLO_FRAME_QUEUE_SIZE</code>, <code>HYBRID_SOFT_ENABLE</code>, <code>FLOW_SOFT_MIN_DY</code>, <code>LINE_SOFT_MARGIN</code>, <code>LINE_Y_RATIO</code>, <code>MIN_MOVE</code>, <code>YOLO_STALE_INFER_GAP</code>, <code>YOLO_SAMPLE_INTERVAL</code>, <code>YOLO_FRAME_MAX_AGE_SEC</code>, <code>YOLO_ROI_TOP_RATIO</code>, <code>YOLO_ROI_TOP_RATIO_HANAM</code>, <code>YOLO_IMGSZ</code>, <code>YOLO_IMGSZ_HANAM</code>, <code>FRAME_WIDTH</code>/<code>FRAME_HEIGHT</code>, <code>DEBUG_IMAGE</code>, <code>DEBUG_IMAGE_EVERY</code>, <code>MODEL_PATH</code>, <code>CCTV_ROTATION_SEC</code>.</p>
        </div>
      </details>
      <button type="button" id="btnStart" onclick="startRotation()">시작</button>
      <button type="button" id="btnStop" onclick="stopRotation()" disabled>정지</button>
      <span id="runStatus" style="margin-left:12px;color:#444;">대기 중</span>
      <br><br>
      <div style="margin-top:8px;">
        <div style="font-size:12px;color:#666;margin-bottom:6px;">
          YOLO 디버그: 모델 입력 roi 크롭 + 박스. <code>DEBUG_IMAGE=1</code>
        </div>
        <div id="debugGrid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px;max-width:1360px;"></div>
      </div>
      <pre id="yoloTelemetry" style="font-size:12px;color:#444;max-width:720px;white-space:pre-wrap;border-left:3px solid #ccc;padding-left:8px;">서버 YOLO: 로테이션 시작 후 여기에 추론 여부가 표시됩니다 (브라우저 영상과 무관).</pre>
      <h3>
        실시간 카운트:
        총 <span id="countValue">0</span>
        (상행 <span id="upCountValue">0</span> / 하행 <span id="downCountValue">0</span>)
        <small id="currentCctv" style="color:#666;"></small>
      </h3>
      <div id="debugBox" style="
        position:fixed;
        right:10px;
        bottom:10px;
        background:rgba(0,0,0,0.7);
        color:#0f0;
        padding:10px;
        font-size:12px;
        font-family:monospace;
        z-index:9999;
      ">YOLO: 대기중</div>
      <div>
        <h4>저장 로그</h4>
        <pre id="logBox" style="height:220px; overflow:auto; border:1px solid #ccc; padding:8px;"></pre>
      </div>
      <script>
        const DEFAULT_DEBUG_CCTVS = [
          "수원신갈IC",
          "판교분기점",
          "서울TG",
          "용인IC",
          "신갈분기점",
          "서평택분기점",
          "비봉IC",
          "매송나들목",
          "장항IC",
          "자유로분기점",
          "일산IC",
          "양주IC",
          "의정부IC",
          "동의정부IC북측",
          "남구리IC",
          "구리IC",
          "중랑IC교",
        ];
        let debugCctvs = DEFAULT_DEBUG_CCTVS.slice();

        function buildDebugCards() {
          const grid = document.getElementById("debugGrid");
          if (!grid) return;
          grid.innerHTML = "";
          for (const cctv of debugCctvs) {
            const card = document.createElement("div");
            card.className = "dbg-card";
            card.setAttribute("data-cctv", cctv);
            card.style.cssText = "border:1px solid #ccc;padding:6px;background:#f9f9f9;";
            card.innerHTML = `
              <div style="font-size:12px;font-weight:600;margin-bottom:4px;">${cctv}</div>
              <img id="debugImage-${cctv}" alt="debug ${cctv}" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;" />
              <div id="debugMeta-${cctv}" style="font-size:11px;color:#555;margin-top:4px;">대기중</div>
            `;
            grid.appendChild(card);
          }
        }

        function ensureDebugCard(cctv) {
          if (!cctv || debugCctvs.includes(cctv)) return;
          debugCctvs.push(cctv);
          buildDebugCards();
        }

        function loadDebugSites() {
          return fetch("/preview-sites")
            .then(r => r.json())
            .then((data) => {
              const names = (data.sites || [])
                .map((site) => site.name)
                .filter(Boolean);
              debugCctvs = names.length ? names : DEFAULT_DEBUG_CCTVS.slice();
              buildDebugCards();
            })
            .catch(() => {
              debugCctvs = DEFAULT_DEBUG_CCTVS.slice();
              buildDebugCards();
            });
        }

        let wsInfer = null;
        let wsPingTimer = null;
        let isRunning = false;
        const debug = document.getElementById("debugBox");

        function fmtNumber(value, digits = 2) {
          const n = Number(value);
          return Number.isFinite(n) ? n.toFixed(digits) : "0.00";
        }

        function updateDebugCard(msg) {
          const cctv = msg.cctv || "";
          if (!cctv) return;
          ensureDebugCard(cctv);
          const img = document.getElementById("debugImage-" + cctv);
          const meta = document.getElementById("debugMeta-" + cctv);
          if (img && msg.debug_image) {
            img.src = "data:image/jpeg;base64," + msg.debug_image;
          }
          if (meta) {
            meta.textContent =
              "count=" + (msg.site_count ?? 0) +
              " | main=" + (msg.main_flow_count ?? 0) +
              " | flow/s=" + fmtNumber(msg.flow_per_sec) +
              " | dir=" + fmtNumber(msg.direction_score) +
              " | valid=" + (msg.is_valid === false ? "N" : "Y") +
              " | up=" + (msg.up_count ?? 0) +
              " | down=" + (msg.down_count ?? 0) +
              " | roi_y0=" + (msg.roi_y0 ?? 0) +
              " | line_y=" + (msg.line_y ?? 0) +
              " | bucket=" + (msg.time_bucket || "") +
              " | t=" + (msg.timestamp || "");
          }
        }

        function setRunUi(running) {
          isRunning = running;
          document.getElementById("btnStart").disabled = running;
          document.getElementById("btnStop").disabled = !running;
          document.getElementById("runStatus").textContent = running ? "실행 중" : "대기 중";
        }

        let statusTimer = null;
        function updateYoloTelemetry() {
          const el = document.getElementById("yoloTelemetry");
          if (!el) return;
          fetch("/rotation/status")
            .then(r => r.json())
            .then((d) => {
              const t = d.telemetry || {};
              const seq = d.sequencer_running ? "실행 중" : "대기";
              const inf = t.infer_total ?? 0;
              const last = t.last_infer_at || "—";
              const mode = t.ingest_mode || "—";
              const yset = t.yolo_ingest_url_set ? "YOLO_INGEST_URL 설정됨(단일 URL 폴백 가능)" : "YOLO_INGEST_URL 없음(지점별 URL만 사용)";
              el.textContent = [
                "서버 YOLO (프론트 CCTV와 무관): 시퀀서 " + seq + " | " + yset,
                "모드: " + mode + " | 추론 누적 " + inf + "회 | 마지막 추론 시각 " + last,
                "마지막 처리 지점: " + (t.last_cctv_processed || "—"),
                t.line_cross_note || "",
              ].join("\\n");
            })
            .catch(() => {
              el.textContent = "/rotation/status 조회 실패 (서버 확인)";
            });
        }
        function startStatusPolling() {
          if (statusTimer) clearInterval(statusTimer);
          const update = () => {
            fetch("/count-status")
              .then(r => r.json())
              .then(s => {
                document.getElementById("countValue").textContent = s.count ?? 0;
                document.getElementById("upCountValue").textContent = s.up_count ?? 0;
                document.getElementById("downCountValue").textContent = s.down_count ?? 0;
                const name = s.cctv_name || "";
                document.getElementById("currentCctv").textContent =
                  name ? "(" + name + ")" : "";
                const logs = Array.isArray(s.logs) ? s.logs : [];
                document.getElementById("logBox").textContent = logs.join("\\n");
              })
              .catch(() => null);
            updateYoloTelemetry();
          };
          update();
          statusTimer = setInterval(update, 1000);
        }

        function startDetectionWebSocket() {
          if (wsInfer && wsInfer.readyState <= 1) {
            wsInfer.close();
          }
          if (wsPingTimer) {
            clearInterval(wsPingTimer);
            wsPingTimer = null;
          }
          const proto = location.protocol === "https:" ? "wss:" : "ws:";
          wsInfer = new WebSocket(proto + "//" + location.host + "/ws");
          wsInfer.onmessage = (evt) => {
            try {
              const msg = JSON.parse(evt.data);
              if (msg.type === "segment") {
                const cctvEl = document.getElementById("currentCctv");
                if (cctvEl && msg.cctv)
                  cctvEl.textContent = "(" + msg.cctv + ")";
              } else if (msg.type === "detection") {
                // 🔥 화면 표시만 (디버깅용)
                const el = document.getElementById("countValue");
                if (el && typeof msg.count === "number")
                  el.textContent = msg.count;
                const upEl = document.getElementById("upCountValue");
                if (upEl && typeof msg.up_count === "number")
                  upEl.textContent = msg.up_count;
                const downEl = document.getElementById("downCountValue");
                if (downEl && typeof msg.down_count === "number")
                  downEl.textContent = msg.down_count;
                const cctvEl = document.getElementById("currentCctv");
                if (cctvEl && msg.cctv)
                  cctvEl.textContent = "(" + msg.cctv + ")";
                updateDebugCard(msg);

                // 👉 디버깅용 로그
                const tracks = (msg.boxes || []).map((b) => ({
                  id: b.track_id,
                  cx: b.cx,
                  cy: b.cy
                }));
                console.log("YOLO:", {
                  cctv: msg.cctv,
                  count: msg.count,
                  site: msg.site_count,
                  main_flow: msg.main_flow_count,
                  flow_per_sec: msg.flow_per_sec,
                  direction_score: msg.direction_score,
                  valid: msg.is_valid,
                  invalid_reason: msg.invalid_reason,
                  time_bucket: msg.time_bucket,
                  roi_y0: msg.roi_y0,
                  line_y: msg.line_y,
                  boxes: (msg.boxes || []).length,
                  tracks: tracks,
                });

                // 🔥 화면 디버그 표시
                if (debug) {
                  const trackIds = tracks.map(t => t.id).join(",");
                  debug.innerHTML =
                    "CCTV: " + (msg.cctv || "") + "<br>" +
                    "COUNT: " + (msg.count ?? 0) + "<br>" +
                    "MAIN FLOW: " + (msg.main_flow_count ?? 0) + "<br>" +
                    "FLOW/S: " + fmtNumber(msg.flow_per_sec) + "<br>" +
                    "DIR SCORE: " + fmtNumber(msg.direction_score) + "<br>" +
                    "VALID: " + (msg.is_valid === false ? "false" : "true") + "<br>" +
                    "INVALID: " + (msg.invalid_reason || "-") + "<br>" +
                    "UP: " + (msg.up_count ?? 0) + "<br>" +
                    "DOWN: " + (msg.down_count ?? 0) + "<br>" +
                    "SITE COUNT: " + (msg.site_count ?? 0) + "<br>" +
                    "ROI Y0: " + (msg.roi_y0 ?? 0) + "<br>" +
                    "LINE Y: " + (msg.line_y ?? 0) + "<br>" +
                    "BUCKET: " + (msg.time_bucket || "") + "<br>" +
                    "LAG: " + fmtNumber(msg.bucket_lag_sec) + "s<br>" +
                    "BOXES: " + ((msg.boxes && msg.boxes.length) ? msg.boxes.length : 0) + "<br>" +
                    "TRACKS: " + trackIds + "<br>" +
                    "TIME: " + (msg.timestamp || "");
                }
              }
            } catch (_) {}
          };
          wsInfer.onopen = () => {
            wsPingTimer = setInterval(() => {
              try {
                if (wsInfer && wsInfer.readyState === WebSocket.OPEN) wsInfer.send("ping");
              } catch (_) {}
            }, 20000);
          };
        }

        function startRotation() {
          if (isRunning) return;
          fetch("/rotation/start", { method: "POST" })
            .then(res => {
              if (res.status === 400) {
                return res.json().then(j => {
                  const d = j.detail;
                  throw new Error(typeof d === "string" ? d : JSON.stringify(d));
                });
              }
              if (!res.ok) throw new Error("start_failed");
              return res.json();
            })
            .then(() => {
              startStatusPolling();
              startDetectionWebSocket();
              setRunUi(true);
            })
            .catch(e => alert("로테이션 시작 실패: " + (e.message || e)));
        }

        function stopRotation() {
          if (!isRunning) return;
          fetch("/rotation/stop", { method: "POST" })
            .then(res => {
              if (!res.ok) throw new Error("stop_failed");
              if (wsPingTimer) {
                clearInterval(wsPingTimer);
                wsPingTimer = null;
              }
              if (wsInfer) {
                try {
                  wsInfer.close();
                } catch (_) {}
                wsInfer = null;
              }
              if (statusTimer) {
                clearInterval(statusTimer);
                statusTimer = null;
              }
              setRunUi(false);
            })
            .catch(() => alert("정지 요청 실패"));
        }

        function initFromServer() {
          // 서버가 이미 로테이션 중이면, 프론트도 즉시 "실행 중"으로 동기화
          fetch("/rotation/status")
            .then(r => r.json())
            .then((d) => {
              const running = !!d.sequencer_running;
              setRunUi(running);
              startStatusPolling();
              startDetectionWebSocket();
            })
            .catch(() => {
              // 서버 상태를 못 읽어도, WS/폴링은 붙여서 사용자가 진단할 수 있게 둔다
              startStatusPolling();
              startDetectionWebSocket();
            });
        }

        // 최초 진입 시 서버 상태 반영
        loadDebugSites();
        initFromServer();
      </script>
    </body>
    </html>
    """
