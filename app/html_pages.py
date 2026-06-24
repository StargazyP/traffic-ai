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
    const PATH_TRAFFIC = location.pathname.match(/^(\/traffic)(?=\\/|$)/);
    const TRAFFIC_BASE = PATH_TRAFFIC ? PATH_TRAFFIC[1] : "";
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(proto + "//" + location.host + TRAFFIC_BASE + "/ws");
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
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>고속도로 디버깅 — Traffic AI</title>
  <style>
    .rotation-chip { display:inline-block; margin:3px 4px 3px 0; padding:3px 8px; border-radius:999px; font-size:11px; border:1px solid #ced4da; background:#fff; color:#495057; }
    .rotation-chip--infer { border-color:#0969da; background:#ddf4ff; color:#0550ae; }
  </style>
</head>
<body style="font-family:system-ui,sans-serif;margin:16px;">
  <h2 style="margin-bottom:8px;">고속도로 실시간 디버깅</h2>
  <p style="margin:0 0 12px;font-size:13px;"><a id="tuningLink" href="/traffic/tuning/roi">CCTV별 ROI · Line Crossing 튜닝</a></p>

  <section id="streamAlert" style="display:none;margin:0 0 12px;padding:14px 16px;border-radius:8px;border:2px solid #cf222e;background:#fff5f5;max-width:1360px;font-size:14px;color:#660e0b;line-height:1.6;"></section>
  <script>
    (function () {
      var m = location.pathname.match(/^(\/traffic)(?=\\/|$)/);
      var base = m ? m[1] : "/traffic";
      function esc(s) {
        return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      }
      function applyUnavailable(d) {
        var h = (d && d.rotation_health) || {};
        var alert = h.alert;
        var badge = document.getElementById("rotationBadge");
        var runEl = document.getElementById("runStatus");
        var telEl = document.getElementById("yoloTelemetry");
        if (badge) {
          badge.textContent = h.system_ok === false ? "CCTV 동작 불가" : badge.textContent;
          if (h.system_ok === false) {
            badge.style.background = "#ffebe9";
            badge.style.color = "#cf222e";
          }
        }
        if (runEl && h.its_health && h.its_health.ok === false) {
          runEl.textContent = "ITS 사용 불가 (" + (h.its_health.its_api_result_code || h.its_health.error_code || "") + ")";
        } else if (runEl && runEl.textContent.indexOf("확인 중") >= 0) {
          runEl.textContent = "상태 응답 수신";
        }
        if (telEl && telEl.textContent.indexOf("로딩") >= 0) {
          telEl.textContent = h.system_ok === false ? "ITS/CCTV 사용 불가 — 상세는 상단 배너 참고" : "텔레메트리 준비됨";
        }
        var box = document.getElementById("streamAlert");
        if (!box || !alert) return;
        box.style.display = "block";
        var code = alert.its_api_result_code || (h.its_health && h.its_health.its_api_result_code) || "";
        box.innerHTML =
          '<div style="font-size:16px;font-weight:700;margin-bottom:8px;">' + esc(alert.title || "CCTV 동작 불가") + "</div>" +
          (code && code !== "—" ? '<div style="font-weight:700;margin-bottom:6px;">ITS API 오류 코드: ' + esc(code) + "</div>" : "") +
          "<div>" + esc(alert.message || "") + "</div>";
      }
      fetch(base + "/rotation/status", { credentials: "same-origin" })
        .then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); })
        .then(applyUnavailable)
        .catch(function (err) {
          var badge = document.getElementById("rotationBadge");
          var runEl = document.getElementById("runStatus");
          var telEl = document.getElementById("yoloTelemetry");
          if (badge) { badge.textContent = "상태 조회 실패"; badge.style.background = "#ffebe9"; badge.style.color = "#cf222e"; }
          if (runEl) runEl.textContent = "API 연결 실패: " + (err.message || err);
          if (telEl) telEl.textContent = base + "/rotation/status 호출 실패";
        });
    })();
  </script>

  <section id="rotationPanel" style="margin:0 0 16px;max-width:1360px;">
    <div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px 16px;margin-bottom:10px;">
      <span id="rotationBadge" style="display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:600;background:#e9ecef;color:#495057;">확인 중</span>
      <span id="runStatus" style="font-size:13px;color:#444;">상태 확인 중…</span>
    </div>
    <div style="font-size:22px;font-weight:700;margin-bottom:6px;">
      로테이션 지점: <span id="activeCctvLabel" style="color:#0969da;">—</span>
    </div>
    <p id="rotationDetail" style="margin:0 0 8px;font-size:13px;color:#555;">마지막 추론 지점: — · 주기 —초</p>
    <div id="rotationSiteChips" style="margin-bottom:4px;line-height:1.6;"></div>
  </section>

  <h3 style="margin-top:0;">
    총 <span id="countValue">0</span>
    (상행 <span id="upCountValue">0</span> / 하행 <span id="downCountValue">0</span>)
    <small id="currentCctv" style="color:#666;"></small>
  </h3>

  <div style="margin-top:16px;">
    <div style="font-size:12px;color:#666;margin-bottom:8px;">
      ROI 디버그 (모델 입력 크롭 + 박스) · WebSocket <code>debug_image</code> 수신 시 갱신
    </div>
    <div id="debugGrid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px;max-width:1360px;"></div>
  </div>

  <pre id="yoloTelemetry" style="font-size:12px;color:#444;max-width:920px;white-space:pre-wrap;border-left:3px solid #ccc;padding-left:10px;margin-top:16px;">YOLO 텔레메트리 로딩…</pre>

  <div id="debugBox" style="
    position:fixed;
    right:10px;
    bottom:10px;
    max-width:min(420px, 92vw);
    background:rgba(0,0,0,0.82);
    color:#0f0;
    padding:10px 12px;
    font-size:11px;
    font-family:ui-monospace,monospace;
    z-index:9999;
    border-radius:6px;
    line-height:1.45;
  ">검출 대기…</div>

  <script>
    const PATH_TRAFFIC = location.pathname.match(/^(\/traffic)(?=\\/|$)/);
    const PATH_TUNING = location.pathname.match(/^(\/tuning)(?=\\/|$)/);
    const TRAFFIC_BASE = PATH_TRAFFIC ? PATH_TRAFFIC[1] : (PATH_TUNING ? "/traffic" : "/traffic");

    function fetchJsonWithTimeout(url, ms) {
      ms = ms || 12000;
      const ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
      const timer = ctrl ? setTimeout(function () { try { ctrl.abort(); } catch (_) {} }, ms) : null;
      return fetch(url, ctrl ? { signal: ctrl.signal } : {})
        .then(function (r) {
          if (!r.ok) throw new Error("HTTP " + r.status);
          return r.json();
        })
        .finally(function () {
          if (timer) clearTimeout(timer);
        });
    }

    function showPanelUnavailable(reason) {
      const badge = document.getElementById("rotationBadge");
      const runEl = document.getElementById("runStatus");
      const telEl = document.getElementById("yoloTelemetry");
      if (badge) {
        badge.textContent = "CCTV 동작 불가";
        badge.style.background = "#ffebe9";
        badge.style.color = "#cf222e";
      }
      if (runEl) runEl.textContent = reason || "서버 상태를 가져오지 못했습니다.";
      if (telEl) telEl.textContent = reason || "상태 API 응답 없음";
      refreshStreamAlert({
        rotation_health: {
          system_ok: false,
          alert: {
            severity: "error",
            title: "CCTV 동작 불가",
            its_api_result_code: "",
            message: reason || "서버와 통신할 수 없습니다. /traffic/ 경로·백엔드 컨테이너를 확인하세요.",
          },
        },
        streams_effective_count: 0,
      });
    }

    const DEFAULT_DEBUG_CCTVS = [
      "수원신갈IC", "판교분기점", "서울TG", "용인IC", "신갈분기점", "서평택분기점",
      "비봉IC", "매송나들목", "장항IC", "자유로분기점", "일산IC", "양주IC",
      "의정부IC", "동의정부IC북측", "남구리IC", "구리IC", "중랑IC교",
      "신월IC", "김포IC", "서운분기점", "토평IC", "서하남IC", "상일IC", "시흥IC", "안현분기점",
    ];
    let debugCctvs = DEFAULT_DEBUG_CCTVS.slice();
    let rotationState = {
      running: false,
      active: "",
      activeGroup: [],
      parallelSlots: 4,
      lastProcessed: "",
      sites: [],
      sec: 30,
      streamsEffective: null,
    };
    let lastDetectionAt = {};
    const BADGE_RECENT_MS = 15000;
    let debugFrameIdByCctv = {};
    let debugImagePollIdx = 0;

    let wsInfer = null;
    let wsPingTimer = null;
    let wsReconnectTimer = null;
    const wsBackoff = { stop: false, backoffMs: 1500 };
    let statusTimer = null;
    let pollLastInferKey = "";
    const debug = document.getElementById("debugBox");

    function setActiveGroup(group) {
      rotationState.activeGroup = Array.isArray(group) ? group.filter(Boolean) : [];
      rotationState.active = rotationState.activeGroup[0] || "";
      applyCardRotationState();
    }

    function buildDebugCards() {
      const grid = document.getElementById("debugGrid");
      if (!grid) return;
      grid.innerHTML = "";
      for (const cctv of debugCctvs) {
        const card = document.createElement("div");
        card.className = "dbg-card";
        card.setAttribute("data-cctv", cctv);
        card.style.cssText = "border:1px solid #ccc;padding:6px;background:#f9f9f9;border-radius:6px;";
        card.innerHTML =
          '<div style="display:flex;align-items:center;justify-content:space-between;gap:6px;margin-bottom:4px;">' +
            '<span style="font-size:12px;font-weight:600;">' + cctv + '</span>' +
            '<span id="debugBadge-' + cctv + '" style="font-size:10px;padding:2px 6px;border-radius:999px;background:#e9ecef;color:#666;">대기</span>' +
          '</div>' +
          '<img id="debugImage-' + cctv + '" alt="" style="width:100%;border:1px solid #ddd;background:#111;min-height:110px;border-radius:4px;object-fit:contain;" />' +
          '<div id="debugMeta-' + cctv + '" style="font-size:11px;color:#555;margin-top:6px;word-break:break-word;">대기중</div>';
        grid.appendChild(card);
      }
      applyCardRotationState();
    }

    function syncInferTimesFromStatus(d) {
      const m = (d && d.last_infer_at_per_cctv)
        || ((d && d.telemetry && d.telemetry.last_infer_at_per_cctv) || {});
      for (const cctv of Object.keys(m)) {
        const iso = m[cctv];
        if (!iso) continue;
        const t = Date.parse(iso);
        if (Number.isFinite(t)) lastDetectionAt[cctv] = t;
      }
    }

    function setDebugImage(cctv, b64) {
      if (!cctv || !b64) return;
      const img = document.getElementById("debugImage-" + cctv);
      if (!img) return;
      img.src = "data:image/jpeg;base64," + b64;
      img.alt = "ROI " + cctv;
      img.style.visibility = "visible";
    }

    function pollDebugImages() {
      const now = Date.now();
      const recent = debugCctvs.filter(function (c) {
        return lastDetectionAt[c] && (now - lastDetectionAt[c] < BADGE_RECENT_MS);
      });
      if (!recent.length) return;
      const batch = 4;
      for (let i = 0; i < batch; i++) {
        const cctv = recent[(debugImagePollIdx + i) % recent.length];
        const knownFid = debugFrameIdByCctv[cctv] || 0;
        fetch(TRAFFIC_BASE + "/rotation/debug-image/" + encodeURIComponent(cctv) + "?frame_id=" + knownFid)
          .then(function (r) { return r.json(); })
          .then(function (data) {
            if (!data || data.unchanged) return;
            const key = data.cctv || cctv;
            if (data.frame_id) debugFrameIdByCctv[key] = data.frame_id;
            if (data.debug_image) setDebugImage(key, data.debug_image);
          })
          .catch(function () { return null; });
      }
      debugImagePollIdx = (debugImagePollIdx + batch) % Math.max(recent.length, 1);
    }

    function cardBadgeText(cctv) {
      const now = Date.now();
      const activeGroup = rotationState.activeGroup || [];
      const inParallel = activeGroup.includes(cctv);
      const last = rotationState.lastProcessed || "";
      const recent = lastDetectionAt[cctv] && (now - lastDetectionAt[cctv] < BADGE_RECENT_MS);
      if (cctv === last && recent) return "디버깅 중";
      if (recent) return "추론 수신";
      if (inParallel) return "병렬 로테이션";
      return "대기";
    }

    function applyBadgeStyle(badge, label) {
      if (label === "디버깅 중" || label === "추론 수신") {
        badge.style.background = "#ddf4ff";
        badge.style.color = "#0550ae";
      } else if (label === "병렬 로테이션") {
        badge.style.background = "#dafbe1";
        badge.style.color = "#116329";
      } else {
        badge.style.background = "#e9ecef";
        badge.style.color = "#666";
      }
    }

    function applyCardRotationState() {
      for (const cctv of debugCctvs) {
        const badge = document.getElementById("debugBadge-" + cctv);
        if (!badge) continue;
        const label = cardBadgeText(cctv);
        badge.textContent = label;
        applyBadgeStyle(badge, label);
      }
    }

    function escapeHtml(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function refreshStreamAlert(d) {
      const el = document.getElementById("streamAlert");
      if (!el) return;
      const health = (d && d.rotation_health) || {};
      const alert = health.alert || null;
      const idle = (d && d.idle_reason) || null;

      if (alert && (alert.title || alert.message)) {
        const apiCode = alert.its_api_result_code;
        const isWarn = alert.severity === "warning";
        el.style.display = "block";
        el.style.borderColor = isWarn ? "#d4a72c" : "#cf222e";
        el.style.background = isWarn ? "#fff8c5" : "#fff5f5";
        el.style.color = isWarn ? "#633c01" : "#660e0b";
        let html = '<div style="font-size:16px;font-weight:700;margin-bottom:8px;">'
          + escapeHtml(alert.title || "CCTV 동작 불가") + "</div>";
        if (apiCode && apiCode !== "—") {
          html += '<div style="font-size:15px;font-weight:700;margin-bottom:6px;color:#cf222e;">'
            + 'ITS API 오류 코드: <span style="font-family:ui-monospace,monospace;">'
            + escapeHtml(apiCode) + "</span></div>";
        }
        const ih = health.its_health || {};
        if (!apiCode && ih.its_api_result_code && ih.its_api_result_code !== "—") {
          html += '<div style="font-size:15px;font-weight:700;margin-bottom:6px;color:#cf222e;">'
            + 'ITS API 오류 코드: <span style="font-family:ui-monospace,monospace;">'
            + escapeHtml(ih.its_api_result_code) + "</span></div>";
        }
        if (alert.its_error_code && String(alert.its_error_code).indexOf("stream_") === 0 && !apiCode) {
          html += '<div style="font-size:13px;font-weight:600;margin-bottom:6px;">오류: '
            + escapeHtml(alert.its_error_code) + "</div>";
        }
        if (alert.its_error_code && apiCode === "4001") {
          html += '<div style="font-size:12px;margin-bottom:6px;opacity:0.9;">분류: '
            + escapeHtml(alert.its_error_code) + "</div>";
        }
        html += "<div>" + escapeHtml(alert.message || "") + "</div>";
        if (health.stream_source) {
          html += '<div style="margin-top:8px;font-size:12px;opacity:0.85;">스트림 소스: '
            + escapeHtml(health.stream_source) + "</div>";
        }
        el.innerHTML = html;
        return;
      }

      if (idle && idle.message && (health.system_ok === false || (d.streams_effective_count || 0) <= 0)) {
        el.style.display = "block";
        el.style.borderColor = "#cf222e";
        el.style.background = "#fff5f5";
        const apiFromIdle = idle.its_api_result_code || "";
        let html = '<div style="font-size:16px;font-weight:700;margin-bottom:8px;">CCTV 동작 불가</div>';
        if (apiFromIdle && apiFromIdle !== "—") {
          html += '<div style="font-weight:700;margin-bottom:6px;">ITS API 오류 코드: '
            + escapeHtml(apiFromIdle) + "</div>";
        }
        html += "<div>" + escapeHtml(idle.message) + "</div>";
        el.innerHTML = html;
        return;
      }

      if (health.system_ok === false && !alert) {
        el.style.display = "block";
        el.innerHTML = "<strong>CCTV 동작 불가</strong><br/>스트림·ITS 설정을 확인하세요.";
        return;
      }

      el.style.display = "none";
      el.innerHTML = "";
    }

    function updateRotationPanel(d) {
      syncInferTimesFromStatus(d);
      const sites = Array.isArray(d.rotation_sites) ? d.rotation_sites : [];
      if (sites.length) rotationState.sites = sites;
      rotationState.running = !!(d.rotation_running || d.sequencer_running || d.yolo_running);
      if (Array.isArray(d.active_cctv_group) && d.active_cctv_group.length) {
        setActiveGroup(d.active_cctv_group);
      } else {
        rotationState.active = (d.active_cctv || "").trim();
        if (rotationState.active) setActiveGroup([rotationState.active]);
      }
      rotationState.parallelSlots = d.rotation_parallel_slots || rotationState.parallelSlots || 4;
      rotationState.lastProcessed = (d.last_cctv_processed || (d.telemetry || {}).last_cctv_processed || "").trim();
      rotationState.sec = d.rotation_sec || rotationState.sec || 30;

      let effSites;
      if (typeof d.streams_effective_count === "number") {
        effSites = d.streams_effective_count;
        rotationState.streamsEffective = effSites;
      } else {
        const nameLen = (rotationState.sites && rotationState.sites.length)
          ? rotationState.sites.length
          : sites.length;
        effSites = rotationState.streamsEffective != null ? rotationState.streamsEffective : nameLen;
      }

      const badge = document.getElementById("rotationBadge");
      const runEl = document.getElementById("runStatus");
      const activeEl = document.getElementById("activeCctvLabel");
      const detailEl = document.getElementById("rotationDetail");
      const chipsEl = document.getElementById("rotationSiteChips");

      const health = d.rotation_health || {};
      const systemOk = health.system_ok !== false && !(health.alert && health.alert.severity === "error");

      if (badge) {
        if (!systemOk) {
          badge.textContent = "CCTV 동작 불가";
          badge.style.background = "#ffebe9";
          badge.style.color = "#cf222e";
        } else if (rotationState.running) {
          badge.textContent = "CCTV 로테이션 실행 중";
          badge.style.background = "#dafbe1";
          badge.style.color = "#116329";
        } else {
          badge.textContent =
            effSites <= 0 ? "스트림·ITS 미설정 (로테이션 불가)" : "로테이션 대기";
          badge.style.background = effSites <= 0 ? "#ffebe9" : "#e9ecef";
          badge.style.color = effSites <= 0 ? "#cf222e" : "#495057";
        }
      }
      if (runEl) {
        const ih = health.its_health || {};
        const itsLine = ih.ok === false
          ? "ITS 사용 불가" + (ih.its_api_result_code ? " (" + ih.its_api_result_code + ")" : "")
          : (ih.ok === true ? "ITS 정상" : "ITS 확인 중");
        const seq = d.sequencer_running ? "시퀀서 ON" : "시퀀서 OFF";
        const yolo = d.yolo_running ? "YOLO ON" : "YOLO OFF";
        const spot = effSites > 0 ? "서버 유효 스트림 " + effSites + "곳" : "유효 스트림 0곳";
        runEl.textContent =
          itsLine + " · " + seq + " · " + yolo + " · " + spot + " · " + rotationState.sec + "초/지점";
      }
      if (activeEl) {
        const group = rotationState.activeGroup || [];
        activeEl.textContent = group.length
          ? group.join(" · ")
          : (rotationState.running ? "전환 대기" : "—");
        activeEl.style.color = group.length ? "#1a7f37" : "#0969da";
      }
      if (detailEl) {
        detailEl.textContent =
          "병렬 " + (rotationState.activeGroup || []).length + "지점 · 마지막 추론: " +
          (rotationState.lastProcessed || "—") +
          " · 카운트 지점: " + ((d.count_status_summary || {}).cctv_name || "—");
      }
      if (chipsEl) {
        if (effSites <= 0) {
          const idleMsg = (d.idle_reason && d.idle_reason.message)
            ? d.idle_reason.message
            : "CCTV 스트림이 서버에 없습니다. .env 의 ITS_API_KEY 또는 CCTV_URL 을 설정하세요.";
          chipsEl.innerHTML =
            '<span class="rotation-chip" style="border-color:#cf222e;background:#fff5f5;">' +
            idleMsg +
            "</span>";
        } else {
          const list = rotationState.sites.length ? rotationState.sites : debugCctvs;
          const now = Date.now();
          chipsEl.innerHTML = list.map(function(name) {
            let cls = "rotation-chip";
            const recent = lastDetectionAt[name] && (now - lastDetectionAt[name] < BADGE_RECENT_MS);
            if (name === rotationState.lastProcessed && recent) cls += " rotation-chip--infer";
            return '<span class="' + cls + '">' + name + "</span>";
          }).join("");
        }
      }
      refreshStreamAlert(d);
      applyCardRotationState();
    }

    function ensureDebugCard(cctv) {
      if (!cctv || debugCctvs.includes(cctv)) return;
      debugCctvs.push(cctv);
      buildDebugCards();
    }

    function loadDebugSites() {
      return fetchJsonWithTimeout(TRAFFIC_BASE + "/preview-sites", 15000)
        .then((data) => {
          const names = (data.sites || []).map((s) => s.name).filter(Boolean);
          const eff = typeof data.effective_site_count === "number" ? data.effective_site_count : names.length;
          rotationState.streamsEffective = eff;
          debugCctvs = names.length ? names : DEFAULT_DEBUG_CCTVS.slice();
          buildDebugCards();
          refreshStreamAlert({
            rotation_health: data.rotation_health,
            idle_reason: data.idle_reason,
            streams_effective_count: eff,
          });
        })
        .catch((err) => {
          debugCctvs = DEFAULT_DEBUG_CCTVS.slice();
          buildDebugCards();
          showPanelUnavailable(
            "지점 목록 조회 실패: " + (err && err.message ? err.message : "네트워크 오류")
          );
        });
    }

    function fmtNumber(value, digits) {
      digits = digits === undefined ? 2 : digits;
      const n = Number(value);
      return Number.isFinite(n) ? n.toFixed(digits) : "0.00";
    }

    function updateDebugCard(msg, opts) {
      opts = opts || {};
      const cctv = msg.cctv || msg.cctv_name || "";
      if (!cctv) return;
      ensureDebugCard(cctv);
      const meta = document.getElementById("debugMeta-" + cctv);
      if (!opts.skipImage && msg.debug_image) {
        setDebugImage(cctv, msg.debug_image);
        if (msg.frame_id) debugFrameIdByCctv[cctv] = msg.frame_id;
      } else if (!opts.skipImage && msg.frame_id) {
        debugFrameIdByCctv[cctv] = msg.frame_id;
      }
      lastDetectionAt[cctv] = Date.now();

      if (meta) {
        meta.textContent =
          "count=" + (msg.site_count ?? 0) +
          " | main=" + (msg.main_flow_count ?? 0) +
          " | flow/s=" + fmtNumber(msg.flow_per_sec) +
          " | dir=" + fmtNumber(msg.direction_score) +
          " | valid=" + (msg.is_valid === false ? "N" : "Y") +
          " | up=" + (msg.up_count ?? 0) +
          " | down=" + (msg.down_count ?? 0) +
          " | roi=(" + (msg.roi_x0 ?? 0) + "," + (msg.roi_y0 ?? 0) + ")-(" +
            (msg.roi_x1 ?? 0) + "," + (msg.roi_y1 ?? 0) + ")" +
          " | line_global=" + (msg.line_y_global ?? msg.line_y ?? 0) +
          " | tracks=" + (msg.active_tracks ?? 0) + "/" + (msg.persisted_tracks ?? 0) +
          " | age_ms=" + Math.round(msg.frame_age_ms ?? 0) +
          " | t=" + (msg.timestamp || "");
      }
      applyCardRotationState();
    }

    function reconcilePollInferActivity(data) {
      const cctv = ((data || {}).cctv || (data || {}).cctv_name || "").trim();
      const fid = (data || {}).frame_id;
      if (!cctv || fid == null || Number(fid) <= 0) return;
      const k = String(cctv) + "|" + String(fid);
      if (k === pollLastInferKey) return;
      pollLastInferKey = k;
      lastDetectionAt[cctv] = Date.now();
      updateDebugCard(data);
    }

    function updateCounts(data) {
      document.getElementById("countValue").textContent = data.count ?? 0;
      document.getElementById("upCountValue").textContent = data.up_count ?? 0;
      document.getElementById("downCountValue").textContent = data.down_count ?? 0;
      const name = data.cctv_name || data.cctv || "";
      document.getElementById("currentCctv").textContent = name ? "(" + name + ")" : "";
    }

    function updateYoloTelemetry() {
      const telEl = document.getElementById("yoloTelemetry");
      return fetchJsonWithTimeout(TRAFFIC_BASE + "/rotation/status", 12000)
        .then((d) => {
          updateRotationPanel(d);
          if (!telEl) return;
          const t = d.telemetry || {};
          const rh = d.rotation_health || {};
          const ih = rh.its_health || {};
          const seq = d.sequencer_running ? "실행 중" : "대기";
          const yolo = d.yolo_running ? "실행 중" : "대기";
          const inf = t.infer_total ?? 0;
          const last = t.last_infer_at || "—";
          const mode = t.ingest_mode || "—";
          const yset = t.yolo_ingest_url_set
            ? "YOLO_INGEST_URL 설정됨"
            : "YOLO_INGEST_URL 없음";
          const itsLine = ih.ok === false
            ? "ITS: 사용 불가 " + (ih.its_api_result_code || ih.error_code || "")
            : (ih.ok === true ? "ITS: 정상 (목록 " + (ih.row_count || "?") + "건)" : "ITS: —");
          telEl.textContent = [
            itsLine,
            "서버 YOLO: 시퀀서 " + seq + " | YOLO 워커 " + yolo + " | " + yset,
            "로테이션 지점: " + (d.active_cctv || "—") + " (주기 " + (d.rotation_sec || "—") + "초)",
            "모드: " + mode + " | 추론 누적 " + inf + "회 | 마지막 추론 시각 " + last,
            "마지막 처리 지점: " + (d.last_cctv_processed || t.last_cctv_processed || "—"),
            t.line_cross_note || "",
          ].join("\\n");
        })
        .catch((err) => {
          showPanelUnavailable(
            "/rotation/status 조회 실패: " + (err && err.message ? err.message : "타임아웃·네트워크")
          );
        });
    }

    function startStatusPolling() {
      if (statusTimer) clearInterval(statusTimer);
      const tick = () => {
        fetch(TRAFFIC_BASE + "/count-status")
          .then((r) => r.json())
          .then(function (payload) {
            updateCounts(payload);
            reconcilePollInferActivity(payload);
            if (payload.debug_image && (payload.cctv || payload.cctv_name)) {
              updateDebugCard(payload);
            }
          })
          .catch(() => null);
        pollDebugImages();
        updateYoloTelemetry();
      };
      tick();
      statusTimer = setInterval(tick, 1000);
    }

    function scheduleWsReconnect(reason) {
      if (wsBackoff.stop || wsReconnectTimer) return;
      const delay = wsBackoff.backoffMs;
      wsBackoff.backoffMs = Math.min(wsBackoff.backoffMs * 1.5, 45000);
      wsReconnectTimer = setTimeout(() => {
        wsReconnectTimer = null;
        if (!wsBackoff.stop) openDetectionWs();
      }, delay);
      if (debug && reason) debug.textContent = "WS 재연결(" + delay + "ms) " + reason + "…";

    }

    function openDetectionWs() {
      if (wsPingTimer) {
        clearInterval(wsPingTimer);
        wsPingTimer = null;
      }
      const old = wsInfer;
      wsInfer = null;
      if (old) {
        try {
          old.onmessage = null;
          old.onopen = null;
          old.onerror = null;
          old.onclose = null;
        } catch (_) {}
        try {
          if (old.readyState === WebSocket.OPEN || old.readyState === WebSocket.CONNECTING) {
            old.close();
          }
        } catch (_) {}
      }

      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const url = proto + "//" + location.host + TRAFFIC_BASE + "/ws";
      wsInfer = new WebSocket(url);

      wsInfer.onmessage = (evt) => {
        try {
          const msg = JSON.parse(evt.data);
          if (msg.type === "segment") {
            const group = Array.isArray(msg.cctvs) && msg.cctvs.length
              ? msg.cctvs
              : (msg.cctv ? [msg.cctv] : []);
            setActiveGroup(group);
            updateCounts({ cctv: group[0] || msg.cctv });
            updateRotationPanel({
              rotation_running: true,
              active_cctv: group[0] || msg.cctv,
              active_cctv_group: group,
              rotation_sites: rotationState.sites,
              rotation_sec: rotationState.sec,
              rotation_parallel_slots: rotationState.parallelSlots,
              telemetry: { last_cctv_processed: rotationState.lastProcessed },
              count_status_summary: { cctv_name: group[0] || msg.cctv },
            });
          } else if (msg.type === "detection") {
            updateCounts(msg);
            updateDebugCard(msg, { skipImage: true });
            if (msg.frame_id) debugFrameIdByCctv[msg.cctv || ""] = msg.frame_id;
            if (debug) {
              const tracks = (msg.boxes || []).map((b) => ({ id: b.track_id, cx: b.cx, cy: b.cy }));
              const trackIds = tracks.map((t) => t.id).filter(Boolean).join(",");
              debug.innerHTML =
                "<strong>" + (msg.cctv || "") + "</strong><br>" +
                "COUNT " + (msg.count ?? 0) +
                " | UP/DOWN " + (msg.up_count ?? 0) + "/" + (msg.down_count ?? 0) + "<br>" +
                "FLOW/S " + fmtNumber(msg.flow_per_sec) +
                " | DIR " + fmtNumber(msg.direction_score) + "<br>" +
                "VALID " + (msg.is_valid === false ? "false" : "true") +
                (msg.invalid_reason ? (" | " + msg.invalid_reason) : "") + "<br>" +
                "ROI y0 " + (msg.roi_y0 ?? 0) +
                " | LINE " + (msg.line_y ?? 0) +
                " | GLOBAL " + (msg.line_y_global ?? msg.line_y ?? 0) + "<br>" +
                "BUCKET " + (msg.time_bucket || "") +
                " | LAG " + fmtNumber(msg.bucket_lag_sec) + "s<br>" +
                "BOXES " + ((msg.boxes && msg.boxes.length) ? msg.boxes.length : 0) +
                " | TRACKS " + trackIds + "<br>" +
                (msg.timestamp || "");
            }
          }
        } catch (_) {}
      };

      wsInfer.onopen = () => {
        wsBackoff.backoffMs = 1500;
        if (debug) debug.textContent = "WS 연결됨 · " + url;
        wsPingTimer = setInterval(() => {
          try {
            if (wsInfer && wsInfer.readyState === WebSocket.OPEN) wsInfer.send("ping");
          } catch (_) {}
        }, 20000);
      };

      wsInfer.onerror = () => {
        scheduleWsReconnect("error");
      };

      wsInfer.onclose = () => {
        wsInfer = null;
        if (wsPingTimer) {
          clearInterval(wsPingTimer);
          wsPingTimer = null;
        }
        if (!wsBackoff.stop) scheduleWsReconnect("연결 종료");
      };
    }

    function startDetectionWebSocket() {
      wsBackoff.stop = false;
      openDetectionWs();
    }

    window.addEventListener("beforeunload", () => {
      wsBackoff.stop = true;
      if (wsReconnectTimer) {
        clearTimeout(wsReconnectTimer);
        wsReconnectTimer = null;
      }
      try {
        if (wsInfer) wsInfer.close();
      } catch (_) {}
    });

    updateYoloTelemetry();

    loadDebugSites().finally(() => {
      const tuningLink = document.getElementById("tuningLink");
      if (tuningLink) {
        tuningLink.href = TRAFFIC_BASE + "/tuning/roi";
      }
      startStatusPolling();
      startDetectionWebSocket();
    });
  </script>
</body>
</html>
"""


def roi_tuning_html() -> str:
    return """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>ROI / Line Crossing 튜닝</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 16px; max-width: 1200px; }
    .row { display: flex; gap: 20px; flex-wrap: wrap; align-items: flex-start; }
    .panel { flex: 1 1 320px; }
    label { display: block; margin: 10px 0 4px; font-size: 13px; color: #444; }
    input[type=range] { width: 100%; }
    select, button { font-size: 14px; padding: 6px 10px; }
    #preview { width: 100%; max-width: 960px; border: 1px solid #ccc; border-radius: 8px; background: #111; min-height: 240px; }
    .meta { font-size: 12px; color: #666; margin-top: 8px; line-height: 1.5; }
    .legend span { display: inline-block; margin-right: 12px; font-size: 12px; }
    .legend .c-roi { color: #00d7ff; }
    .legend .c-line { color: #ff3737; }
    .legend .c-sug { color: #50ff78; }
  </style>
</head>
<body>
  <h2>CCTV별 ROI · Line Crossing 튜닝</h2>
  <p style="color:#555;font-size:14px;">
    빨간선=현재 카운트 라인, 초록선=프레임 엣지 기반 제안. 저장 시 <code>data/camera_config_overrides.json</code>에 반영(재시작 불필요).
    <a id="dashboardLink" href="/traffic/">← 대시보드</a>
  </p>
  <div class="row">
    <div class="panel">
      <label for="siteSelect">지점</label>
      <select id="siteSelect"></select>
      <button type="button" id="refreshBtn" style="margin-left:8px;">프레임 새로고침</button>
      <label>ROI top <span id="topVal"></span></label>
      <input id="topSlider" type="range" min="0.20" max="0.55" step="0.01" />
      <label>ROI left <span id="leftVal"></span></label>
      <input id="leftSlider" type="range" min="0" max="0.30" step="0.01" />
      <label>ROI width <span id="widthVal"></span></label>
      <input id="widthSlider" type="range" min="0.50" max="1.00" step="0.01" />
      <label>Line Y ratio (ROI 내부) <span id="lineVal"></span></label>
      <input id="lineSlider" type="range" min="0.45" max="0.90" step="0.005" />
      <label>min_move <span id="minMoveVal"></span></label>
      <input id="minMoveSlider" type="range" min="1" max="8" step="1" />
      <label>soft_margin_ratio <span id="softVal"></span></label>
      <input id="softSlider" type="range" min="0.20" max="0.50" step="0.01" />
      <p>
        <button type="button" id="applySuggestBtn">제안선 적용</button>
        <button type="button" id="saveBtn">저장</button>
      </p>
      <div id="saveMsg" class="meta"></div>
    </div>
    <div class="panel" style="flex:2 1 480px;">
      <img id="preview" alt="ROI preview" />
      <div class="legend meta">
        <span class="c-roi">■ ROI 테두리</span>
        <span class="c-line">■ 현재 라인</span>
        <span class="c-sug">■ 제안 라인</span>
      </div>
      <div id="previewMeta" class="meta"></div>
    </div>
  </div>
  <script>
    const PATH_TRAFFIC = location.pathname.match(/^(\/traffic)(?=\\/|$)/);
    const PATH_TUNING = location.pathname.match(/^(\/tuning)(?=\\/|$)/);
    const BASE = PATH_TRAFFIC ? PATH_TRAFFIC[1] : (PATH_TUNING ? "/traffic" : "");
    const dashboardLink = document.getElementById("dashboardLink");
    if (dashboardLink) dashboardLink.href = (BASE || "/traffic") + "/";
    const siteSelect = document.getElementById("siteSelect");
    const sliders = {
      top: document.getElementById("topSlider"),
      left: document.getElementById("leftSlider"),
      width: document.getElementById("widthSlider"),
      line_y_ratio: document.getElementById("lineSlider"),
      min_move: document.getElementById("minMoveSlider"),
      soft_margin_ratio: document.getElementById("softSlider"),
    };
    const labels = {
      top: document.getElementById("topVal"),
      left: document.getElementById("leftVal"),
      width: document.getElementById("widthVal"),
      line_y_ratio: document.getElementById("lineVal"),
      min_move: document.getElementById("minMoveVal"),
      soft_margin_ratio: document.getElementById("softVal"),
    };
    let currentKey = "";
    let suggested = null;
    let previewTimer = null;

    function cfgFromSliders() {
      return {
        top: parseFloat(sliders.top.value),
        left: parseFloat(sliders.left.value),
        width: parseFloat(sliders.width.value),
        line_y_ratio: parseFloat(sliders.line_y_ratio.value),
        min_move: parseFloat(sliders.min_move.value),
        soft_margin_ratio: parseFloat(sliders.soft_margin_ratio.value),
      };
    }

    function setSliders(cfg) {
      const c = cfg || {};
      sliders.top.value = c.top ?? 0.38;
      sliders.left.value = c.left ?? 0;
      sliders.width.value = c.width ?? 1;
      sliders.line_y_ratio.value = c.line_y_ratio ?? 0.72;
      sliders.min_move.value = c.min_move ?? 2;
      sliders.soft_margin_ratio.value = c.soft_margin_ratio ?? 0.36;
      for (const k of Object.keys(sliders)) {
        labels[k].textContent = sliders[k].value;
      }
    }

    async function loadSites() {
      const res = await fetch(BASE + "/rotation/camera-config");
      const data = await res.json();
      siteSelect.innerHTML = "";
      for (const row of data.sites || []) {
        const opt = document.createElement("option");
        opt.value = row.key;
        opt.textContent = row.key;
        siteSelect.appendChild(opt);
      }
      if (siteSelect.options.length) {
        currentKey = siteSelect.value;
        setSliders((data.sites.find(s => s.key === currentKey) || {}).config);
        schedulePreview(true);
      }
    }

    function schedulePreview(immediate) {
      if (previewTimer) clearTimeout(previewTimer);
      previewTimer = setTimeout(() => fetchPreview(), immediate ? 0 : 180);
    }

    async function fetchPreview() {
      if (!currentKey) return;
      const q = new URLSearchParams(cfgFromSliders());
      const res = await fetch(BASE + "/rotation/camera-config/" + encodeURIComponent(currentKey) + "/preview?" + q);
      const data = await res.json();
      suggested = data.suggested_line_y_ratio ?? null;
      if (data.preview_jpeg_b64) {
        document.getElementById("preview").src = "data:image/jpeg;base64," + data.preview_jpeg_b64;
      }
      const roi = data.roi || {};
      document.getElementById("previewMeta").textContent =
        (data.frame_available ? "프레임 수신됨" : "프레임 없음(로테이션 대기)") +
        " | line_y_roi=" + (roi.line_y_roi_px ?? "-") +
        " | 제안=" + (suggested != null ? suggested.toFixed(3) : "-") +
        (data.delta_ratio != null ? " | Δ=" + data.delta_ratio : "");
    }

    async function saveConfig() {
      const res = await fetch(BASE + "/rotation/camera-config/" + encodeURIComponent(currentKey), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cfgFromSliders()),
      });
      const data = await res.json();
      document.getElementById("saveMsg").textContent = data.ok ? "저장됨: " + currentKey : (data.message || "실패");
      if (data.config) setSliders(data.config);
    }

    for (const k of Object.keys(sliders)) {
      sliders[k].addEventListener("input", () => {
        labels[k].textContent = sliders[k].value;
        schedulePreview(false);
      });
    }
    siteSelect.addEventListener("change", () => {
      currentKey = siteSelect.value;
      fetch(BASE + "/rotation/camera-config/" + encodeURIComponent(currentKey))
        .then(r => r.json())
        .then(d => { setSliders(d.config); schedulePreview(true); });
    });
    document.getElementById("refreshBtn").addEventListener("click", () => schedulePreview(true));
    document.getElementById("applySuggestBtn").addEventListener("click", () => {
      if (suggested != null) {
        sliders.line_y_ratio.value = Math.min(0.90, Math.max(0.45, suggested)).toFixed(3);
        labels.line_y_ratio.textContent = sliders.line_y_ratio.value;
        schedulePreview(true);
      }
    });
    document.getElementById("saveBtn").addEventListener("click", saveConfig);
    loadSites();
  </script>
</body>
</html>
"""
