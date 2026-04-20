/**
 * sourcing.js — 신제품 소싱 탭 프론트엔드 (v0.3.1 — UX/UI 현대화)
 *
 * 변경점 (v0.3.1):
 *   1. 인라인 스타일 → .src-* class 기반 (sourcing.css 에 정의)
 *   2. 영상 처리 시 진행률(%) 프로그레스 바 표시 (internal_step 폴링)
 *   3. 별도의 "분석 결과" 탭 추가 — 시장성/마케팅 자료 영구 보관·조회
 *   4. 마케팅 자료 모달 (마크다운 + 클립보드 복사)
 *
 * 기존 API 호출·DOM id 는 그대로 유지 (백엔드 변경 없음).
 */
(function () {
  "use strict";

  const API = "/api/sourcing";

  // ─── order-agent api.js 호환 shim ────────────────────────────
  async function authFetch(path, opts = {}) {
    const token = localStorage.getItem("jwt_token") || "";
    const headers = Object.assign(
      { "Authorization": "Bearer " + token },
      opts.headers || {}
    );
    if (opts.body && typeof opts.body === "string" && !headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }
    const res = await fetch(path, { ...opts, headers });
    if (res.status === 401) {
      if (typeof window.onAuthRequired === "function") window.onAuthRequired();
    }
    return res;
  }

  // ─── 공통 유틸 ────────────────────────────────────────────
  function escape(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatUploadDate(iso) {
    if (!iso) return "-";
    const s = String(iso);
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    return m ? `${m[1]}-${m[2]}-${m[3]}` : s.slice(0, 10);
  }

  function showToast(msg, kind) {
    const el = document.createElement("div");
    const bg = kind === "error" ? "#ef4444" : kind === "success" ? "#10b981" : "#111827";
    el.style.cssText = `
      position:fixed;bottom:24px;left:50%;transform:translateX(-50%);
      background:${bg};color:#fff;padding:10px 20px;border-radius:10px;
      font-size:13px;font-weight:500;z-index:10000;
      box-shadow:0 10px 25px rgba(0,0,0,.25);
      animation:src-slide-up .2s ease;
    `;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => {
      el.style.transition = "opacity .3s";
      el.style.opacity = "0";
      setTimeout(() => el.remove(), 300);
    }, 2500);
  }

  function buildModal(title, bodyHtml, { width } = {}) {
    const backdrop = document.createElement("div");
    backdrop.className = "src-modal-backdrop";
    backdrop.innerHTML = `
      <div class="src-modal" ${width ? `style="max-width:${width}"` : ""}>
        <div class="src-modal-header">
          <h3>${escape(title)}</h3>
          <button class="src-modal-close" title="닫기">×</button>
        </div>
        <div class="src-modal-body"></div>
      </div>
    `;
    const body = backdrop.querySelector(".src-modal-body");
    if (typeof bodyHtml === "string") body.innerHTML = bodyHtml;
    else if (bodyHtml instanceof Node) body.appendChild(bodyHtml);
    const close = () => backdrop.remove();
    backdrop.querySelector(".src-modal-close").addEventListener("click", close);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) close(); });
    document.body.appendChild(backdrop);
    return { backdrop, body, close };
  }

  // ─── 상태 배지 ──────────────────────────────────────────────
  const STATUS_MAP = {
    pending:     { cls: "src-badge-new",      icon: "🆕", label: "대기" },
    in_progress: { cls: "src-badge-progress", icon: "🔄", label: "처리중" },
    done:        { cls: "src-badge-done",     icon: "✅", label: "완료" },
    failed:      { cls: "src-badge-failed",   icon: "⚠️", label: "실패" },
  };
  function statusBadge(status, retry) {
    const b = STATUS_MAP[status] || STATUS_MAP.pending;
    const r = retry > 0 ? ` (재시도 ${retry})` : "";
    return `<span class="src-badge ${b.cls}">${b.icon} ${b.label}${r}</span>`;
  }

  // ─── 진행률 매핑 ─────────────────────────────────────────
  // internal_step 을 0~100 % 로 변환. status 에 따라 완료/실패 색 적용.
  function stepToProgress(step, status) {
    if (status === "done") return { pct: 100, label: "완료", cls: "done" };
    if (status === "failed") return { pct: 100, label: "실패", cls: "failed" };
    const map = {
      "transcribing":  { pct: 30, label: "자막 전사 중... (30%)" },
      "correcting":    { pct: 60, label: "오타·문맥 보정 중... (60%)" },
      "extracting":    { pct: 85, label: "제품 정보 추출 중... (85%)" },
      "done":          { pct: 100, label: "완료 (100%)", cls: "done" },
      "failed":        { pct: 100, label: "실패", cls: "failed" },
    };
    return map[step] || { pct: 10, label: "처리 대기 중... (10%)" };
  }

  function progressBarHtml(pct, label, cls) {
    return `
      <div class="src-progress-wrap">
        <div class="src-progress">
          <div class="src-progress-bar ${cls || ""}" style="width:${pct}%"></div>
        </div>
        <div class="src-progress-label">${escape(label)}</div>
      </div>
    `;
  }

  // ─── 대시보드 ───────────────────────────────────────────────
  async function renderDashboard(container) {
    try {
      const res = await authFetch(`${API}/dashboard`);
      if (!res.ok) {
        container.innerHTML = `<div class="src-banner src-banner-error">대시보드를 불러올 수 없습니다. (${res.status})</div>`;
        return;
      }
      const d = await res.json();
      const rate = (d.hit_rate.hit_rate * 100).toFixed(1);
      container.innerHTML = `
        <div class="src-kpi-grid">
          ${kpi("오늘 신규 영상", d.today_videos)}
          ${kpi("오늘 신규 제품", d.today_products)}
          ${kpi("대기 영상", d.pending_videos)}
          ${kpi("실패 재시도", d.failed_videos)}
          ${kpi("진행 중 컨택", d.active_outreach_drafts)}
          ${kpi("소싱 적중률", `${rate}%`, `${d.hit_rate.hits}/${d.hit_rate.total_purchased}`)}
        </div>
        <p class="src-hint">
          ※ 적중 기준: 30일 매출 ≥ ₩${(d.hit_rate.threshold_revenue_krw / 10000).toFixed(0)}만원
        </p>`;
    } catch (e) {
      container.innerHTML = `<div class="src-banner src-banner-error">오류: ${escape(e.message)}</div>`;
    }
  }
  function kpi(label, value, sub = "") {
    return `
      <div class="src-kpi">
        <div class="src-kpi-label">${escape(label)}</div>
        <div class="src-kpi-value">${value}</div>
        ${sub ? `<div class="src-kpi-sub">${escape(sub)}</div>` : ""}
      </div>`;
  }

  // ─── 채널 관리 ──────────────────────────────────────────────
  async function renderChannelsPanel(container) {
    const res = await authFetch(`${API}/channels`);
    const channels = res.ok ? await res.json() : [];
    container.innerHTML = `
      <form id="chan-form" class="src-row" style="margin-bottom:16px">
        <input id="chan-url" type="text" class="src-input src-grow" required
          placeholder="YouTube URL / @handle / UCxxx / 영상 URL" style="min-width:280px">
        <input id="chan-cat" type="text" class="src-input"
          placeholder="카테고리 (예: 알리추천)" style="width:200px">
        <button type="submit" class="src-btn src-btn-primary">➕ 추가</button>
      </form>

      <div class="src-table-wrap">
        <table class="src-table">
          <thead><tr>
            <th>#</th><th>핸들/ID</th><th>제목</th><th>카테고리</th><th>마지막 폴링</th><th>액션</th>
          </tr></thead>
          <tbody>
            ${channels.map(c => `
              <tr>
                <td>${c.id}</td>
                <td class="src-mono">${escape(c.channel_handle || c.channel_id)}</td>
                <td>${escape(c.channel_title || "(미확인)")}</td>
                <td>${escape(c.category || "")}</td>
                <td class="src-muted" style="font-size:12px">${escape(c.last_polled_at || "미실행")}</td>
                <td style="white-space:nowrap">
                  <button data-action="poll" data-id="${c.id}" class="src-btn src-btn-sm">⚡ 즉시 폴링</button>
                  <button data-action="poll-period" data-id="${c.id}" class="src-btn src-btn-sm">📅 기간 폴링</button>
                </td>
              </tr>`).join("") || '<tr><td colspan="6" class="src-empty">등록된 채널이 없습니다</td></tr>'}
          </tbody>
        </table>
      </div>
      <p class="src-hint">
        ℹ️ 즉시 폴링: 최근 업로드 10개 수집 (~3 API units) · 기간 폴링: 날짜 범위 영상 수집 (~100 units, 신중히 사용)
      </p>`;

    document.getElementById("chan-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const body = JSON.stringify({
        url_or_id: document.getElementById("chan-url").value.trim(),
        category:  document.getElementById("chan-cat").value.trim() || null,
        polling_mode: "auto",
      });
      const r = await authFetch(`${API}/channels`, { method: "POST", body });
      if (r.ok) { renderChannelsPanel(container); showToast("✓ 채널이 추가되었습니다", "success"); }
      else alert("채널 추가 실패: " + (await r.text()));
    });

    container.querySelectorAll("button[data-action='poll']").forEach((btn) =>
      btn.addEventListener("click", async () => {
        btn.disabled = true; btn.innerHTML = `<span class="src-spinner"></span>수집 중...`;
        try {
          const r = await authFetch(`${API}/channels/${btn.dataset.id}/poll`, { method: "POST" });
          const data = await r.json().catch(() => ({}));
          if (r.ok) {
            showToast(data.message || "폴링 완료", "success");
            renderChannelsPanel(container);
          } else {
            alert("폴링 실패: " + (data.detail || r.status));
            btn.disabled = false; btn.innerHTML = "⚡ 즉시 폴링";
          }
        } catch (e) {
          alert("오류: " + e.message);
          btn.disabled = false; btn.innerHTML = "⚡ 즉시 폴링";
        }
      }));

    container.querySelectorAll("button[data-action='poll-period']").forEach((btn) =>
      btn.addEventListener("click", async () => {
        const today = new Date().toISOString().slice(0, 10);
        const weekAgo = new Date(Date.now() - 7*24*60*60*1000).toISOString().slice(0, 10);
        const start = prompt("시작일 (YYYY-MM-DD)", weekAgo);
        if (!start) return;
        const end = prompt("종료일 (YYYY-MM-DD)", today);
        if (!end) return;
        btn.disabled = true; btn.innerHTML = `<span class="src-spinner"></span>수집 중...`;
        try {
          const r = await authFetch(`${API}/channels/${btn.dataset.id}/poll-period`, {
            method: "POST",
            body: JSON.stringify({ start, end }),
          });
          const data = await r.json().catch(() => ({}));
          if (r.ok) {
            showToast(data.message || "기간 폴링 완료", "success");
            renderChannelsPanel(container);
          } else {
            alert("기간 폴링 실패: " + (data.detail || r.status));
            btn.disabled = false; btn.innerHTML = "📅 기간 폴링";
          }
        } catch (e) {
          alert("오류: " + e.message);
          btn.disabled = false; btn.innerHTML = "📅 기간 폴링";
        }
      }));
  }

  // ─── 영상 목록 ──────────────────────────────────────────────
  async function renderVideosPanel(container) {
    const res = await authFetch(`${API}/videos`);
    const videos = res.ok ? await res.json() : [];
    container.innerHTML = `
      <div class="src-table-wrap">
        <table class="src-table">
          <thead><tr>
            <th>상태</th><th>업로드</th><th>제목</th><th>타입</th>
            <th style="min-width:200px">진행률</th><th>에러</th><th>액션</th>
          </tr></thead>
          <tbody id="videos-tbody">
            ${videos.map(v => renderVideoRow(v)).join("") ||
              '<tr><td colspan="7" class="src-empty">수집된 영상이 없습니다</td></tr>'}
          </tbody>
        </table>
      </div>
      <div class="src-between" style="margin-top:10px">
        <p class="src-hint" style="margin:0;flex:1">
          ℹ️ <b>처리 시작</b> = 자동 자막 → 보정 → 제품 추출 · <b>자막 업로드</b> = IP 차단 시 수동 업로드 경로
        </p>
        <button id="diag-cookies-btn" class="src-btn src-btn-indigo src-btn-sm">🔍 쿠키 진단</button>
      </div>`;

    bindVideoRowActions(container);

    const diagBtn = document.getElementById("diag-cookies-btn");
    if (diagBtn) diagBtn.addEventListener("click", () => openDiagnosticsModal());
  }

  function renderVideoRow(v) {
    const canProcess = v.processed_status === 'pending' || v.processed_status === 'failed';
    const btnLabel = v.processed_status === 'failed' ? '재시도' : '처리 시작';
    const pubLabel = formatUploadDate(v.published_at);
    const prog = stepToProgress(v.internal_step, v.processed_status);
    const showProgress = v.processed_status === 'in_progress' || v.processed_status === 'done';

    return `
      <tr data-row-vid="${v.id}">
        <td>${statusBadge(v.processed_status, v.retry_count)}</td>
        <td class="src-mono" style="font-size:12px;white-space:nowrap">${escape(pubLabel)}</td>
        <td>
          <a href="https://www.youtube.com/watch?v=${escape(v.video_id)}" target="_blank" rel="noopener">
            ${escape(v.title || v.video_id)}
          </a>
        </td>
        <td class="src-muted" style="font-size:12px">${escape(v.video_type)}</td>
        <td data-cell-progress>
          ${showProgress ? progressBarHtml(prog.pct, prog.label, prog.cls)
                         : `<span class="src-muted" style="font-size:12px">${escape(v.internal_step || "-")}</span>`}
        </td>
        <td style="color:#991b1b;font-size:12px;max-width:200px;overflow:hidden;text-overflow:ellipsis" title="${escape(v.error_reason || '')}">
          ${escape(v.error_reason || "")}
        </td>
        <td style="white-space:nowrap">
          ${canProcess
            ? `<button data-process="${v.id}" class="src-btn src-btn-primary src-btn-sm">${btnLabel}</button>
               <button data-upload-transcript="${v.id}" data-title="${escape(v.title||'')}" data-videoid="${escape(v.video_id)}"
                       class="src-btn src-btn-sm" title="IP 차단 시 수동 업로드">📋 업로드</button>`
            : v.processed_status === 'done'
              ? '<span style="color:#065f46;font-size:11px;font-weight:600">✓ 완료</span>'
              : v.processed_status === 'in_progress'
                ? `<span class="src-spinner"></span><span style="font-size:11px;color:#1e40af">진행 중</span>` : ''}
        </td>
      </tr>`;
  }

  function bindVideoRowActions(container) {
    container.querySelectorAll("button[data-process]").forEach((btn) =>
      btn.addEventListener("click", async () => {
        if (!confirm("이 영상을 처리하시겠습니까? 15~60초 소요됩니다.")) return;
        const vid = btn.dataset.process;
        startVideoProcess(vid, container);
      }));

    container.querySelectorAll("button[data-upload-transcript]").forEach((btn) =>
      btn.addEventListener("click", () => {
        openUploadTranscriptModal(
          btn.dataset.uploadTranscript,
          btn.dataset.videoid,
          btn.dataset.title || "",
          () => renderVideosPanel(container),
        );
      }));
  }

  // 영상 처리 시작 → 진행률 폴링
  async function startVideoProcess(vid, container) {
    const row = container.querySelector(`tr[data-row-vid="${vid}"]`);
    if (!row) return;

    // 상태 셀 즉시 업데이트
    const progCell = row.querySelector("[data-cell-progress]");
    if (progCell) progCell.innerHTML = progressBarHtml(15, "처리 요청 전송 중... (15%)", "");

    // 액션 버튼 비활성화
    row.querySelectorAll("button").forEach(b => { b.disabled = true; });

    // 폴링 시작 (서버 응답 전에 시작 — 실제 단계 전환 반영)
    const pollTimer = pollVideoProgress(vid, container);

    try {
      const r = await authFetch(`${API}/videos/${vid}/process`, { method: "POST" });
      const data = await r.json().catch(() => ({}));
      // 폴링 타이머는 자신이 완료/실패 감지 시 자동 종료
      clearInterval(pollTimer);
      // 최종 결과 반영
      if (r.ok) {
        showToast(data.message || "처리 완료", "success");
      } else {
        showToast("처리 실패: " + (data.detail || r.status), "error");
      }
    } catch (e) {
      clearInterval(pollTimer);
      showToast("오류: " + e.message, "error");
    } finally {
      // 행 재렌더링으로 최종 상태 표시
      renderVideosPanel(container);
    }
  }

  // 2.5초 간격으로 internal_step 조회 → 프로그레스 업데이트
  function pollVideoProgress(vid, container) {
    const timer = setInterval(async () => {
      try {
        const r = await authFetch(`${API}/videos`);
        if (!r.ok) return;
        const videos = await r.json();
        const v = videos.find(x => String(x.id) === String(vid));
        if (!v) return;
        const row = container.querySelector(`tr[data-row-vid="${vid}"]`);
        if (!row) { clearInterval(timer); return; }
        const progCell = row.querySelector("[data-cell-progress]");
        if (!progCell) return;
        const prog = stepToProgress(v.internal_step, v.processed_status);
        progCell.innerHTML = progressBarHtml(prog.pct, prog.label, prog.cls);
        if (v.processed_status === "done" || v.processed_status === "failed") {
          clearInterval(timer);
        }
      } catch (_) { /* 폴링 에러는 무시 */ }
    }, 2500);
    return timer;
  }

  // ─── 진단 모달 ──────────────────────────────────────────────
  async function openDiagnosticsModal() {
    const { body } = buildModal("🔍 YouTube 쿠키·네트워크 진단", "로딩 중...", { width: "760px" });

    try {
      const [cookieRes, ytRes] = await Promise.all([
        authFetch(`${API}/diagnostics/cookies`).then(r => r.ok ? r.json() : { error: r.status }),
        authFetch(`${API}/diagnostics/test-youtube`, { method: "POST" }).then(r => r.ok ? r.json() : { error: r.status }),
      ]);

      const statusColors = {
        ok: "src-banner-success", no_cookies_configured: "src-banner-error",
        no_auth_cookies: "src-banner-error", expired: "src-banner-warn",
        parse_error: "src-banner-error", unknown: "src-banner-info",
      };
      const statusLabels = {
        ok: "✅ 정상",
        no_cookies_configured: "❌ 쿠키 미설정",
        no_auth_cookies: "❌ 인증 쿠키 없음",
        expired: "⚠️ 만료됨",
        parse_error: "❌ 파싱 실패",
        unknown: "❓ 알 수 없음",
      };
      const st = cookieRes.status || "unknown";
      const envEntries = cookieRes.env || {};

      body.innerHTML = `
        <section style="margin-bottom:20px">
          <h4 style="margin:0 0 10px;font-size:14px">🍪 쿠키 설정 상태</h4>
          <div class="src-banner ${statusColors[st] || 'src-banner-info'}">
            <b>${statusLabels[st] || st}</b>
            ${cookieRes.hint ? `<br>${escape(cookieRes.hint)}` : ""}
          </div>
          <table class="src-table" style="margin-top:10px">
            <tbody>
              <tr><td class="src-muted" style="width:42%">YOUTUBE_COOKIES_FILE 설정</td><td>${envEntries.YOUTUBE_COOKIES_FILE_set ? '✓ ' + escape(envEntries.YOUTUBE_COOKIES_FILE_value||'') : '—'}</td></tr>
              <tr><td class="src-muted">YOUTUBE_COOKIES_TEXT 길이</td><td>${envEntries.YOUTUBE_COOKIES_TEXT_length||0}자</td></tr>
              <tr><td class="src-muted">Webshare 프록시</td><td>${envEntries.YT_TRANSCRIPT_PROXY_configured ? '✓ 설정됨' : '—'}</td></tr>
              <tr><td class="src-muted">Resolved 쿠키 경로</td><td class="src-mono">${escape(cookieRes.resolved_path||'—')}</td></tr>
              <tr><td class="src-muted">파일 존재</td><td>${cookieRes.path_exists ? '✓' : '✗'}</td></tr>
              <tr><td class="src-muted">총 쿠키 수</td><td>${cookieRes.total_cookies ?? '—'}</td></tr>
              <tr><td class="src-muted">YouTube/Google 도메인</td><td>${cookieRes.youtube_domain_cookies ?? '—'}</td></tr>
              <tr><td class="src-muted">인증 쿠키 발견</td><td class="src-mono" style="font-size:11px">${(cookieRes.auth_cookies_found||[]).join(', ') || '—'}</td></tr>
              <tr><td class="src-muted">만료된 인증 쿠키</td><td style="color:#991b1b">${(cookieRes.auth_cookies_expired||[]).join(', ') || '—'}</td></tr>
            </tbody>
          </table>
        </section>

        <section style="margin-bottom:20px">
          <h4 style="margin:0 0 10px;font-size:14px">🌐 youtube.com 접속 테스트</h4>
          ${ytRes.ok ? `
          <table class="src-table">
            <tbody>
              <tr><td class="src-muted" style="width:42%">HTTP 상태</td><td>${ytRes.http_status}</td></tr>
              <tr><td class="src-muted">최종 URL</td><td class="src-mono" style="font-size:11px;word-break:break-all">${escape(ytRes.final_url||'')}</td></tr>
              <tr><td class="src-muted">consent 페이지 리다이렉트</td><td>${ytRes.redirected_to_consent ? '⚠️ 예 (비로그인)' : '아니오'}</td></tr>
              <tr><td class="src-muted">로그인 상태 감지</td><td>${ytRes.looks_logged_in ? '✅ 로그인됨' : '❌ 비로그인'}</td></tr>
              <tr><td class="src-muted">봇 탐지 페이지</td><td>${ytRes.has_bot_guard ? '⚠️ 감지됨 (IP 차단 가능성)' : '아니오'}</td></tr>
              <tr><td class="src-muted">응답 크기</td><td>${(ytRes.response_bytes||0).toLocaleString()} bytes</td></tr>
              <tr><td class="src-muted">전송된 쿠키 수</td><td>${ytRes.cookies_sent||0}</td></tr>
            </tbody>
          </table>` : `
          <div class="src-banner src-banner-error">에러: ${escape(ytRes.error||'unknown')}</div>
          `}
        </section>

        <details style="margin-bottom:16px" class="src-muted">
          <summary style="cursor:pointer;font-size:12px">📦 Raw JSON</summary>
          <pre class="src-mono" style="background:#f9fafb;padding:10px;border-radius:6px;overflow:auto;margin-top:6px;font-size:10px">${escape(JSON.stringify({ cookies: cookieRes, youtube: ytRes }, null, 2))}</pre>
        </details>

        <section style="padding-top:16px;border-top:1px solid #e5e7eb">
          <h4 style="margin:0 0 10px;font-size:14px">🎯 자막 수집 경로 테스트</h4>
          <p class="src-hint" style="margin:0 0 10px">
            <b>🌟 Gemini</b>: IP 차단 무관 (Google이 YouTube 직접 처리, 추천) · <b>transcript-api</b>: 기존 경로
          </p>
          <div class="src-row" style="margin-bottom:12px">
            <input id="diag-vid" type="text" class="src-input src-input-mono src-grow"
              placeholder="영상 ID (예: gZPdX8NRv24)" value="gZPdX8NRv24" style="min-width:200px">
            <button id="diag-test-gemini" class="src-btn src-btn-success src-btn-sm">🌟 Gemini 테스트</button>
            <button id="diag-test-transcript" class="src-btn src-btn-primary src-btn-sm">transcript-api 테스트</button>
          </div>
          <div id="diag-transcript-result"></div>
        </section>

        <div class="src-banner src-banner-info" style="margin-top:16px">
          <b>💡 해결 가이드 (진단 결과 기반)</b><br>
          ${ytRes.ok && ytRes.looks_logged_in === false && (cookieRes.status === 'ok') ?
            `<span style="color:#991b1b"><b>⚠️ 현재 상황</b>: 쿠키는 정상이지만 "비로그인" 감지 = Render 데이터센터 IP가 차단되어 쿠키만으론 부족합니다.</span><br>
             <b>→ 다음 단계</b>: <b>Gemini</b> 사용 권장 (IP 차단 무관)` :
            `❌ <b>쿠키 미설정</b> → Render Environment 에서 YOUTUBE_COOKIES_TEXT 추가<br>
             ❌ <b>인증 쿠키 없음</b> → youtube.com 에 로그인한 상태에서 쿠키 재추출<br>
             ⚠️ <b>만료됨</b> → 브라우저에서 쿠키 새로 추출<br>
             ⚠️ <b>봇 탐지 페이지</b> → Gemini 경로 사용 권장`}
        </div>
      `;

      const vidInput = body.querySelector("#diag-vid");
      const testResult = body.querySelector("#diag-transcript-result");

      body.querySelector("#diag-test-gemini").addEventListener("click", async (ev) => {
        const btn = ev.currentTarget;
        const vid = (vidInput.value || "").trim() || "gZPdX8NRv24";
        btn.disabled = true; btn.innerHTML = `<span class="src-spinner"></span>처리 중...`;
        testResult.innerHTML = `<div class="src-muted" style="padding:8px;font-size:12px">🌟 Gemini API 호출 중... (30초~3분 소요)</div>`;
        try {
          const r = await authFetch(`${API}/diagnostics/test-gemini`, {
            method: "POST", body: JSON.stringify({ video_id: vid }),
          });
          const d = await r.json();
          if (d.ok) {
            testResult.innerHTML = `
              <div class="src-banner src-banner-success" style="margin:0">
                <b>✅ Gemini 전사 성공!</b><br>
                모델: ${escape(d.model)} · ${d.chars}자 · ${(d.latency_ms||0)/1000}s<br>
                토큰: in ${(d.input_tokens||0).toLocaleString()} / out ${(d.output_tokens||0).toLocaleString()}<br>
                <small>발췌: ${escape((d.first_300_chars||'')+'...')}</small><br>
                <div style="margin-top:6px">💡 ${escape(d.hint||'')}</div>
              </div>`;
          } else {
            testResult.innerHTML = `
              <div class="src-banner src-banner-error" style="margin:0">
                <b>❌ Gemini 실패</b><br>
                <pre class="src-mono" style="white-space:pre-wrap;font-size:11px;max-height:160px;overflow:auto;background:#fff;padding:8px;border-radius:4px;margin:6px 0">${escape(d.error||'')}</pre>
                <div style="color:#1e40af">💡 ${escape(d.hint||'')}</div>
              </div>`;
          }
        } catch (e) {
          testResult.innerHTML = `<div class="src-banner src-banner-error">요청 실패: ${escape(e.message)}</div>`;
        } finally {
          btn.disabled = false; btn.innerHTML = "🌟 Gemini 테스트";
        }
      });

      body.querySelector("#diag-test-transcript").addEventListener("click", async (ev) => {
        const btn = ev.currentTarget;
        const vid = (vidInput.value || "").trim() || "gZPdX8NRv24";
        btn.disabled = true; btn.innerHTML = `<span class="src-spinner"></span>테스트 중...`;
        testResult.innerHTML = `<div class="src-muted" style="padding:8px;font-size:12px">📡 youtube-transcript-api 호출 중... (최대 30초)</div>`;
        try {
          const r = await authFetch(`${API}/diagnostics/test-transcript`, {
            method: "POST", body: JSON.stringify({ video_id: vid }),
          });
          const d = await r.json();
          if (d.ok) {
            testResult.innerHTML = `
              <div class="src-banner src-banner-success" style="margin:0">
                <b>✅ 자막 수집 성공!</b><br>
                언어: ${escape(d.lang)} · 세그먼트: ${d.segments_count}개 · ${d.cleaned_chars}자<br>
                쿠키 사용: ${d.cookies_used ? '✓' : '—'} · 프록시 사용: ${d.proxy_used ? '✓' : '—'}<br>
                <small>발췌: ${escape((d.first_200_chars||'')+'...')}</small>
              </div>`;
          } else {
            testResult.innerHTML = `
              <div class="src-banner src-banner-error" style="margin:0">
                <b>❌ 실패</b> ${d.ip_blocked_detected?' · <span>IP 차단 감지됨</span>':''}<br>
                <pre class="src-mono" style="white-space:pre-wrap;font-size:11px;max-height:160px;overflow:auto;background:#fff;padding:8px;border-radius:4px;margin:6px 0">${escape(d.error||'')}</pre>
                <div style="color:#1e40af">💡 ${escape(d.hint||'')}</div>
              </div>`;
          }
        } catch (e) {
          testResult.innerHTML = `<div class="src-banner src-banner-error">요청 실패: ${escape(e.message)}</div>`;
        } finally {
          btn.disabled = false; btn.innerHTML = "transcript-api 테스트";
        }
      });
    } catch (e) {
      body.innerHTML = `<div class="src-banner src-banner-error">진단 실패: ${escape(e.message)}</div>`;
    }
  }

  // ─── 자막 수동 업로드 모달 ──────────────────────────────────
  function openUploadTranscriptModal(vid, ytId, title, onDone) {
    const downsubUrl = `https://downsub.com/?url=https://www.youtube.com/watch?v=${ytId}`;
    const savesubsUrl = `https://savesubs.com/process?url=https://www.youtube.com/watch?v=${ytId}`;
    const html = `
      <p style="font-size:13px;margin:0 0 8px">
        <b>영상</b>: ${escape(title || ytId)}
      </p>
      <div class="src-banner src-banner-warn">
        <b>왜 이 기능이 필요한가요?</b><br>
        Render 서버 IP가 YouTube에 일시 차단되면 자동 자막 수집이 실패합니다.
        대신 본인 PC 브라우저에서 아래 서비스로 자막 파일을 받아
        텍스트 박스에 붙여넣으면 자동으로 보정·제품 추출이 이어집니다.
      </div>
      <p style="font-weight:600;font-size:13px;margin:12px 0 6px">1️⃣ 자막 다운로드 (브라우저에서)</p>
      <div class="src-row" style="margin-bottom:12px">
        <a href="${downsubUrl}" target="_blank" rel="noopener"
           class="src-btn src-btn-indigo src-btn-sm" style="text-decoration:none">🔗 DownSub.com에서 열기</a>
        <a href="${savesubsUrl}" target="_blank" rel="noopener"
           class="src-btn src-btn-indigo src-btn-sm" style="text-decoration:none">🔗 SaveSubs.com에서 열기</a>
      </div>
      <p class="src-hint" style="margin:0 0 12px">
        ↑ 한국어 자막을 <b>SRT 형식</b>으로 다운받거나 <b>Plain Text</b>로 복사해주세요.
      </p>
      <p style="font-weight:600;font-size:13px;margin:12px 0 6px">2️⃣ 아래에 붙여넣기</p>
      <textarea id="transcript-input" rows="10" class="src-input src-input-mono"
        placeholder="여기에 SRT 또는 평문 자막을 붙여넣어주세요&#10;&#10;예:&#10;1&#10;00:00:00,000 --> 00:00:05,000&#10;알리에서 실패없는 꿀템들 소개합니다"
        style="width:100%;resize:vertical"></textarea>
      <div class="src-between" style="margin-top:12px">
        <span id="char-count" class="src-muted" style="font-size:12px">0자 / 최소 200자</span>
        <div>
          <button id="do-cancel" class="src-btn">취소</button>
          <button id="do-upload" class="src-btn src-btn-primary">📤 이 자막으로 처리하기</button>
        </div>
      </div>
    `;
    const { body, close } = buildModal("📋 자막 수동 업로드", html, { width: "720px" });

    const ta = body.querySelector("#transcript-input");
    const cnt = body.querySelector("#char-count");
    ta.addEventListener("input", () => {
      const n = ta.value.length;
      cnt.textContent = `${n.toLocaleString()}자 / 최소 200자`;
      cnt.style.color = n >= 200 ? "#065f46" : "#6b7280";
    });

    body.querySelector("#do-cancel").addEventListener("click", close);

    body.querySelector("#do-upload").addEventListener("click", async (ev) => {
      const raw = ta.value.trim();
      if (raw.length < 200) {
        alert("자막이 너무 짧습니다. 최소 200자 이상 붙여넣어주세요.");
        return;
      }
      const btn = ev.currentTarget;
      btn.disabled = true; btn.innerHTML = `<span class="src-spinner"></span>처리 중...`;
      try {
        const r = await authFetch(`${API}/videos/${vid}/upload-transcript`, {
          method: "POST", body: JSON.stringify({ raw_transcript: raw }),
        });
        const data = await r.json().catch(() => ({}));
        if (r.ok) {
          showToast(data.message || "처리 완료!", "success");
          close();
          if (onDone) onDone();
        } else {
          alert("처리 실패: " + (data.detail || r.status));
          btn.disabled = false; btn.innerHTML = "📤 이 자막으로 처리하기";
        }
      } catch (e) {
        alert("오류: " + e.message);
        btn.disabled = false; btn.innerHTML = "📤 이 자막으로 처리하기";
      }
    });
  }

  // ─── 제품 목록 ──────────────────────────────────────────────
  async function renderProductsPanel(container) {
    const res = await authFetch(`${API}/products`);
    const products = res.ok ? await res.json() : [];
    container.innerHTML = `
      <div class="src-card-grid">
        ${products.map(p => {
          const persona = (p.target_persona && p.target_persona.label) || "";
          return `
          <div class="src-card" data-product="${p.id}">
            ${p.thumbnail_url ? `<img src="${escape(p.thumbnail_url)}" class="src-card-thumb" alt="">` : ""}
            <h4>${escape(p.product_name)}</h4>
            <div class="src-card-meta">${escape(p.brand || "[?]")} · ${escape(p.category)}</div>
            ${persona ? `<span class="src-chip">${escape(persona)}</span>` : ""}
            <div class="src-card-meta src-muted" style="font-size:11px">상태: ${escape(p.sourcing_status)}</div>
            <div class="src-card-btns">
              <button class="src-btn src-btn-sm" data-act="analyze"    data-id="${p.id}">🔍 시장성</button>
              <button class="src-btn src-btn-sm" data-act="marketing"  data-id="${p.id}" data-kind="b2c">📄 B2C</button>
              <button class="src-btn src-btn-sm" data-act="marketing"  data-id="${p.id}" data-kind="b2b">📄 B2B</button>
              <button class="src-btn src-btn-sm" data-act="marketing"  data-id="${p.id}" data-kind="influencer">📄 인플루언서</button>
              <button class="src-btn src-btn-sm" data-act="findinf"    data-id="${p.id}">🎥 인플루언서 찾기</button>
              <button class="src-btn src-btn-sm src-btn-indigo" data-act="view"  data-id="${p.id}">📊 상세</button>
            </div>
          </div>`;
        }).join("") || '<div class="src-empty">추출된 제품이 없습니다</div>'}
      </div>
      <p class="src-hint">
        ℹ️ 순서: 시장성 분석 → 마케팅 자료 → 인플루언서 찾기 → 인플루언서 컨택 탭에서 초안 생성 ·
        <b>"분석 결과" 탭</b>에서 저장된 분석 내용을 언제든 다시 확인할 수 있습니다.
      </p>`;

    container.querySelectorAll("button[data-act]").forEach((btn) =>
      btn.addEventListener("click", () => handleProductAction(btn, container)));
  }

  async function handleProductAction(btn, container) {
    const pid = btn.dataset.id;
    const act = btn.dataset.act;
    const origHtml = btn.innerHTML;
    btn.disabled = true; btn.innerHTML = `<span class="src-spinner"></span>실행 중...`;
    try {
      if (act === "analyze") {
        const r = await authFetch(`${API}/products/${pid}/analyze`, { method: "POST" });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          showToast(`✓ 시장성 분석 완료 (v${d.version}) — 분석 결과 탭에서 확인하세요`, "success");
        } else {
          showToast("시장성 분석 실패: " + (d.detail || r.status), "error");
        }
      } else if (act === "marketing") {
        const kind = btn.dataset.kind;
        const r = await authFetch(`${API}/products/${pid}/marketing`, {
          method: "POST", body: JSON.stringify({ kind }),
        });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          const warn = d.needs_human_review ? ` ⚠️ 검토 필요` : ``;
          showToast(`✓ ${kind.toUpperCase()} 자료 생성 완료${warn}`, "success");
        } else {
          showToast("자료 생성 실패: " + (d.detail || r.status), "error");
        }
      } else if (act === "findinf") {
        const r = await authFetch(`${API}/products/${pid}/find-influencers`, { method: "POST" });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          showToast(`✓ 인플루언서 ${d.accepted}명 추출 — 컨택 탭에서 초안 생성`, "success");
        } else {
          showToast("인플루언서 검색 실패: " + (d.detail || r.status), "error");
        }
      } else if (act === "view") {
        // "분석 결과" 탭으로 이동
        window.__sourcingGotoAnalysis && window.__sourcingGotoAnalysis(pid);
      }
    } finally {
      btn.disabled = false; btn.innerHTML = origHtml;
    }
  }

  // ─── 분석 결과 탭 (신규) ─────────────────────────────────
  let _analysisPreselectPid = null;

  async function renderAnalysisPanel(container) {
    container.innerHTML = `
      <div class="src-analysis-layout">
        <aside class="src-product-list" id="analysis-product-list">
          <div class="src-muted" style="padding:12px;font-size:12px">제품 목록 로드 중...</div>
        </aside>
        <section class="src-analysis-content" id="analysis-content">
          <div class="src-empty">← 좌측 제품을 선택하세요</div>
        </section>
      </div>
    `;

    const listEl = container.querySelector("#analysis-product-list");
    const contentEl = container.querySelector("#analysis-content");

    const r = await authFetch(`${API}/products`);
    const products = r.ok ? await r.json() : [];

    if (!products.length) {
      listEl.innerHTML = `<div class="src-empty" style="padding:20px;font-size:12px">추출된 제품이 없습니다</div>`;
      return;
    }

    // 제품별 최신 분석 존재 여부를 미리 확인 (has-analysis 마크용)
    // → 병렬 호출이지만 제품 수가 많으면 비용이 크므로 렌더링 후 첫 제품 선택 시만 호출
    listEl.innerHTML = products.map(p => `
      <div class="src-product-item" data-pid="${p.id}">
        <div class="src-product-name">${escape(p.product_name)}</div>
        <div class="src-product-meta">${escape(p.brand || "[?]")} · ${escape(p.category)}</div>
      </div>
    `).join("");

    listEl.querySelectorAll(".src-product-item").forEach(el =>
      el.addEventListener("click", () => {
        listEl.querySelectorAll(".src-product-item").forEach(x => x.classList.remove("active"));
        el.classList.add("active");
        loadAnalysisFor(el.dataset.pid, contentEl, el);
      }));

    // 딥 링크 (제품 탭 "📊 상세" 에서 넘어온 경우)
    const targetPid = _analysisPreselectPid || (products[0] && products[0].id);
    _analysisPreselectPid = null;
    const targetEl = listEl.querySelector(`.src-product-item[data-pid="${targetPid}"]`);
    if (targetEl) {
      targetEl.classList.add("active");
      loadAnalysisFor(targetPid, contentEl, targetEl);
    }
  }

  async function loadAnalysisFor(pid, contentEl, listItemEl) {
    contentEl.innerHTML = `<div class="src-muted" style="padding:20px"><span class="src-spinner"></span>분석 결과 로딩 중...</div>`;

    try {
      const [latest, assets, matches] = await Promise.all([
        authFetch(`${API}/products/${pid}/market-latest`).then(r => r.ok ? r.json() : null).catch(() => null),
        authFetch(`${API}/products/${pid}/marketing`).then(r => r.ok ? r.json() : []).catch(() => []),
        authFetch(`${API}/products/${pid}/matches`).then(r => r.ok ? r.json() : []).catch(() => []),
      ]);

      // 사이드바에 has-analysis 마크
      if (listItemEl && latest && latest.version) listItemEl.classList.add("has-analysis");

      const hasMarket = latest && latest.version;
      const priceLo = (latest && latest.recommended_price_range_krw && latest.recommended_price_range_krw.low) || 0;
      const priceHi = (latest && latest.recommended_price_range_krw && latest.recommended_price_range_krw.high) || 0;
      const risks = (latest && latest.risk_factors) || [];

      contentEl.innerHTML = `
        <section>
          <h3>📊 시장성 분석 ${hasMarket ? `<small class="src-muted">· v${latest.version}</small>` : ""}</h3>
          ${hasMarket ? `
            <div class="src-score-grid">
              <div class="src-score-item">
                <div class="src-score-label">시장 규모</div>
                <div class="src-score-value">${latest.market_size_score}/5</div>
              </div>
              <div class="src-score-item">
                <div class="src-score-label">경쟁도</div>
                <div class="src-score-value">${latest.competition_score}/5</div>
              </div>
            </div>

            <h4>기회 요약</h4>
            <div class="src-markdown">${escape(latest.opportunity_summary || "(없음)")}</div>

            <h4>추천 가격 범위</h4>
            <div class="src-price-range">
              <b>₩${priceLo.toLocaleString()} ~ ₩${priceHi.toLocaleString()}</b>
            </div>

            ${risks.length ? `
              <h4>⚠️ 리스크 요인</h4>
              <ul class="src-risk-list">
                ${risks.map(r => `<li>${escape(r)}</li>`).join("")}
              </ul>
            ` : ""}

            <div class="src-row" style="margin-top:14px">
              <button class="src-btn src-btn-sm src-btn-indigo" data-analyze-again="${pid}">🔄 재분석</button>
              <button class="src-btn src-btn-sm" data-history="${pid}">📜 이력 보기</button>
            </div>
          ` : `
            <div class="src-banner src-banner-info">
              아직 시장성 분석이 없습니다.
              <button class="src-btn src-btn-sm src-btn-primary" data-analyze-again="${pid}" style="margin-left:10px">🔍 시장성 분석 실행</button>
            </div>
          `}
        </section>

        <section>
          <h3>📄 마케팅 자료 <small class="src-muted">· ${assets.length}건</small></h3>
          ${assets.length ? assets.map(a => `
            <div class="src-asset-card" data-asset-id="${a.id}">
              <span class="src-asset-kind" data-kind="${escape(a.kind)}">${escape(a.kind)}</span>
              <span class="src-asset-title">${escape(a.title || "(제목 없음)")}</span>
              <span class="src-asset-date src-muted">${escape(a.created_at || "")}</span>
              <button class="src-btn src-btn-sm" data-open-asset="${a.id}">📖 보기</button>
            </div>
          `).join("") : `
            <div class="src-banner src-banner-info">
              아직 생성된 마케팅 자료가 없습니다.
              <span class="src-muted">제품 탭에서 B2C / B2B / 인플루언서 자료를 생성해주세요.</span>
            </div>
          `}
        </section>

        <section>
          <h3>🎯 매칭된 인플루언서 <small class="src-muted">· ${matches.length}명</small></h3>
          ${matches.length ? `
            <div class="src-table-wrap">
              <table class="src-table">
                <thead><tr><th>채널</th><th>구독자</th><th>품질점수</th><th>추정 단가</th></tr></thead>
                <tbody>
                  ${matches.slice(0, 10).map(m => `
                    <tr>
                      <td>${escape(m.display_name || m.handle)}</td>
                      <td>${(m.follower_count || 0).toLocaleString()}</td>
                      <td>${m.quality_score || 0}/100</td>
                      <td>${m.estimated_quote_krw ? `₩${m.estimated_quote_krw.toLocaleString()}` : "-"}</td>
                    </tr>
                  `).join("")}
                </tbody>
              </table>
            </div>
            ${matches.length > 10 ? `<p class="src-hint">상위 10명만 표시됨. 전체 목록은 "인플루언서 컨택" 탭에서 확인.</p>` : ""}
          ` : `
            <div class="src-banner src-banner-info src-muted">아직 매칭된 인플루언서가 없습니다.</div>
          `}
        </section>
      `;

      // 재분석 / 자료 열기 / 이력 바인딩
      contentEl.querySelectorAll("button[data-analyze-again]").forEach(b =>
        b.addEventListener("click", async () => {
          if (!confirm("시장성 분석을 다시 실행합니다. 계속할까요?")) return;
          b.disabled = true; b.innerHTML = `<span class="src-spinner"></span>분석 중...`;
          try {
            const r = await authFetch(`${API}/products/${pid}/analyze`, { method: "POST" });
            if (r.ok) {
              showToast("✓ 재분석 완료", "success");
              loadAnalysisFor(pid, contentEl, listItemEl);
            } else {
              const d = await r.json().catch(() => ({}));
              showToast("재분석 실패: " + (d.detail || r.status), "error");
              b.disabled = false; b.innerHTML = "🔄 재분석";
            }
          } catch (e) {
            showToast("오류: " + e.message, "error");
            b.disabled = false; b.innerHTML = "🔄 재분석";
          }
        }));

      contentEl.querySelectorAll("button[data-open-asset]").forEach(b =>
        b.addEventListener("click", () => openAssetDetail(b.dataset.openAsset)));

      contentEl.querySelectorAll("button[data-history]").forEach(b =>
        b.addEventListener("click", () => openMarketHistory(pid)));

    } catch (e) {
      contentEl.innerHTML = `<div class="src-banner src-banner-error">로딩 실패: ${escape(e.message)}</div>`;
    }
  }

  async function openAssetDetail(aid) {
    const r = await authFetch(`${API}/marketing/${aid}`);
    if (!r.ok) { showToast("자료를 불러올 수 없습니다", "error"); return; }
    const a = await r.json();
    const html = `
      <div class="src-row" style="margin-bottom:10px">
        <span class="src-asset-kind" data-kind="${escape(a.kind)}">${escape(a.kind)}</span>
        <span class="src-muted" style="font-size:12px">${escape(a.created_at || "")}</span>
        ${a.needs_human_review ? `<span class="src-badge src-badge-failed">⚠️ 검토 필요</span>` : ""}
      </div>
      <h4 style="margin:0 0 10px">${escape(a.title || "(제목 없음)")}</h4>
      <div class="src-markdown" id="asset-markdown-body">${escape(a.body_markdown || a.body || "(본문 없음)")}</div>
      ${a.review_reasons && a.review_reasons.length ? `
        <div class="src-banner src-banner-warn" style="margin-top:10px">
          <b>검토 사유:</b><br>
          ${a.review_reasons.map(r => `• ${escape(r)}`).join("<br>")}
        </div>
      ` : ""}
      <div class="src-between" style="margin-top:14px">
        <span class="src-muted" style="font-size:12px">마크다운 형식 · 복사하여 Notion/Google Docs 등에 붙여넣으세요</span>
        <button id="copy-asset-btn" class="src-btn src-btn-primary src-btn-sm">📋 본문 복사</button>
      </div>
    `;
    const { body, close } = buildModal(`📄 마케팅 자료 #${aid}`, html, { width: "760px" });
    body.querySelector("#copy-asset-btn").addEventListener("click", async () => {
      const text = (a.title ? `# ${a.title}\n\n` : "") + (a.body_markdown || a.body || "");
      try {
        await navigator.clipboard.writeText(text);
        showToast("✓ 클립보드에 복사되었습니다", "success");
      } catch (_) {
        prompt("복사가 차단되어 수동 복사가 필요합니다:", text);
      }
    });
  }

  async function openMarketHistory(pid) {
    const r = await authFetch(`${API}/products/${pid}/market-history`);
    if (!r.ok) { showToast("이력 로딩 실패", "error"); return; }
    const history = await r.json();
    const html = history.length ? `
      <div class="src-table-wrap">
        <table class="src-table">
          <thead><tr><th>버전</th><th>생성일</th><th>시장 규모</th><th>경쟁도</th><th>요약</th></tr></thead>
          <tbody>
            ${history.map(h => `
              <tr>
                <td>v${h.version}</td>
                <td class="src-muted" style="font-size:12px">${escape(h.created_at || "")}</td>
                <td>${h.market_size_score}/5</td>
                <td>${h.competition_score}/5</td>
                <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escape(h.opportunity_summary || '')}">${escape(h.opportunity_summary || "")}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    ` : `<div class="src-empty">이력이 없습니다</div>`;
    buildModal(`📜 시장성 분석 이력 (제품 #${pid})`, html, { width: "820px" });
  }

  // ─── 인플루언서 컨택 초안 (자동 발송 없음) ──────────────────
  async function renderInfluencerPanel(container) {
    const [products, drafts] = await Promise.all([
      authFetch(`${API}/products`).then(r => r.ok ? r.json() : []),
      authFetch(`${API}/outreach-drafts`).then(r => r.ok ? r.json() : []),
    ]);
    container.innerHTML = `
      <div class="src-banner src-banner-warn">
        ⚠️ 이 시스템은 메일·DM을 <b>자동으로 발송하지 않습니다</b>.
        초안을 클립보드에 복사한 뒤 사람이 직접 발송하세요.
      </div>

      <section style="margin-bottom:24px">
        <h4 style="margin:0 0 10px;font-size:14px">제품별 매칭된 인플루언서</h4>
        <div class="src-row" style="margin-bottom:12px">
          <label class="src-muted" style="font-size:13px">제품 선택:</label>
          <select id="pick-product" class="src-select" style="min-width:280px">
            <option value="">-- 제품을 선택하세요 --</option>
            ${products.map(p => `<option value="${p.id}">${escape(p.product_name)}</option>`).join("")}
          </select>
        </div>
        <div id="matches-list" class="src-muted" style="padding:12px;font-size:13px">제품을 선택하면 매칭 목록이 표시됩니다</div>
      </section>

      <section>
        <h4 style="margin:16px 0 10px;font-size:14px">생성된 초안 <small class="src-muted">· ${drafts.length}건</small></h4>
        <div class="src-table-wrap">
          <table class="src-table">
            <thead><tr>
              <th>#</th><th>채널</th><th>유형</th><th>제목</th><th>상태</th><th>액션</th>
            </tr></thead>
            <tbody>
              ${drafts.map(d => `
                <tr data-draft="${d.id}">
                  <td>${d.id}</td>
                  <td>${escape(d.channel_kind)}</td>
                  <td>${escape(d.offer_kind)}</td>
                  <td>${escape(d.subject || "(제목 없음 · DM)")}</td>
                  <td>${escape(d.status)}</td>
                  <td style="white-space:nowrap">
                    <button data-copy="${d.id}" class="src-btn src-btn-sm src-btn-primary">📋 복사</button>
                    <button data-status="${d.id}" class="src-btn src-btn-sm">상태 변경</button>
                  </td>
                </tr>`).join("") || '<tr><td colspan="6" class="src-empty">생성된 초안이 없습니다</td></tr>'}
            </tbody>
          </table>
        </div>
      </section>`;

    document.getElementById("pick-product").addEventListener("change", async (e) => {
      const pid = e.target.value;
      const listEl = document.getElementById("matches-list");
      if (!pid) { listEl.innerHTML = "제품을 선택하면 매칭 목록이 표시됩니다"; return; }
      listEl.innerHTML = `<span class="src-spinner"></span>로딩 중...`;
      const mr = await authFetch(`${API}/products/${pid}/matches`);
      const matches = mr.ok ? await mr.json() : [];
      if (!matches.length) {
        listEl.innerHTML = '<div class="src-banner src-banner-info">아직 매칭이 없습니다. "제품" 탭에서 "🎥 인플루언서 찾기"를 먼저 실행하세요.</div>';
        return;
      }
      listEl.innerHTML = `
        <div class="src-table-wrap">
          <table class="src-table">
            <thead><tr>
              <th>채널</th><th>구독/뷰/ER</th><th>품질점수</th><th>추정 단가</th><th>초안 생성</th>
            </tr></thead>
            <tbody>
              ${matches.map(m => {
                const disabled = m.is_excluded ? "disabled" : "";
                const quote = m.estimated_quote_krw ? `₩${m.estimated_quote_krw.toLocaleString()}` : "-";
                return `
                <tr>
                  <td>${escape(m.display_name || m.handle)}<br><small class="src-muted">${escape(m.handle)}</small></td>
                  <td style="font-size:12px">
                    구독 ${(m.follower_count||0).toLocaleString()} /
                    뷰 ${(m.avg_views||0).toLocaleString()} /
                    ER ${m.engagement_rate||0}%
                  </td>
                  <td>${m.quality_score||0}/100</td>
                  <td>${quote}</td>
                  <td style="white-space:nowrap">
                    <button class="src-btn src-btn-sm" data-draft-for="${m.id}" data-ch="email" data-offer="gift"   ${disabled}>📧 무상</button>
                    <button class="src-btn src-btn-sm" data-draft-for="${m.id}" data-ch="email" data-offer="paid"   ${disabled}>💼 유료</button>
                    <button class="src-btn src-btn-sm" data-draft-for="${m.id}" data-ch="instagram_dm" data-offer="gift" ${disabled}>💬 DM</button>
                  </td>
                </tr>`;
              }).join("")}
            </tbody>
          </table>
        </div>
        <p class="src-hint">💡 단가는 CPM 기반 추정치 (±50% 협상 범위)</p>
      `;

      listEl.querySelectorAll("button[data-draft-for]").forEach(btn =>
        btn.addEventListener("click", async () => {
          btn.disabled = true;
          const orig = btn.innerHTML;
          btn.innerHTML = `<span class="src-spinner"></span>`;
          try {
            const r = await authFetch(`${API}/matches/${btn.dataset.draftFor}/outreach-draft`, {
              method: "POST",
              body: JSON.stringify({ channel_kind: btn.dataset.ch, offer_kind: btn.dataset.offer }),
            });
            const d = await r.json().catch(() => ({}));
            if (r.ok) {
              showToast(`✓ 초안 생성 완료 (#${d.draft_id})`, "success");
              renderInfluencerPanel(container);
            } else {
              showToast("초안 생성 실패: " + (d.detail || r.status), "error");
              btn.disabled = false; btn.innerHTML = orig;
            }
          } catch (e) {
            showToast("오류: " + e.message, "error");
            btn.disabled = false; btn.innerHTML = orig;
          }
        }));
    });

    const draftsById = Object.fromEntries(drafts.map(d => [String(d.id), d]));
    container.querySelectorAll("button[data-copy]").forEach(btn =>
      btn.addEventListener("click", async () => {
        const d = draftsById[btn.dataset.copy];
        const text = d.subject ? `제목: ${d.subject}\n\n${d.message_body}` : d.message_body;
        try {
          await navigator.clipboard.writeText(text);
          await authFetch(`${API}/outreach-drafts/${d.id}/mark-copied`, { method: "POST" });
          showToast("✓ 클립보드에 복사되었습니다", "success");
        } catch (_) {
          prompt("복사가 차단되어 수동 복사가 필요합니다:", text);
        }
      }));
    container.querySelectorAll("button[data-status]").forEach(btn =>
      btn.addEventListener("click", async () => {
        const id = btn.dataset.status;
        const next = prompt("상태 (sent/replied/agreed/published/settled/declined):");
        if (!next) return;
        const note = prompt("비고 (선택):") || null;
        await authFetch(`${API}/outreach-drafts/${id}`, {
          method: "PATCH", body: JSON.stringify({ status: next, note }),
        });
        renderInfluencerPanel(container);
      }));
  }

  // ─── 상위 라우터 ───────────────────────────────────────────
  function initSourcingPage() {
    const root = document.getElementById("sourcing-root");
    if (!root) return;
    root.innerHTML = `
      <div class="src-wrap">
        <div class="src-subtabs">
          <button class="src-subtab active" data-sub="dashboard">📊 대시보드</button>
          <button class="src-subtab" data-sub="channels">📺 채널 관리</button>
          <button class="src-subtab" data-sub="videos">🎬 영상 목록</button>
          <button class="src-subtab" data-sub="products">📦 제품</button>
          <button class="src-subtab" data-sub="analysis">🔬 분석 결과</button>
          <button class="src-subtab" data-sub="influencer">🎯 인플루언서 컨택</button>
        </div>
        <div id="sourcing-content"></div>
      </div>
    `;
    const content = root.querySelector("#sourcing-content");
    const show = (sub) => {
      root.querySelectorAll(".src-subtab").forEach(b =>
        b.classList.toggle("active", b.dataset.sub === sub));
      if (sub === "dashboard")         renderDashboard(content);
      else if (sub === "channels")     renderChannelsPanel(content);
      else if (sub === "videos")       renderVideosPanel(content);
      else if (sub === "products")     renderProductsPanel(content);
      else if (sub === "analysis")     renderAnalysisPanel(content);
      else if (sub === "influencer")   renderInfluencerPanel(content);
    };
    root.querySelectorAll(".src-subtab").forEach(b =>
      b.addEventListener("click", () => show(b.dataset.sub)));

    // 제품 탭 "📊 상세" 에서 분석 탭으로 점프하는 hook
    window.__sourcingGotoAnalysis = (pid) => {
      _analysisPreselectPid = pid;
      show("analysis");
    };

    show("dashboard");
  }

  window.initSourcingPage = initSourcingPage;
})();
