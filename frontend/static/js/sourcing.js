/**
 * sourcing.js — 신제품 소싱 탭 프론트엔드 (v0.3.0)
 *
 * order-agent의 api.js와 호환되는 authFetch shim을 포함.
 * 호출: navigateTo('sourcing') 시 window.initSourcingPage()
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

  // ─── 상태 배지 (4단계) ──────────────────────────────────────
  const STATUS_BADGE = {
    pending:     { icon: "🆕", label: "대기",  bg: "#fef3c7", fg: "#92400e" },
    in_progress: { icon: "🔄", label: "처리중", bg: "#dbeafe", fg: "#1e40af" },
    done:        { icon: "✅", label: "완료",  bg: "#d1fae5", fg: "#065f46" },
    failed:      { icon: "⚠️", label: "실패",  bg: "#fee2e2", fg: "#991b1b" },
  };
  function statusBadge(status, retry) {
    const b = STATUS_BADGE[status] || STATUS_BADGE.pending;
    const r = retry > 0 ? ` (재시도 ${retry})` : "";
    return `<span style="display:inline-block;padding:2px 10px;border-radius:12px;
      font-size:11px;font-weight:600;background:${b.bg};color:${b.fg}">
      ${b.icon} ${b.label}${r}</span>`;
  }

  function escape(s) {
    return String(s || "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  // 2026-04-19T12:00:00Z / 2026-04-19 12:00:00 → "2026-04-19" (공백 시 "-")
  function formatUploadDate(iso) {
    if (!iso) return "-";
    const s = String(iso);
    // YYYY-MM-DD 부분만 추출
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    return m ? `${m[1]}-${m[2]}-${m[3]}` : s.slice(0, 10);
  }

  // ─── 대시보드 ───────────────────────────────────────────────
  async function renderDashboard(container) {
    try {
      const res = await authFetch(`${API}/dashboard`);
      if (!res.ok) {
        container.innerHTML = `<div class="muted">대시보드를 불러올 수 없습니다. (${res.status})</div>`;
        return;
      }
      const d = await res.json();
      const rate = (d.hit_rate.hit_rate * 100).toFixed(1);
      container.innerHTML = `
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:20px">
          ${card("오늘 신규 영상", d.today_videos)}
          ${card("오늘 신규 제품", d.today_products)}
          ${card("대기 영상", d.pending_videos)}
          ${card("실패 재시도", d.failed_videos)}
          ${card("진행 중 컨택", d.active_outreach_drafts)}
          ${card("소싱 적중률", `${rate}%`, `${d.hit_rate.hits}/${d.hit_rate.total_purchased}`)}
        </div>
        <p style="color:#6b7280;font-size:13px">
          ※ 적중 기준: 30일 매출 ≥ ₩${(d.hit_rate.threshold_revenue_krw / 10000).toFixed(0)}만원
        </p>`;
    } catch (e) {
      container.innerHTML = `<div class="muted">오류: ${escape(e.message)}</div>`;
    }
  }
  function card(label, value, sub = "") {
    return `
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px 16px">
        <div style="font-size:11px;color:#6b7280">${label}</div>
        <div style="font-size:22px;font-weight:700;color:#111827;margin-top:2px">${value}</div>
        ${sub ? `<div style="font-size:11px;color:#9ca3af">${sub}</div>` : ""}
      </div>`;
  }

  // ─── 채널 관리 ──────────────────────────────────────────────
  async function renderChannelsPanel(container) {
    const res = await authFetch(`${API}/channels`);
    const channels = res.ok ? await res.json() : [];
    container.innerHTML = `
      <form id="chan-form" style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">
        <input id="chan-url" type="text"
          placeholder="YouTube URL / @handle / UCxxx / 영상 URL"
          required
          style="flex:1;min-width:280px;padding:8px 10px;border:1px solid #d1d5db;border-radius:6px">
        <input id="chan-cat" type="text" placeholder="카테고리 (예: 알리추천)"
          style="width:180px;padding:8px 10px;border:1px solid #d1d5db;border-radius:6px">
        <button type="submit" class="btn-primary" style="padding:8px 16px">추가</button>
      </form>
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead><tr style="background:#f9fafb;text-align:left">
          <th style="padding:8px">#</th>
          <th style="padding:8px">핸들/ID</th>
          <th style="padding:8px">제목</th>
          <th style="padding:8px">카테고리</th>
          <th style="padding:8px">마지막 폴링</th>
          <th style="padding:8px">액션</th>
        </tr></thead>
        <tbody>
          ${channels.map(c => `
            <tr style="border-top:1px solid #e5e7eb">
              <td style="padding:8px">${c.id}</td>
              <td style="padding:8px">${escape(c.channel_handle || c.channel_id)}</td>
              <td style="padding:8px">${escape(c.channel_title || "(미확인)")}</td>
              <td style="padding:8px">${escape(c.category || "")}</td>
              <td style="padding:8px">${escape(c.last_polled_at || "미실행")}</td>
              <td style="padding:8px;white-space:nowrap">
                <button data-action="poll" data-id="${c.id}" class="btn-small" style="margin-right:4px">즉시 폴링</button>
                <button data-action="poll-period" data-id="${c.id}" class="btn-small">기간 폴링</button>
              </td>
            </tr>`).join("") || '<tr><td colspan="6" style="padding:20px;color:#9ca3af;text-align:center">등록된 채널이 없습니다</td></tr>'}
        </tbody>
      </table>
      <p style="color:#6b7280;font-size:12px;margin-top:8px">
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
      if (r.ok) { renderChannelsPanel(container); }
      else alert("채널 추가 실패: " + (await r.text()));
    });

    // ─ 즉시 폴링 ─
    container.querySelectorAll("button[data-action='poll']").forEach((btn) =>
      btn.addEventListener("click", async () => {
        btn.disabled = true; btn.textContent = "수집 중...";
        try {
          const r = await authFetch(`${API}/channels/${btn.dataset.id}/poll`, { method: "POST" });
          const data = await r.json().catch(() => ({}));
          if (r.ok) {
            alert(data.message || "폴링 완료");
            renderChannelsPanel(container);  // 마지막 폴링·제목 갱신
          } else {
            alert("폴링 실패: " + (data.detail || r.status));
          }
        } finally {
          btn.disabled = false; btn.textContent = "즉시 폴링";
        }
      }));

    // ─ 기간 폴링 ─
    container.querySelectorAll("button[data-action='poll-period']").forEach((btn) =>
      btn.addEventListener("click", async () => {
        const today = new Date().toISOString().slice(0, 10);
        const weekAgo = new Date(Date.now() - 7*24*60*60*1000).toISOString().slice(0, 10);
        const start = prompt("시작일 (YYYY-MM-DD)", weekAgo);
        if (!start) return;
        const end = prompt("종료일 (YYYY-MM-DD)", today);
        if (!end) return;
        btn.disabled = true; btn.textContent = "수집 중...";
        try {
          const r = await authFetch(`${API}/channels/${btn.dataset.id}/poll-period`, {
            method: "POST",
            body: JSON.stringify({ start, end }),
          });
          const data = await r.json().catch(() => ({}));
          if (r.ok) {
            alert(data.message || "기간 폴링 완료");
            renderChannelsPanel(container);
          } else {
            alert("기간 폴링 실패: " + (data.detail || r.status));
          }
        } finally {
          btn.disabled = false; btn.textContent = "기간 폴링";
        }
      }));
  }

  // ─── 영상 목록 ──────────────────────────────────────────────
  async function renderVideosPanel(container) {
    const res = await authFetch(`${API}/videos`);
    const videos = res.ok ? await res.json() : [];
    container.innerHTML = `
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead><tr style="background:#f9fafb;text-align:left">
          <th style="padding:8px">상태</th>
          <th style="padding:8px">업로드</th>
          <th style="padding:8px">제목</th>
          <th style="padding:8px">타입</th>
          <th style="padding:8px">단계</th>
          <th style="padding:8px">에러</th>
          <th style="padding:8px">액션</th>
        </tr></thead>
        <tbody>
          ${videos.map(v => {
            const canProcess = v.processed_status === 'pending' || v.processed_status === 'failed';
            const btnLabel = v.processed_status === 'failed' ? '재시도' : '처리 시작';
            const pubLabel = formatUploadDate(v.published_at);
            return `
            <tr style="border-top:1px solid #e5e7eb">
              <td style="padding:8px">${statusBadge(v.processed_status, v.retry_count)}</td>
              <td style="padding:8px;white-space:nowrap;color:#374151;font-size:12px">${escape(pubLabel)}</td>
              <td style="padding:8px">
                <a href="https://www.youtube.com/watch?v=${escape(v.video_id)}" target="_blank"
                   style="color:#2563eb;text-decoration:none">${escape(v.title || v.video_id)}</a>
              </td>
              <td style="padding:8px">${escape(v.video_type)}</td>
              <td style="padding:8px;color:#6b7280">${escape(v.internal_step || "-")}</td>
              <td style="padding:8px;color:#991b1b;font-size:12px">${escape(v.error_reason || "")}</td>
              <td style="padding:8px;white-space:nowrap">
                ${canProcess
                  ? `<button data-process="${v.id}" class="btn-small" style="margin-right:4px">${btnLabel}</button>
                     <button data-upload-transcript="${v.id}" data-title="${escape((v.title||'').replace(/"/g,'&quot;'))}" data-videoid="${escape(v.video_id)}" class="btn-small" title="IP 차단 시 수동 업로드">📋 자막 업로드</button>`
                  : v.processed_status === 'done' ? '<span style="color:#065f46;font-size:11px">✓</span>' : ''}
              </td>
            </tr>`;
          }).join("") || '<tr><td colspan="7" style="padding:20px;color:#9ca3af;text-align:center">수집된 영상이 없습니다</td></tr>'}
        </tbody>
      </table>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;gap:8px;flex-wrap:wrap">
        <p style="color:#6b7280;font-size:12px;margin:0;flex:1">
          ℹ️ <b>처리 시작</b> = 자동 자막 → 보정 → 제품 추출 · <b>자막 업로드</b> = IP 차단 시 수동 업로드 경로
        </p>
        <button id="diag-cookies-btn" class="btn-small" style="background:#eef2ff;color:#4338ca">🔍 쿠키 진단</button>
      </div>`;

    // 영상 처리 트리거
    container.querySelectorAll("button[data-process]").forEach((btn) =>
      btn.addEventListener("click", async () => {
        if (!confirm("이 영상을 처리하시겠습니까? 15~60초 소요됩니다.")) return;
        btn.disabled = true; btn.textContent = "처리 중...";
        try {
          const r = await authFetch(`${API}/videos/${btn.dataset.process}/process`, { method: "POST" });
          const data = await r.json().catch(() => ({}));
          if (r.ok) {
            alert(data.message || "처리 완료");
          } else {
            alert("처리 실패: " + (data.detail || r.status));
          }
        } finally {
          renderVideosPanel(container);
        }
      }));

    // 자막 수동 업로드 (IP 차단 우회)
    container.querySelectorAll("button[data-upload-transcript]").forEach((btn) =>
      btn.addEventListener("click", () => {
        openUploadTranscriptModal(
          btn.dataset.uploadTranscript,
          btn.dataset.videoid,
          btn.dataset.title || "",
          () => renderVideosPanel(container),
        );
      }));

    // 쿠키 진단 버튼
    const diagBtn = document.getElementById("diag-cookies-btn");
    if (diagBtn) {
      diagBtn.addEventListener("click", () => openDiagnosticsModal());
    }
  }

  // ─── 진단 모달 ──────────────────────────────────────────────
  async function openDiagnosticsModal() {
    const modal = document.createElement("div");
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;" +
      "align-items:center;justify-content:center;z-index:9999;padding:20px";
    modal.innerHTML = `
      <div style="background:#fff;border-radius:10px;max-width:720px;width:100%;max-height:90vh;display:flex;flex-direction:column">
        <div style="padding:18px 24px;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center">
          <h3 style="margin:0;font-size:16px">🔍 YouTube 쿠키·네트워크 진단</h3>
          <button style="background:none;border:none;font-size:22px;cursor:pointer;color:#6b7280"
                  onclick="this.closest('[style*=fixed]').remove()">×</button>
        </div>
        <div id="diag-content" style="padding:20px 24px;overflow:auto;font-size:13px">
          로딩 중...
        </div>
      </div>`;
    document.body.appendChild(modal);
    const content = modal.querySelector("#diag-content");

    try {
      const [cookieRes, ytRes] = await Promise.all([
        authFetch(`${API}/diagnostics/cookies`).then(r => r.ok ? r.json() : { error: r.status }),
        authFetch(`${API}/diagnostics/test-youtube`, { method: "POST" }).then(r => r.ok ? r.json() : { error: r.status }),
      ]);

      const statusColors = {
        ok: "#065f46", no_cookies_configured: "#991b1b",
        no_auth_cookies: "#991b1b", expired: "#92400e",
        parse_error: "#991b1b", unknown: "#6b7280",
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

      content.innerHTML = `
        <section style="margin-bottom:20px">
          <h4 style="margin:0 0 10px">🍪 쿠키 설정 상태</h4>
          <div style="padding:10px 14px;background:#f9fafb;border-left:4px solid ${statusColors[st]||'#d1d5db'};border-radius:4px">
            <div style="font-weight:600;color:${statusColors[st]||'#111'};margin-bottom:4px">${statusLabels[st]||st}</div>
            ${cookieRes.hint ? `<div style="color:#374151">${escape(cookieRes.hint)}</div>` : ""}
          </div>
          <table style="width:100%;margin-top:12px;font-size:12px;border-collapse:collapse">
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">YOUTUBE_COOKIES_FILE 설정</td><td style="padding:6px">${envEntries.YOUTUBE_COOKIES_FILE_set ? '✓ ' + escape(envEntries.YOUTUBE_COOKIES_FILE_value||'') : '—'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">YOUTUBE_COOKIES_TEXT 길이</td><td style="padding:6px">${envEntries.YOUTUBE_COOKIES_TEXT_length||0}자</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">Webshare 프록시</td><td style="padding:6px">${envEntries.YT_TRANSCRIPT_PROXY_configured ? '✓ 설정됨' : '—'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">Resolved 쿠키 경로</td><td style="padding:6px;font-family:monospace">${escape(cookieRes.resolved_path||'—')}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">파일 존재</td><td style="padding:6px">${cookieRes.path_exists ? '✓' : '✗'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">총 쿠키 수</td><td style="padding:6px">${cookieRes.total_cookies ?? '—'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">YouTube/Google 도메인</td><td style="padding:6px">${cookieRes.youtube_domain_cookies ?? '—'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">인증 쿠키 발견</td><td style="padding:6px;font-family:monospace;font-size:11px">${(cookieRes.auth_cookies_found||[]).join(', ') || '—'}</td></tr>
            <tr><td style="padding:6px;color:#6b7280">만료된 인증 쿠키</td><td style="padding:6px;color:#991b1b">${(cookieRes.auth_cookies_expired||[]).join(', ') || '—'}</td></tr>
          </table>
        </section>

        <section>
          <h4 style="margin:0 0 10px">🌐 youtube.com 접속 테스트</h4>
          ${ytRes.ok ? `
          <table style="width:100%;font-size:12px;border-collapse:collapse">
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">HTTP 상태</td><td style="padding:6px">${ytRes.http_status}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">최종 URL</td><td style="padding:6px;word-break:break-all;font-family:monospace;font-size:11px">${escape(ytRes.final_url||'')}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">consent 페이지 리다이렉트</td><td style="padding:6px">${ytRes.redirected_to_consent ? '⚠️ 예 (비로그인)' : '아니오'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">로그인 상태 감지</td><td style="padding:6px">${ytRes.looks_logged_in ? '✅ 로그인됨' : '❌ 비로그인'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">봇 탐지 페이지</td><td style="padding:6px">${ytRes.has_bot_guard ? '⚠️ 감지됨 (IP 차단 가능성)' : '아니오'}</td></tr>
            <tr style="border-bottom:1px solid #e5e7eb"><td style="padding:6px;color:#6b7280">응답 크기</td><td style="padding:6px">${(ytRes.response_bytes||0).toLocaleString()} bytes</td></tr>
            <tr><td style="padding:6px;color:#6b7280">전송된 쿠키 수</td><td style="padding:6px">${ytRes.cookies_sent||0}</td></tr>
          </table>` : `
          <div style="padding:10px;background:#fef2f2;color:#991b1b;border-radius:4px">에러: ${escape(ytRes.error||'unknown')}</div>
          `}
        </section>

        <details style="margin-top:16px;color:#6b7280;font-size:11px">
          <summary style="cursor:pointer">📦 Raw JSON</summary>
          <pre style="background:#f9fafb;padding:10px;border-radius:4px;overflow:auto;margin-top:6px;font-size:10px">${escape(JSON.stringify({ cookies: cookieRes, youtube: ytRes }, null, 2))}</pre>
        </details>

        <section style="margin-top:20px;padding-top:16px;border-top:1px solid #e5e7eb">
          <h4 style="margin:0 0 10px">🎯 자막 API 실제 호출 테스트</h4>
          <p style="color:#6b7280;font-size:12px;margin:0 0 8px">
            youtube-transcript-api 를 직접 호출해 어디서 실패하는지 확인합니다.
          </p>
          <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <input id="diag-vid" type="text" placeholder="영상 ID (예: gZPdX8NRv24)"
              value="gZPdX8NRv24"
              style="flex:1;min-width:200px;padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;font-family:monospace">
            <button id="diag-test-transcript" class="btn-small" style="background:#2563eb;color:#fff;padding:6px 14px">테스트 실행</button>
          </div>
          <div id="diag-transcript-result" style="font-size:12px"></div>
        </section>

        <div style="margin-top:20px;padding:12px 16px;background:#eff6ff;border-radius:6px;font-size:12px;line-height:1.7;color:#1e40af">
          <b>💡 해결 가이드 (진단 결과 기반)</b><br>
          ${ytRes.ok && ytRes.looks_logged_in === false && (cookieRes.status === 'ok') ?
            `<span style="color:#991b1b"><b>⚠️ 현재 상황</b>: 쿠키는 정상이지만 "비로그인" 감지 = Render 데이터센터 IP가 차단되어 쿠키만으론 부족합니다.</span><br>
             <b>→ 다음 단계</b>: <b>Webshare 프록시</b> 가입 (무료 플랜 있음) — 아래 가이드 참고` :
            `❌ <b>쿠키 미설정</b> → Render Environment 에서 YOUTUBE_COOKIES_TEXT 추가<br>
             ❌ <b>인증 쿠키 없음</b> → youtube.com 에 로그인한 상태에서 쿠키 재추출<br>
             ⚠️ <b>만료됨</b> → 브라우저에서 쿠키 새로 추출<br>
             ⚠️ <b>봇 탐지 페이지</b> → Webshare 프록시 필요`}
        </div>

        <details style="margin-top:14px;font-size:12px" open>
          <summary style="cursor:pointer;color:#4338ca;font-weight:600;padding:8px">🌐 Webshare 프록시 3분 가이드 (권장 해결책)</summary>
          <div style="padding:10px 14px;background:#f9fafb;border-radius:6px;line-height:1.8;margin-top:6px">
            <b>왜 필요한가?</b> 쿠키가 있어도 Render 공용 IP는 YouTube 블랙리스트. 주거용 IP 프록시로 우회 필요.<br><br>
            <b>단계:</b><br>
            ① <a href="https://www.webshare.io/" target="_blank" rel="noopener" style="color:#4338ca;text-decoration:underline">webshare.io</a> 가입 (무료 — 10 프록시, 1GB/월)<br>
            ② Dashboard → Proxy → <b>"Residential" 플랜 선택</b> (Datacenter 아님)<br>
            ③ Proxy Settings 에서 <b>Username / Password</b> 확인<br>
            ④ Render Environment → 추가:<br>
            &nbsp;&nbsp;&nbsp;<code style="background:#fff;padding:2px 6px;border-radius:3px;border:1px solid #e5e7eb">YT_TRANSCRIPT_PROXY_USERNAME</code> = (webshare username)<br>
            &nbsp;&nbsp;&nbsp;<code style="background:#fff;padding:2px 6px;border-radius:3px;border:1px solid #e5e7eb">YT_TRANSCRIPT_PROXY_PASSWORD</code> = (webshare password)<br>
            ⑤ Save → 재배포 → 이 진단 버튼으로 다시 확인<br><br>
            <b>💰 비용</b>: 무료 플랜은 한 달 ~100영상. 초과 시 $3.5/월부터.<br>
            <b>🔒 보안</b>: 환경변수는 Render 내부에서 암호화 저장.
          </div>
        </details>
      `;

      // 자막 API 테스트 버튼 바인딩
      const testBtn = modal.querySelector("#diag-test-transcript");
      const testResult = modal.querySelector("#diag-transcript-result");
      const vidInput = modal.querySelector("#diag-vid");
      if (testBtn) testBtn.addEventListener("click", async () => {
        const vid = (vidInput.value || "").trim() || "gZPdX8NRv24";
        testBtn.disabled = true; testBtn.textContent = "테스트 중...";
        testResult.innerHTML = `<div style="color:#6b7280;padding:8px">📡 youtube-transcript-api 호출 중... (최대 30초)</div>`;
        try {
          const r = await authFetch(`${API}/diagnostics/test-transcript`, {
            method: "POST",
            body: JSON.stringify({ video_id: vid }),
          });
          const d = await r.json();
          if (d.ok) {
            testResult.innerHTML = `
              <div style="padding:10px 14px;background:#d1fae5;border-left:4px solid #065f46;border-radius:4px;margin-top:4px">
                <b style="color:#065f46">✅ 자막 수집 성공!</b><br>
                언어: ${escape(d.lang)} · 세그먼트: ${d.segments_count}개 · ${d.cleaned_chars}자<br>
                쿠키 사용: ${d.cookies_used ? '✓' : '—'} · 프록시 사용: ${d.proxy_used ? '✓' : '—'}<br>
                <small style="color:#374151">발췌: ${escape((d.first_200_chars||'')+'...')}</small>
              </div>`;
          } else {
            testResult.innerHTML = `
              <div style="padding:10px 14px;background:#fef2f2;border-left:4px solid #991b1b;border-radius:4px;margin-top:4px">
                <b style="color:#991b1b">❌ 실패</b> ${d.ip_blocked_detected?' · <span style="color:#991b1b">IP 차단 감지됨</span>':''}<br>
                <pre style="white-space:pre-wrap;font-size:11px;max-height:160px;overflow:auto;background:#fff;padding:8px;border-radius:4px;margin:6px 0;border:1px solid #e5e7eb">${escape(d.error||'')}</pre>
                <div style="color:#1e40af">💡 ${escape(d.hint||'')}</div>
              </div>`;
          }
        } catch (e) {
          testResult.innerHTML = `<div style="color:#991b1b">요청 실패: ${escape(e.message)}</div>`;
        } finally {
          testBtn.disabled = false; testBtn.textContent = "테스트 실행";
        }
      });
    } catch (e) {
      content.innerHTML = `<div style="color:#991b1b">진단 실패: ${escape(e.message)}</div>`;
    }
  }

  // ─── 자막 수동 업로드 모달 ──────────────────────────────────
  function openUploadTranscriptModal(vid, ytId, title, onDone) {
    const modal = document.createElement("div");
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;" +
      "align-items:center;justify-content:center;z-index:9999;padding:20px";
    const downsubUrl = `https://downsub.com/?url=https://www.youtube.com/watch?v=${ytId}`;
    const savesubsUrl = `https://savesubs.com/process?url=https://www.youtube.com/watch?v=${ytId}`;
    modal.innerHTML = `
      <div style="background:#fff;border-radius:10px;max-width:720px;width:100%;max-height:90vh;display:flex;flex-direction:column">
        <div style="padding:20px 24px;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center">
          <h3 style="margin:0;font-size:16px">📋 자막 수동 업로드</h3>
          <button style="background:none;border:none;font-size:22px;cursor:pointer;color:#6b7280" onclick="this.closest('[style*=fixed]').remove()">×</button>
        </div>
        <div style="padding:20px 24px;overflow:auto">
          <p style="color:#374151;font-size:13px;margin:0 0 8px">
            <b>영상</b>: ${escape(title || ytId)}
          </p>
          <div style="background:#fef3c7;border-left:4px solid #f59e0b;padding:10px 14px;margin:12px 0;border-radius:4px;font-size:13px;line-height:1.6">
            <b>왜 이 기능이 필요한가요?</b><br>
            Render 서버 IP가 YouTube에 일시 차단되면 자동 자막 수집이 실패합니다.
            대신 본인 PC 브라우저에서 아래 서비스로 자막 파일을 받아
            텍스트 박스에 붙여넣으면 자동으로 보정·제품 추출이 이어집니다.
          </div>
          <p style="font-weight:600;font-size:13px;margin:12px 0 6px">1️⃣ 자막 다운로드 (브라우저에서)</p>
          <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">
            <a href="${downsubUrl}" target="_blank" rel="noopener"
               style="padding:6px 12px;background:#eef2ff;color:#4338ca;border-radius:6px;text-decoration:none;font-size:13px">
               🔗 DownSub.com에서 열기</a>
            <a href="${savesubsUrl}" target="_blank" rel="noopener"
               style="padding:6px 12px;background:#eef2ff;color:#4338ca;border-radius:6px;text-decoration:none;font-size:13px">
               🔗 SaveSubs.com에서 열기</a>
          </div>
          <p style="color:#6b7280;font-size:12px;margin:0 0 12px">
            ↑ 한국어 자막을 <b>SRT 형식</b>으로 다운받거나 <b>Plain Text</b>로 복사해주세요.
          </p>
          <p style="font-weight:600;font-size:13px;margin:12px 0 6px">2️⃣ 아래에 붙여넣기</p>
          <textarea id="transcript-input" rows="10"
            placeholder="여기에 SRT 또는 평문 자막을 붙여넣어주세요&#10;&#10;예:&#10;1&#10;00:00:00,000 --> 00:00:05,000&#10;알리에서 실패없는 꿀템들 소개합니다"
            style="width:100%;padding:10px;border:1px solid #d1d5db;border-radius:6px;font-family:monospace;font-size:12px;resize:vertical"></textarea>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px">
            <span id="char-count" style="color:#6b7280;font-size:12px">0자 / 최소 200자</span>
            <div>
              <button onclick="this.closest('[style*=fixed]').remove()"
                style="padding:8px 14px;background:#f3f4f6;border:1px solid #d1d5db;border-radius:6px;cursor:pointer;margin-right:6px">취소</button>
              <button id="do-upload"
                style="padding:8px 16px;background:#2563eb;color:#fff;border:none;border-radius:6px;cursor:pointer">
                이 자막으로 처리하기
              </button>
            </div>
          </div>
        </div>
      </div>`;
    document.body.appendChild(modal);

    const ta = modal.querySelector("#transcript-input");
    const cnt = modal.querySelector("#char-count");
    ta.addEventListener("input", () => {
      const n = ta.value.length;
      cnt.textContent = `${n.toLocaleString()}자 / 최소 200자`;
      cnt.style.color = n >= 200 ? "#065f46" : "#6b7280";
    });

    modal.querySelector("#do-upload").addEventListener("click", async () => {
      const raw = ta.value.trim();
      if (raw.length < 200) {
        alert("자막이 너무 짧습니다. 최소 200자 이상 붙여넣어주세요.");
        return;
      }
      const btn = modal.querySelector("#do-upload");
      btn.disabled = true; btn.textContent = "처리 중...";
      try {
        const r = await authFetch(`${API}/videos/${vid}/upload-transcript`, {
          method: "POST",
          body: JSON.stringify({ raw_transcript: raw }),
        });
        const data = await r.json().catch(() => ({}));
        if (r.ok) {
          alert(data.message || "처리 완료!");
          modal.remove();
          if (onDone) onDone();
        } else {
          alert("처리 실패: " + (data.detail || r.status));
          btn.disabled = false; btn.textContent = "이 자막으로 처리하기";
        }
      } catch (e) {
        alert("오류: " + e.message);
        btn.disabled = false; btn.textContent = "이 자막으로 처리하기";
      }
    });
  }

  // ─── 제품 목록 ──────────────────────────────────────────────
  async function renderProductsPanel(container) {
    const res = await authFetch(`${API}/products`);
    const products = res.ok ? await res.json() : [];
    container.innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px">
        ${products.map(p => {
          const persona = (p.target_persona && p.target_persona.label) || "";
          return `
          <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px" data-product="${p.id}">
            ${p.thumbnail_url ? `<img src="${escape(p.thumbnail_url)}" style="width:100%;height:120px;object-fit:cover;border-radius:6px;margin-bottom:8px">` : ""}
            <h4 style="margin:0 0 4px;font-size:14px">${escape(p.product_name)}</h4>
            <p style="margin:0 0 6px;color:#6b7280;font-size:12px">${escape(p.brand || "[?]")} · ${escape(p.category)}</p>
            ${persona ? `<span style="display:inline-block;padding:2px 8px;background:#eef2ff;color:#4338ca;border-radius:10px;font-size:11px;margin-bottom:6px">${escape(persona)}</span>` : ""}
            <p style="margin:8px 0 6px;font-size:11px;color:#9ca3af">상태: ${escape(p.sourcing_status)}</p>
            <div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:8px">
              <button class="btn-small" data-act="analyze"    data-id="${p.id}">🔍 시장성</button>
              <button class="btn-small" data-act="marketing"  data-id="${p.id}" data-kind="b2c">📄 B2C</button>
              <button class="btn-small" data-act="marketing"  data-id="${p.id}" data-kind="b2b">📄 B2B</button>
              <button class="btn-small" data-act="marketing"  data-id="${p.id}" data-kind="influencer">📄 인플루언서 자료</button>
              <button class="btn-small" data-act="findinf"    data-id="${p.id}">🎥 인플루언서 찾기</button>
              <button class="btn-small" data-act="detail"     data-id="${p.id}">📊 상세</button>
            </div>
          </div>`;
        }).join("") || '<div style="color:#9ca3af;padding:40px;text-align:center">추출된 제품이 없습니다</div>'}
      </div>
      <p style="color:#6b7280;font-size:12px;margin-top:12px">
        ℹ️ 순서: 시장성 분석 → 마케팅 자료 → 인플루언서 찾기 → 인플루언서 컨택 탭에서 초안 생성
      </p>`;

    container.querySelectorAll("button[data-act]").forEach((btn) =>
      btn.addEventListener("click", () => handleProductAction(btn, container)));
  }

  async function handleProductAction(btn, container) {
    const pid = btn.dataset.id;
    const act = btn.dataset.act;
    const orig = btn.textContent;
    btn.disabled = true; btn.textContent = "실행 중...";
    try {
      if (act === "analyze") {
        const r = await authFetch(`${API}/products/${pid}/analyze`, { method: "POST" });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          alert(
            `✓ 시장성 분석 완료 (v${d.version})\n\n` +
            `시장 규모: ${d.market_size_score}/5\n` +
            `경쟁도: ${d.competition_score}/5\n\n` +
            `${d.opportunity_summary || ""}\n\n` +
            `추천 가격: ₩${(d.recommended_price_range_krw?.low||0).toLocaleString()} ~ ₩${(d.recommended_price_range_krw?.high||0).toLocaleString()}\n\n` +
            `⚠️ 리스크:\n• ${(d.risk_factors||[]).join("\n• ")}`
          );
        } else {
          alert("시장성 분석 실패: " + (d.detail || r.status));
        }
      } else if (act === "marketing") {
        const kind = btn.dataset.kind;
        const r = await authFetch(`${API}/products/${pid}/marketing`, {
          method: "POST", body: JSON.stringify({ kind }),
        });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          alert(
            `✓ ${kind.toUpperCase()} 자료 생성 완료\n\n` +
            `제목: ${d.title}\n\n` +
            (d.needs_human_review ? `⚠️ 사람 검토 필요: ${(d.review_reasons||[]).join("; ")}` : "검토 경고 없음")
          );
        } else {
          alert("자료 생성 실패: " + (d.detail || r.status));
        }
      } else if (act === "findinf") {
        const r = await authFetch(`${API}/products/${pid}/find-influencers`, { method: "POST" });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          alert(
            `✓ 인플루언서 ${d.accepted}명 추출\n\n` +
            `"인플루언서 컨택" 탭에서 초안을 생성하세요.`
          );
        } else {
          alert("인플루언서 검색 실패: " + (d.detail || r.status));
        }
      } else if (act === "detail") {
        await openProductDetail(pid);
      }
    } finally {
      btn.disabled = false; btn.textContent = orig;
    }
  }

  async function openProductDetail(pid) {
    const [latest, assets] = await Promise.all([
      authFetch(`${API}/products/${pid}/market-latest`).then(r => r.ok ? r.json() : {}),
      authFetch(`${API}/products/${pid}/marketing`).then(r => r.ok ? r.json() : []),
    ]);
    const modal = document.createElement("div");
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,.5);display:flex;" +
      "align-items:center;justify-content:center;z-index:9999";
    modal.innerHTML = `
      <div style="background:#fff;border-radius:10px;max-width:720px;max-height:85vh;overflow:auto;padding:24px;position:relative">
        <button style="position:absolute;top:10px;right:14px;background:none;border:none;font-size:24px;cursor:pointer"
                onclick="this.closest('[style*=fixed]').remove()">×</button>
        <h3>제품 #${pid} 상세</h3>
        <section><h4>📊 시장성 (최신)</h4>
          <pre style="background:#f9fafb;padding:10px;border-radius:6px;font-size:11px;max-height:200px;overflow:auto">${escape(JSON.stringify(latest, null, 2))}</pre>
        </section>
        <section><h4>📄 마케팅 자료 (${assets.length}건)</h4>
          <ul style="font-size:13px">${assets.map(a=>`<li>[${a.kind}] ${escape(a.title)} <small style="color:#9ca3af">(${a.created_at})</small></li>`).join("")||"<li style='color:#9ca3af'>없음</li>"}</ul>
        </section>
      </div>`;
    document.body.appendChild(modal);
  }

  // ─── 인플루언서 컨택 초안 (자동 발송 없음) ──────────────────
  async function renderInfluencerPanel(container) {
    const [products, drafts] = await Promise.all([
      authFetch(`${API}/products`).then(r => r.ok ? r.json() : []),
      authFetch(`${API}/outreach-drafts`).then(r => r.ok ? r.json() : []),
    ]);
    container.innerHTML = `
      <div style="background:#fef3c7;border-left:4px solid #f59e0b;padding:10px 14px;margin-bottom:16px;border-radius:6px">
        ⚠️ 이 시스템은 메일·DM을 <b>자동으로 발송하지 않습니다</b>.
        초안을 클립보드에 복사한 뒤 사람이 직접 발송하세요.
      </div>

      <section style="margin-bottom:20px">
        <h4 style="margin:0 0 8px">제품별 매칭된 인플루언서</h4>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px">
          <label style="font-size:13px;color:#6b7280">제품 선택:</label>
          <select id="pick-product" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;min-width:260px">
            <option value="">-- 제품을 선택하세요 --</option>
            ${products.map(p => `<option value="${p.id}">${escape(p.product_name)}</option>`).join("")}
          </select>
        </div>
        <div id="matches-list" style="color:#9ca3af;font-size:13px;padding:8px">제품을 선택하면 매칭 목록이 표시됩니다</div>
      </section>

      <section>
        <h4 style="margin:16px 0 8px">생성된 초안 (${drafts.length}건)</h4>
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead><tr style="background:#f9fafb;text-align:left">
            <th style="padding:8px">#</th>
            <th style="padding:8px">채널</th>
            <th style="padding:8px">유형</th>
            <th style="padding:8px">제목</th>
            <th style="padding:8px">상태</th>
            <th style="padding:8px">액션</th>
          </tr></thead>
          <tbody>
            ${drafts.map(d => `
              <tr style="border-top:1px solid #e5e7eb" data-draft="${d.id}">
                <td style="padding:8px">${d.id}</td>
                <td style="padding:8px">${escape(d.channel_kind)}</td>
                <td style="padding:8px">${escape(d.offer_kind)}</td>
                <td style="padding:8px">${escape(d.subject || "(제목 없음 · DM)")}</td>
                <td style="padding:8px">${escape(d.status)}</td>
                <td style="padding:8px">
                  <button data-copy="${d.id}" class="btn-small">📋 복사</button>
                  <button data-status="${d.id}" class="btn-small">상태 변경</button>
                </td>
              </tr>`).join("") || '<tr><td colspan="6" style="padding:20px;color:#9ca3af;text-align:center">생성된 초안이 없습니다</td></tr>'}
          </tbody>
        </table>
      </section>`;

    // 제품 선택 시 매칭 목록 로드
    document.getElementById("pick-product").addEventListener("change", async (e) => {
      const pid = e.target.value;
      const listEl = document.getElementById("matches-list");
      if (!pid) { listEl.textContent = "제품을 선택하면 매칭 목록이 표시됩니다"; return; }
      listEl.innerHTML = "로딩...";
      const mr = await authFetch(`${API}/products/${pid}/matches`);
      const matches = mr.ok ? await mr.json() : [];
      if (!matches.length) {
        listEl.innerHTML = '<div style="padding:12px;color:#9ca3af">아직 매칭이 없습니다. "제품" 탭에서 "🎥 인플루언서 찾기"를 먼저 실행하세요.</div>';
        return;
      }
      listEl.innerHTML = `
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead><tr style="background:#f9fafb;text-align:left">
            <th style="padding:8px">채널</th>
            <th style="padding:8px">구독/평균뷰/ER</th>
            <th style="padding:8px">품질점수</th>
            <th style="padding:8px">추정단가</th>
            <th style="padding:8px">초안 생성</th>
          </tr></thead>
          <tbody>
            ${matches.map(m => {
              const disabled = m.is_excluded ? "disabled" : "";
              const quote = m.estimated_quote_krw ? `₩${m.estimated_quote_krw.toLocaleString()}` : "-";
              return `
              <tr style="border-top:1px solid #e5e7eb">
                <td style="padding:8px">${escape(m.display_name || m.handle)}<br><small style="color:#9ca3af">${escape(m.handle)}</small></td>
                <td style="padding:8px;font-size:12px">
                  구독 ${(m.follower_count||0).toLocaleString()} /
                  뷰 ${(m.avg_views||0).toLocaleString()} /
                  ER ${m.engagement_rate||0}%
                </td>
                <td style="padding:8px">${m.quality_score||0}/100</td>
                <td style="padding:8px">${quote}</td>
                <td style="padding:8px;white-space:nowrap">
                  <button class="btn-small" data-draft-for="${m.id}" data-ch="email" data-offer="gift"   ${disabled}>📧 무상 메일</button>
                  <button class="btn-small" data-draft-for="${m.id}" data-ch="email" data-offer="paid"   ${disabled}>💼 유료 메일</button>
                  <button class="btn-small" data-draft-for="${m.id}" data-ch="instagram_dm" data-offer="gift" ${disabled}>💬 DM</button>
                </td>
              </tr>`;
            }).join("")}
          </tbody>
        </table>
        <p style="color:#6b7280;font-size:12px;margin-top:8px">💡 단가는 CPM 기반 추정치 (±50% 협상 범위)</p>
      `;

      listEl.querySelectorAll("button[data-draft-for]").forEach(btn =>
        btn.addEventListener("click", async () => {
          btn.disabled = true;
          try {
            const r = await authFetch(`${API}/matches/${btn.dataset.draftFor}/outreach-draft`, {
              method: "POST",
              body: JSON.stringify({ channel_kind: btn.dataset.ch, offer_kind: btn.dataset.offer }),
            });
            const d = await r.json().catch(() => ({}));
            if (r.ok) {
              alert(`✓ 초안 생성 완료 (#${d.draft_id})\n아래 '생성된 초안' 섹션에서 복사하세요.`);
              renderInfluencerPanel(container);
            } else {
              alert("초안 생성 실패: " + (d.detail || r.status));
            }
          } finally {
            btn.disabled = false;
          }
        }));
    });

    // Bind copy/status — we already have message_body in the list response
    const draftsById = Object.fromEntries(drafts.map(d => [String(d.id), d]));
    container.querySelectorAll("button[data-copy]").forEach(btn =>
      btn.addEventListener("click", async () => {
        const d = draftsById[btn.dataset.copy];
        const text = d.subject ? `제목: ${d.subject}\n\n${d.message_body}` : d.message_body;
        try {
          await navigator.clipboard.writeText(text);
          await authFetch(`${API}/outreach-drafts/${d.id}/mark-copied`, { method: "POST" });
          alert("클립보드에 복사되었습니다. 메일·DM에 붙여넣어 발송하세요.");
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
          method: "PATCH",
          body: JSON.stringify({ status: next, note }),
        });
        renderInfluencerPanel(container);
      }));
  }

  // ─── 상위 라우터 ───────────────────────────────────────────
  function initSourcingPage() {
    const root = document.getElementById("sourcing-root");
    if (!root) return;
    root.innerHTML = `
      <div style="display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid #e5e7eb">
        <button class="src-subtab active" data-sub="dashboard">대시보드</button>
        <button class="src-subtab" data-sub="channels">채널 관리</button>
        <button class="src-subtab" data-sub="videos">영상 목록</button>
        <button class="src-subtab" data-sub="products">제품</button>
        <button class="src-subtab" data-sub="influencer">인플루언서 컨택</button>
      </div>
      <div id="sourcing-content"></div>
      <style>
        .src-subtab {
          padding: 8px 16px; border: none; background: none; cursor: pointer;
          font-size: 13px; color: #6b7280; border-bottom: 2px solid transparent;
        }
        .src-subtab:hover { color: #374151; background: #f9fafb; }
        .src-subtab.active { color: #2563eb; border-bottom-color: #2563eb; font-weight: 600; }
      </style>`;
    const content = root.querySelector("#sourcing-content");
    const show = (sub) => {
      root.querySelectorAll(".src-subtab").forEach(b =>
        b.classList.toggle("active", b.dataset.sub === sub));
      if (sub === "dashboard")         renderDashboard(content);
      else if (sub === "channels")     renderChannelsPanel(content);
      else if (sub === "videos")       renderVideosPanel(content);
      else if (sub === "products")     renderProductsPanel(content);
      else if (sub === "influencer")   renderInfluencerPanel(content);
    };
    root.querySelectorAll(".src-subtab").forEach(b =>
      b.addEventListener("click", () => show(b.dataset.sub)));
    show("dashboard");
  }

  window.initSourcingPage = initSourcingPage;
})();
