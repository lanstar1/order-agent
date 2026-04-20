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
          <th style="padding:8px">제목</th>
          <th style="padding:8px">타입</th>
          <th style="padding:8px">단계</th>
          <th style="padding:8px">에러</th>
        </tr></thead>
        <tbody>
          ${videos.map(v => `
            <tr style="border-top:1px solid #e5e7eb">
              <td style="padding:8px">${statusBadge(v.processed_status, v.retry_count)}</td>
              <td style="padding:8px">
                <a href="https://www.youtube.com/watch?v=${escape(v.video_id)}" target="_blank"
                   style="color:#2563eb;text-decoration:none">${escape(v.title || v.video_id)}</a>
              </td>
              <td style="padding:8px">${escape(v.video_type)}</td>
              <td style="padding:8px;color:#6b7280">${escape(v.internal_step || "-")}</td>
              <td style="padding:8px;color:#991b1b;font-size:12px">${escape(v.error_reason || "")}</td>
            </tr>`).join("") || '<tr><td colspan="5" style="padding:20px;color:#9ca3af;text-align:center">수집된 영상이 없습니다</td></tr>'}
        </tbody>
      </table>`;
  }

  // ─── 제품 목록 ──────────────────────────────────────────────
  async function renderProductsPanel(container) {
    const res = await authFetch(`${API}/products`);
    const products = res.ok ? await res.json() : [];
    container.innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px">
        ${products.map(p => {
          const persona = (p.target_persona && p.target_persona.label) || "";
          return `
          <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px">
            ${p.thumbnail_url ? `<img src="${escape(p.thumbnail_url)}" style="width:100%;height:120px;object-fit:cover;border-radius:6px;margin-bottom:8px">` : ""}
            <h4 style="margin:0 0 4px;font-size:14px">${escape(p.product_name)}</h4>
            <p style="margin:0 0 6px;color:#6b7280;font-size:12px">${escape(p.brand || "[?]")} · ${escape(p.category)}</p>
            ${persona ? `<span style="display:inline-block;padding:2px 8px;background:#eef2ff;color:#4338ca;border-radius:10px;font-size:11px">${escape(persona)}</span>` : ""}
            <p style="margin:8px 0 0;font-size:11px;color:#9ca3af">상태: ${escape(p.sourcing_status)}</p>
          </div>`;
        }).join("") || '<div style="color:#9ca3af;padding:40px;text-align:center">추출된 제품이 없습니다</div>'}
      </div>`;
  }

  // ─── 인플루언서 컨택 초안 (자동 발송 없음) ──────────────────
  async function renderInfluencerPanel(container) {
    const res = await authFetch(`${API}/outreach-drafts`);
    const drafts = res.ok ? await res.json() : [];
    container.innerHTML = `
      <div style="background:#fef3c7;border-left:4px solid #f59e0b;padding:10px 14px;margin-bottom:16px;border-radius:6px">
        ⚠️ 이 시스템은 메일·DM을 <b>자동으로 발송하지 않습니다</b>.
        초안을 클립보드에 복사한 뒤 사람이 직접 발송하세요.
      </div>
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
                <button data-copy="${d.id}" data-subject="${escape(d.subject || "")}" class="btn-small">📋 복사</button>
                <button data-status="${d.id}" class="btn-small">상태 변경</button>
              </td>
            </tr>`).join("") || '<tr><td colspan="6" style="padding:20px;color:#9ca3af;text-align:center">생성된 초안이 없습니다</td></tr>'}
        </tbody>
      </table>`;

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
