/*
 * AI 상품소싱 (SPA 모듈)
 * 네이버 쇼핑 인기검색어 크롤링 백필 + 키워드 트렌드 분석 UI.
 * index.html의 #page-ai_sourcing 내부에서만 동작하며, 기존 SPA의 showPage() 훅에서
 * window.initAiSourcingPage() 가 호출된다.
 */
(function () {
  "use strict";

  const API_BASE = "/api/ai-sourcing";
  const DEVICE_OPTIONS = [["pc", "PC"], ["mo", "모바일"]];
  const GENDER_OPTIONS = [["f", "여성"], ["m", "남성"]];
  const AGE_OPTIONS = [
    ["10", "10대"], ["20", "20대"], ["30", "30대"],
    ["40", "40대"], ["50", "50대"], ["60", "60대 이상"],
  ];
  const RESULT_COUNT_OPTIONS = [20, 40];

  const state = {
    initialized: false,
    board: null,
    currentRun: null,
    level1: [], level2: [], level3: [],
    form: {
      category1: "", category2: "", category3: "",
      devices: [], genders: [], ages: [],
      resultCount: 20, excludeBrandProducts: false, customExcludedTerms: "",
    },
    snapshotPanel: null,
    selectedPlannerMonth: "01",
    heatmapMode: "season",
    submitting: false,
    actionSubmitting: false,
    pollingHandle: null,
  };

  function $(id) { return document.getElementById(id); }

  function authHeaders() {
    const token = localStorage.getItem("authToken") || localStorage.getItem("access_token") || "";
    const headers = { "Content-Type": "application/json" };
    if (token) headers["Authorization"] = `Bearer ${token}`;
    return headers;
  }

  async function request(path, init = {}) {
    const response = await fetch(`${API_BASE}${path}`, {
      headers: { ...authHeaders(), ...(init.headers || {}) },
      ...init,
    });
    const text = await response.text();
    let payload = {};
    try { payload = text ? JSON.parse(text) : {}; }
    catch { return { ok: false, code: "INVALID_JSON", message: `응답 파싱 실패: ${text.slice(0, 180)}` }; }
    if (!response.ok && payload && payload.ok === undefined) {
      return { ok: false, code: "HTTP_ERROR", message: payload.message || payload.detail || `HTTP ${response.status}` };
    }
    return payload;
  }

  const api = {
    board: () => request("/board"),
    categories: (cid) => request(`/categories/${cid}`),
    collect: (body) => request("/collect", { method: "POST", body: JSON.stringify(body) }),
    run: (runId) => request(`/runs/${runId}`),
    cancelRun: (runId) => request(`/runs/${runId}/cancel`, { method: "POST" }),
    deleteRun: (runId) => request(`/runs/${runId}`, { method: "DELETE" }),
    retryRun: (runId) => request(`/runs/${runId}/retry-failures`, { method: "POST" }),
    snapshots: (runId, period, page) => {
      const params = new URLSearchParams();
      if (period) params.set("period", period);
      if (page) params.set("page", String(page));
      const qs = params.toString();
      return request(`/runs/${runId}/snapshots${qs ? `?${qs}` : ""}`);
    },
  };

  // ── DOM helpers ────────────────────────────────────
  function h(tag, attrs, children) {
    const el = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value === undefined || value === null || value === false) continue;
      if (key === "class") el.className = value;
      else if (key === "html") el.innerHTML = value;
      else if (key.startsWith("on") && typeof value === "function") el.addEventListener(key.slice(2).toLowerCase(), value);
      else if (value === true) el.setAttribute(key, "");
      else el.setAttribute(key, value);
    }
    const list = Array.isArray(children) ? children : (children === undefined ? [] : [children]);
    for (const child of list) {
      if (child === null || child === undefined || child === false) continue;
      if (typeof child === "string" || typeof child === "number") el.appendChild(document.createTextNode(String(child)));
      else if (child instanceof Node) el.appendChild(child);
    }
    return el;
  }

  function clear(el) { if (el) while (el.firstChild) el.removeChild(el.firstChild); }

  function formatDateTime(iso) {
    if (!iso) return "";
    try { return new Date(iso).toLocaleString("ko-KR", { hour12: false }); }
    catch { return iso; }
  }

  function formatSigned(value) {
    const num = Number(value);
    if (Number.isNaN(num)) return "0";
    return num > 0 ? `+${num.toFixed(1)}` : num.toFixed(1);
  }

  function sparkline(points) {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "ais-sparkline");
    svg.setAttribute("viewBox", "0 0 140 30");
    svg.setAttribute("preserveAspectRatio", "none");
    const polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    const arr = Array.isArray(points) ? points : [];
    const max = Math.max(...arr.map((p) => Number(p.value || 0)), 1);
    const step = arr.length > 1 ? 140 / (arr.length - 1) : 140;
    polyline.setAttribute(
      "points",
      arr.map((p, i) => `${(i * step).toFixed(1)},${(30 - (Number(p.value || 0) / max) * 30).toFixed(1)}`).join(" ")
    );
    svg.appendChild(polyline);
    return svg;
  }

  function confidenceBadge(value, label) {
    const kind = label || (value >= 80 ? "high" : value >= 58 ? "medium" : "low");
    const text = kind === "high" ? "신뢰 높음" : kind === "medium" ? "신뢰 보통" : "신뢰 낮음";
    return h("span", { class: `ais-confidence ais-confidence--${kind}` }, `${text} ${Number(value).toFixed(0)}`);
  }

  function heatColor(value, max) {
    if (!max || max <= 0 || !value) return "#f1f5f9";
    const ratio = Math.max(0, Math.min(1, value / max));
    if (ratio < 0.001) return "#f1f5f9";
    const r = Math.round(241 + (29 - 241) * ratio);
    const g = Math.round(245 + (78 - 245) * ratio);
    const b = Math.round(249 + (216 - 249) * ratio);
    return `rgb(${r},${g},${b})`;
  }

  // ── form chips ─────────────────────────────────────
  function renderChips(containerId, options, selected, onToggle, single) {
    const container = $(containerId);
    clear(container);
    for (const [code, label] of options) {
      const active = single ? String(selected) === code : selected.includes(code);
      container.appendChild(
        h("label", { class: `ais-chip${active ? " is-active" : ""}` }, [
          h("input", {
            type: single ? "radio" : "checkbox",
            name: containerId,
            value: code,
            checked: active,
            onchange: () => onToggle(code),
          }),
          label,
        ])
      );
    }
  }

  function renderFilterChips() {
    renderChips("ais-devices", DEVICE_OPTIONS, state.form.devices, (c) => toggleMulti("devices", c));
    renderChips("ais-genders", GENDER_OPTIONS, state.form.genders, (c) => toggleMulti("genders", c));
    renderChips("ais-ages", AGE_OPTIONS, state.form.ages, (c) => toggleMulti("ages", c));
    renderChips(
      "ais-result-count",
      RESULT_COUNT_OPTIONS.map((v) => [String(v), `TOP ${v}`]),
      String(state.form.resultCount),
      (c) => { state.form.resultCount = Number(c); renderFilterChips(); },
      true
    );
  }

  function toggleMulti(field, code) {
    const current = new Set(state.form[field]);
    if (current.has(code)) current.delete(code); else current.add(code);
    state.form[field] = Array.from(current);
    renderFilterChips();
  }

  // ── categories ─────────────────────────────────────
  function renderCategorySelect(id, nodes, value, placeholder) {
    const el = $(id);
    clear(el);
    el.appendChild(h("option", { value: "" }, placeholder));
    for (const node of nodes) {
      el.appendChild(h("option", { value: String(node.cid), selected: String(node.cid) === value }, node.name));
    }
    el.disabled = !nodes.length;
  }

  async function loadCategories(parentCid) {
    const response = await api.categories(parentCid);
    return response.ok ? (response.nodes || []) : [];
  }

  async function bootstrapCategories() {
    if (!state.level1.length) {
      state.level1 = await loadCategories("0");
    }
    renderCategorySelect("ais-cat1", state.level1, state.form.category1, "1분류 선택");
    renderCategorySelect("ais-cat2", state.level2, state.form.category2, "2분류 선택");
    renderCategorySelect("ais-cat3", state.level3, state.form.category3, "3분류 선택");
  }

  async function onCat1Change(value) {
    state.form.category1 = value;
    state.form.category2 = ""; state.form.category3 = "";
    state.level2 = value ? await loadCategories(value) : [];
    state.level3 = [];
    renderCategorySelect("ais-cat2", state.level2, "", "2분류 선택");
    renderCategorySelect("ais-cat3", state.level3, "", "3분류 선택");
  }

  async function onCat2Change(value) {
    state.form.category2 = value;
    state.form.category3 = "";
    state.level3 = value ? await loadCategories(value) : [];
    renderCategorySelect("ais-cat3", state.level3, "", "3분류 선택");
  }

  function getSelectedCategory() {
    const find = (list, v) => list.find((n) => String(n.cid) === v);
    return find(state.level3, state.form.category3) || find(state.level2, state.form.category2) || find(state.level1, state.form.category1) || null;
  }

  // ── submit ─────────────────────────────────────────
  function setFeedback(text, tone) {
    const el = $("ais-feedback");
    el.textContent = text || "";
    el.className = `ais-muted ais-feedback--${tone || "info"}`;
  }

  function splitExcludedTerms(value) {
    return String(value || "").split(/[,\n]/).map((t) => t.trim()).filter(Boolean);
  }

  function buildRequestName(categoryPath, form) {
    const parts = [categoryPath];
    if (form.devices.length) parts.push(`기기:${form.devices.join("+")}`);
    if (form.genders.length) parts.push(`성별:${form.genders.join("+")}`);
    if (form.ages.length) parts.push(`연령:${form.ages.join("+")}`);
    parts.push(`TOP${form.resultCount}`);
    if (form.excludeBrandProducts) parts.push("브랜드제외");
    return parts.join(" · ");
  }

  async function handleSubmit(event) {
    event.preventDefault();
    if (state.submitting) return;
    const category = getSelectedCategory();
    if (!category) { setFeedback("1/2/3분류 중 최소 하나를 선택해 주세요.", "error"); return; }
    state.submitting = true;
    setFeedback("조건을 저장하고 수집 런을 시작합니다…", "info");
    const payload = {
      name: buildRequestName(category.fullPath, state.form),
      categoryCid: category.cid,
      categoryPath: category.fullPath,
      categoryDepth: category.level,
      timeUnit: "month",
      devices: state.form.devices,
      genders: state.form.genders,
      ages: state.form.ages,
      spreadsheetId: "",
      resultCount: state.form.resultCount,
      excludeBrandProducts: state.form.excludeBrandProducts,
      customExcludedTerms: splitExcludedTerms(state.form.customExcludedTerms),
    };
    const response = await api.collect(payload);
    state.submitting = false;
    if (!response.ok) { setFeedback(response.message || "분석 시작 실패", "error"); return; }
    state.currentRun = response.run;
    state.snapshotPanel = null;
    setFeedback(
      response.reusedCachedResult
        ? "기존 완료된 데이터를 재활용했습니다."
        : "수집 런을 생성했습니다. 오른쪽 패널에서 진행 상황을 확인하세요.",
      "success"
    );
    render();
    refreshBoard();
  }

  // ── board polling ─────────────────────────────────
  async function refreshBoard() {
    const response = await api.board();
    if (!response.ok) return;
    state.board = response.board;
    if (state.currentRun) {
      const matched = state.board.runs.find((r) => r.id === state.currentRun.id);
      state.currentRun = matched ? { ...state.currentRun, ...matched } : (state.board.runs[0] || null);
    } else {
      state.currentRun = state.board.runs[0] || null;
    }
    render();
    ensurePolling();
  }

  function ensurePolling() {
    const running = state.currentRun && (state.currentRun.status === "running" || state.currentRun.status === "queued");
    const interval = running ? 5000 : 15000;
    if (state.pollingHandle) clearInterval(state.pollingHandle);
    state.pollingHandle = setInterval(refreshBoard, interval);
  }

  // ── render pipeline ────────────────────────────────
  function render() {
    renderRunHeader();
    renderRunMetrics();
    renderProgress();
    renderSnapshotPanel();
    renderAnalysis();
  }

  function renderRunHeader() {
    const run = state.currentRun;
    const title = $("ais-run-title");
    const subtitle = $("ais-run-subtitle");
    const cancelBtn = $("ais-cancel");
    const retryBtn = $("ais-retry");
    const deleteBtn = $("ais-delete");
    if (!run) {
      title.textContent = "수집 현황";
      subtitle.textContent = "조건을 설정하고 분석을 시작하면 여기에 진행 상황이 표시됩니다.";
      cancelBtn.hidden = retryBtn.hidden = deleteBtn.hidden = true;
      return;
    }
    title.textContent = run.profile?.name || run.profile?.categoryPath || "수집 런";
    subtitle.textContent = `${run.profile.categoryPath} · ${run.profile.startPeriod}~${run.profile.endPeriod} · TOP ${run.profile.resultCount}`;
    cancelBtn.hidden = !run.canCancel;
    retryBtn.hidden = !(run.failedTasks && run.failedTasks > 0);
    deleteBtn.hidden = !run.canDelete;
  }

  function renderRunMetrics() {
    const container = $("ais-metrics");
    clear(container);
    const run = state.currentRun;
    if (!run) return;
    const toneMap = { queued: "attention", running: "progress", failed: "attention" };
    const items = [
      { label: "상태", value: run.status, tone: toneMap[run.status] || "" },
      { label: "진행/전체", value: `${run.completedTasks}/${run.totalTasks}` },
      { label: "실패", value: run.failedTasks, tone: run.failedTasks ? "attention" : "" },
      { label: "스냅샷", value: Number(run.totalSnapshots).toLocaleString("ko-KR") },
      { label: "평균 태스크", value: run.averageTaskSeconds ? `${run.averageTaskSeconds}s` : "-" },
      { label: "예상 잔여", value: run.etaMinutes ? `${run.etaMinutes}분` : "0분" },
    ];
    for (const item of items) {
      container.appendChild(
        h("div", { class: `ais-metric${item.tone ? ` ais-metric--${item.tone}` : ""}` }, [
          h("div", { class: "ais-metric__label" }, item.label),
          h("div", { class: "ais-metric__value" }, String(item.value)),
        ])
      );
    }
  }

  function renderProgress() {
    const run = state.currentRun;
    const bar = $("ais-progress-bar");
    const label = $("ais-progress-label");
    if (!run || !run.totalTasks) {
      bar.style.setProperty("--ais-progress", "0%");
      label.textContent = "진행 상황이 아직 없습니다.";
      return;
    }
    const ratio = Math.round((run.completedTasks / run.totalTasks) * 100);
    bar.style.setProperty("--ais-progress", `${ratio}%`);
    const current = run.currentPeriod ? `현재 ${run.currentPeriod}${run.currentPage ? ` · 페이지 ${run.currentPage}` : ""}` : "";
    label.textContent = `${ratio}% 진행${current ? ` — ${current}` : ""}`;
  }

  async function loadSnapshotPage(period, page) {
    if (!state.currentRun) return;
    const response = await api.snapshots(state.currentRun.id, period, page);
    if (!response.ok) {
      state.snapshotPanel = { period, page, totalPages: 1, totalItems: 0, items: [], error: response.message };
    } else {
      state.snapshotPanel = {
        period: response.period, page: response.page, totalPages: response.totalPages,
        totalItems: response.totalItems, items: response.items, error: null,
      };
    }
    renderSnapshotPanel();
  }

  function renderSnapshotPanel() {
    const body = $("ais-snapshot-body");
    clear(body);
    const periodSelect = $("ais-snapshot-period");
    const pageLabel = $("ais-snap-page");
    const run = state.currentRun;
    if (!run) { pageLabel.textContent = "-"; clear(periodSelect); return; }
    const periods = (run.tasks || [])
      .filter((t) => t.status === "completed").map((t) => t.period)
      .filter((v, i, self) => self.indexOf(v) === i).sort().reverse();
    const currentPeriod = state.snapshotPanel?.period || run.latestCompletedPeriod || periods[0] || "";
    clear(periodSelect);
    if (!periods.length) {
      periodSelect.appendChild(h("option", { value: "" }, "아직 수집된 월 없음"));
      periodSelect.disabled = true;
    } else {
      periodSelect.disabled = false;
      for (const p of periods) periodSelect.appendChild(h("option", { value: p, selected: p === currentPeriod }, p));
    }
    const panel = state.snapshotPanel;
    if (!panel || !panel.items) {
      if (run.latestCompletedPeriod && !panel) loadSnapshotPage(run.latestCompletedPeriod, 1);
      pageLabel.textContent = run.latestCompletedPeriod ? "불러오는 중…" : "아직 수집된 데이터가 없습니다.";
      return;
    }
    if (panel.error) {
      body.appendChild(h("tr", {}, h("td", { colspan: 4, class: "dim" }, panel.error)));
      pageLabel.textContent = panel.error;
      return;
    }
    for (const item of panel.items) {
      body.appendChild(h("tr", {}, [
        h("td", { class: "rank" }, String(item.rank)),
        h("td", {}, item.keyword),
        h("td", { class: "dim" }, item.linkId || "-"),
        h("td", { class: item.brandExcluded ? "brand" : "dim" }, item.brandExcluded ? "브랜드 제외" : "-"),
      ]));
    }
    pageLabel.textContent = `${panel.period} · 페이지 ${panel.page} / ${panel.totalPages}`;
  }

  function renderAnalysis() {
    const section = $("ais-analysis");
    const run = state.currentRun;
    if (!run || !run.analysisReady || !run.analysisSummary) { section.hidden = true; return; }
    section.hidden = false;
    const summary = run.analysisSummary;
    $("ais-analysis-hint").textContent = `포함 ${summary.includedKeywordCount} · 제외 ${summary.excludedKeywordCount} · 관측 ${summary.observedMonths}개월 · 신뢰 ${run.confidenceScore ?? 0}`;
    renderHeroMetrics(summary.heroMetrics || []);
    renderCards(run.analysisCards || []);
    renderPlanner(summary.monthlyPlanner || []);
    renderHeatmap(summary.seasonalityHeatmap || []);
    renderDrilldown(summary.keywordDrilldownSeries || []);
  }

  function renderHeroMetrics(metrics) {
    const container = $("ais-hero"); clear(container);
    for (const m of metrics) {
      container.appendChild(h("div", { class: "ais-hero__card" }, [
        h("div", { class: "ais-hero__label" }, m.monthLabel ? `${m.label} · ${m.monthLabel}` : m.label),
        h("div", { class: "ais-hero__keyword" }, [m.keyword, " ", confidenceBadge(m.confidence, m.confidenceLabel)]),
        h("div", { class: "ais-hero__meta" }, m.rationale),
        sparkline(m.sparkline),
      ]));
    }
  }

  function renderCards(cards) {
    const container = $("ais-cards"); clear(container);
    for (const card of cards) {
      const list = h("ol");
      for (const item of card.items) {
        list.appendChild(h("li", {}, [
          h("strong", {}, item.keyword),
          confidenceBadge(item.confidence, item.confidenceLabel),
          h("div", { class: "ais-muted" }, item.rationale),
        ]));
      }
      container.appendChild(h("div", { class: "ais-card" }, [
        h("h4", {}, card.title),
        h("p", { class: "ais-muted" }, card.description),
        list,
      ]));
    }
  }

  function renderPlanner(planner) {
    const tabs = $("ais-planner-tabs"); const body = $("ais-planner-body");
    clear(tabs); clear(body);
    if (!planner.length) return;
    if (!planner.find((m) => m.month === state.selectedPlannerMonth)) state.selectedPlannerMonth = planner[0].month;
    for (const month of planner) {
      tabs.appendChild(h("button", {
        type: "button",
        class: month.month === state.selectedPlannerMonth ? "is-active" : "",
        onclick: () => { state.selectedPlannerMonth = month.month; renderPlanner(planner); },
      }, month.label));
    }
    const selected = planner.find((m) => m.month === state.selectedPlannerMonth) || planner[0];
    body.appendChild(plannerBucket("추천 키워드", selected.recommendedKeywords));
    body.appendChild(plannerBucket("주의 키워드", selected.cautionKeywords));
  }

  function plannerBucket(title, items) {
    const list = h("ul");
    if (!items || !items.length) list.appendChild(h("li", { class: "ais-muted" }, "데이터가 충분하지 않습니다."));
    else {
      for (const item of items) {
        list.appendChild(h("li", {}, [
          h("strong", {}, item.keyword), " ",
          confidenceBadge(item.confidence, item.confidenceLabel),
          h("div", { class: "ais-muted" }, item.rationale),
        ]));
      }
    }
    return h("div", { class: "ais-planner__bucket" }, [h("h5", {}, title), list]);
  }

  function renderHeatmap(rows) {
    const body = $("ais-heatmap-body"); clear(body);
    if (!rows.length) return;
    const max = Math.max(...rows.flatMap((r) => (r.periodCells || []).map((c) => Number(c.value || 0))), 1);
    const seasonMax = Math.max(...rows.flatMap((r) => (r.seasonCells || []).map((c) => Number(c.value || 0))), 1);
    const cellKey = state.heatmapMode === "season" ? "seasonCells" : "periodCells";
    const scale = state.heatmapMode === "season" ? seasonMax : max;
    for (const row of rows) {
      body.appendChild(h("div", { class: "ais-heatmap-row" }, [
        h("div", { class: "ais-heatmap-row__keyword" }, [row.keyword, " ", confidenceBadge(row.confidence, row.confidenceLabel)]),
        h("div", {
          class: `ais-heatmap-row__cells${state.heatmapMode === "season" ? " ais-heatmap-row__cells--season" : ""}`,
        },
        (row[cellKey] || []).map((cell) => h("div", {
          class: "ais-heat-cell",
          title: `${cell.label} · ${Number(cell.value).toFixed(2)}`,
          style: `background:${heatColor(Number(cell.value || 0), scale)}`,
        }, state.heatmapMode === "season" ? String(cell.label) : ""))),
      ]));
    }
  }

  function renderDrilldown(series) {
    const body = $("ais-drilldown-body") || document.querySelector("#page-ai_sourcing .ais-drilldown > div") || (function () {
      const wrapper = $("ais-drilldown-body");
      if (wrapper) return wrapper;
      const ensured = h("div", { id: "ais-drilldown-body" });
      document.querySelector("#page-ai_sourcing .ais-drilldown").appendChild(ensured);
      return ensured;
    })();
    clear(body);
    for (const item of series.slice(0, 12)) {
      body.appendChild(h("div", { class: "ais-drilldown-item" }, [
        h("div", {}, [h("strong", {}, item.keyword), " ", confidenceBadge(item.confidence, item.confidenceLabel)]),
        h("p", {}, `최근 변화 ${formatSigned(item.recentTrendValue)} · 계절성 ${Number(item.seasonalityScore).toFixed(0)} · 유지율 ${Number(item.recentRetentionValue).toFixed(0)}%`),
        sparkline(item.points),
      ]));
    }
  }

  // ── action modal ──────────────────────────────────
  function openModal(kind) {
    const run = state.currentRun; if (!run) return;
    const modal = $("ais-modal");
    const confirmBtn = $("ais-modal-confirm");
    confirmBtn.dataset.kind = kind;
    if (kind === "cancel") { $("ais-modal-title").textContent = "취합 중지"; $("ais-modal-message").textContent = `"${run.profile.name}" 런을 중지합니다. 진행 중인 월 데이터는 초기화됩니다.`; }
    else if (kind === "delete") { $("ais-modal-title").textContent = "런 삭제"; $("ais-modal-message").textContent = `"${run.profile.name}" 런과 미완료 스냅샷을 삭제합니다.`; }
    else if (kind === "retry") { $("ais-modal-title").textContent = "실패 태스크 재시도"; $("ais-modal-message").textContent = "실패한 월을 다시 큐에 올립니다."; }
    modal.hidden = false;
  }
  function closeModal() { $("ais-modal").hidden = true; }
  async function handleModalConfirm(event) {
    if (state.actionSubmitting) return;
    const kind = event.currentTarget.dataset.kind;
    const run = state.currentRun;
    if (!kind || !run) return;
    state.actionSubmitting = true;
    let response;
    if (kind === "cancel") response = await api.cancelRun(run.id);
    else if (kind === "delete") response = await api.deleteRun(run.id);
    else if (kind === "retry") response = await api.retryRun(run.id);
    state.actionSubmitting = false;
    closeModal();
    if (response && !response.ok) { setFeedback(response.message || "작업 실패", "error"); return; }
    if (kind === "delete") { state.currentRun = null; state.snapshotPanel = null; }
    else if (response && response.run) state.currentRun = response.run;
    await refreshBoard();
  }

  // ── wiring ────────────────────────────────────────
  function wire() {
    if (state.initialized) return;
    state.initialized = true;
    // ensure drilldown inner div exists
    const drilldown = document.querySelector("#page-ai_sourcing .ais-drilldown");
    if (drilldown && !document.getElementById("ais-drilldown-body")) {
      drilldown.appendChild(h("div", { id: "ais-drilldown-body" }));
    }
    $("ais-cat1").addEventListener("change", (e) => onCat1Change(e.target.value).then(render));
    $("ais-cat2").addEventListener("change", (e) => onCat2Change(e.target.value).then(render));
    $("ais-cat3").addEventListener("change", (e) => { state.form.category3 = e.target.value; });
    $("ais-exclude-brand").addEventListener("change", (e) => { state.form.excludeBrandProducts = e.target.checked; });
    $("ais-custom-terms").addEventListener("input", (e) => { state.form.customExcludedTerms = e.target.value; });
    $("ais-form").addEventListener("submit", handleSubmit);
    $("ais-refresh").addEventListener("click", refreshBoard);
    $("ais-cancel").addEventListener("click", () => openModal("cancel"));
    $("ais-retry").addEventListener("click", () => openModal("retry"));
    $("ais-delete").addEventListener("click", () => openModal("delete"));
    $("ais-modal-confirm").addEventListener("click", handleModalConfirm);
    document.querySelectorAll("#ais-modal [data-ais-dismiss]").forEach((el) => el.addEventListener("click", closeModal));
    $("ais-snapshot-period").addEventListener("change", (e) => loadSnapshotPage(e.target.value, 1));
    $("ais-snap-prev").addEventListener("click", () => {
      const p = state.snapshotPanel; if (!p || p.page <= 1) return;
      loadSnapshotPage(p.period, p.page - 1);
    });
    $("ais-snap-next").addEventListener("click", () => {
      const p = state.snapshotPanel; if (!p || p.page >= p.totalPages) return;
      loadSnapshotPage(p.period, p.page + 1);
    });
    $("ais-heatmap-toggle").querySelectorAll("button").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.heatmapMode = btn.dataset.mode || "season";
        $("ais-heatmap-toggle").querySelectorAll("button").forEach((b) => b.classList.toggle("is-active", b === btn));
        renderAnalysis();
      });
    });
  }

  async function init() {
    wire();
    renderFilterChips();
    await bootstrapCategories();
    await refreshBoard();
  }

  // Expose hook for the SPA's showPage() switch.
  window.initAiSourcingPage = function () {
    init().catch((error) => {
      console.error("[ai_sourcing] init failed:", error);
      const el = document.getElementById("ais-feedback");
      if (el) { el.textContent = error.message || "초기화 실패"; el.className = "ais-muted ais-feedback--error"; }
    });
  };
})();
