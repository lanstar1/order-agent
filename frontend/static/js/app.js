/* =============================================
   Order Agent - 메인 앱 로직
   ============================================= */

// ─── 전역 상태 ───
const state = {
  customers:    [],
  currentOrder: null,
  currentTab:   "text",
};

// ─── 드롭다운 키보드 네비게이션 유틸 ───
const _dropdownNav = {};

function initDropdownKeyNav(inputId, dropdownId, selectFn) {
  const key = inputId;
  _dropdownNav[key] = { idx: -1 };

  const inputEl = document.getElementById(inputId);
  if (!inputEl) return;

  inputEl.addEventListener("keydown", e => {
    const dd = document.getElementById(dropdownId);
    if (!dd || dd.style.display === "none") return;

    const items = dd.querySelectorAll("div[onclick]");
    if (!items.length) return;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      _dropdownNav[key].idx = Math.min(_dropdownNav[key].idx + 1, items.length - 1);
      _highlightDropdownItem(items, _dropdownNav[key].idx);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      _dropdownNav[key].idx = Math.max(_dropdownNav[key].idx - 1, 0);
      _highlightDropdownItem(items, _dropdownNav[key].idx);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (_dropdownNav[key].idx >= 0 && _dropdownNav[key].idx < items.length) {
        items[_dropdownNav[key].idx].click();
        _dropdownNav[key].idx = -1;
      }
    } else if (e.key === "Escape") {
      dd.style.display = "none";
      _dropdownNav[key].idx = -1;
    } else {
      // 다른 키 입력 시 인덱스 리셋
      _dropdownNav[key].idx = -1;
    }
  });
}

function _highlightDropdownItem(items, activeIdx) {
  items.forEach((item, i) => {
    if (i === activeIdx) {
      item.style.background = "#ebf8ff";
      item.scrollIntoView({ block: "nearest" });
    } else {
      item.style.background = "";
    }
  });
}

// 페이지 로드 시 드롭다운 키보드 네비게이션 초기화
document.addEventListener("DOMContentLoaded", () => {
  initDropdownKeyNav("cust-search", "cust-dropdown", "selectCust");
  initDropdownKeyNav("so-cust-search", "so-cust-dropdown", "selectSOCust");
  initDropdownKeyNav("tr-cust-search", "tr-cust-dropdown", "selectTrainingCust");
});

// ─── 유틸 ───
function toast(msg, type = "info") {
  const container = document.getElementById("toast-container");
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.textContent = msg;
  container.appendChild(el);
  setTimeout(() => el.remove(), 3500);
}

function showProcessing(step = "처리중...") {
  document.getElementById("processing-overlay").classList.add("show");
  document.getElementById("processing-step").textContent = step;
}

function hideProcessing() {
  document.getElementById("processing-overlay").classList.remove("show");
}

function updateStep(step) {
  document.getElementById("processing-step").textContent = step;
}

function navigateTo(pageId) {
  document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach(n => n.classList.remove("active"));
  const page = document.getElementById("page-" + pageId);
  if (page) page.classList.add("active");
  const nav = document.querySelector(`[data-page="${pageId}"]`);
  if (nav) nav.classList.add("active");
  document.getElementById("topbar-title").textContent = {
    dashboard:    "대시보드",
    new_order:    "판매입력",
    history:      "처리 이력",
    sale_order:   "견적서입력",
    so_result:    "견적서 결과",
    so_history:   "견적서 이력",
    inventory:    "재고 조회",
    doc_search:   "자료검색",
    price_sheet:  "단가표 조회",
    training:     "발주서 학습",
    shipping:     "택배조회",
    cs_rma:       "CS/RMA",
    sales_agent:  "판매에이전트",
    ai_dashboard: "AI 대시보드",
    settings:     "설정",
  }[pageId] || "";
  // CS/RMA 페이지 진입 시 초기화
  if (pageId === "cs_rma") csInit();
  if (pageId === "sales_agent") saInit();
  // 택배조회 페이지 진입 시 통계 로드
  if (pageId === "shipping") initShippingPage();
  // 주문서 페이지 진입 시 드롭존 초기화
  if (pageId === "sale_order") initSODropzone();
  // 자료검색 페이지 진입 시 카테고리 로드
  if (pageId === "doc_search") initDocSearchPage().catch(e => console.error("initDocSearchPage 실패:", e));
  // 단가표 조회 페이지 진입 시 거래처 로드
  if (pageId === "price_sheet") initPriceSheetPage().catch(e => console.error("initPriceSheetPage 실패:", e));
  // 발주서 학습 페이지 진입 시 데이터 로드
  if (pageId === "training") initTrainingPage().catch(e => console.error("initTrainingPage 실패:", e));
  // AI 대시보드 진입 시 데이터 로드
  if (pageId === "ai_dashboard" && typeof loadDashboard === "function") loadDashboard();
}

function statusBadge(status) {
  const labels = {
    pending: "대기", processing: "처리중", reviewing: "검토필요",
    confirmed: "확인완료", submitted: "ERP전송", failed: "실패",
  };
  return `<span class="badge badge-${status}">${labels[status] || status}</span>`;
}

function confidenceBadge(conf) {
  const labels = { high: "자동", medium: "검토권고", low: "검토필요" };
  return `<span class="badge badge-${conf}">${labels[conf] || conf}</span>`;
}

// ─── 거래처 로드 ───
async function loadCustomers() {
  // 전체 로드 없음 - 검색 시 서버 쿼리
  state.customers = [];
}

// ── 거래처 검색 드롭다운 (서버 검색)
let _custSearchTimer = null;
async function onCustSearch(query) {
  const dd = document.getElementById("cust-dropdown");
  if (!query.trim()) { dd.style.display = "none"; return; }
  clearTimeout(_custSearchTimer);
  _custSearchTimer = setTimeout(async () => {
    try {
      const data = await api.get(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const matches = data.customers || [];
      if (!matches.length) {
        dd.innerHTML = `<div style="padding:10px 14px;color:#a0aec0;font-size:13px">검색 결과 없음</div>`;
      } else {
        dd.innerHTML = matches.map(c =>
          `<div onclick="selectCust('${c.cust_code.replace(/'/g,"\\'")}','${c.cust_name.replace(/'/g,"\\'")}'); "
            style="padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid #f7fafc"
            onmouseover="this.style.background='#ebf8ff'" onmouseout="this.style.background=''">
            <strong>${c.cust_name}</strong>
            <span style="color:#a0aec0;margin-left:6px;font-size:12px">${c.cust_code}</span>
          </div>`
        ).join("");
      }
      dd.style.display = "block";
      if (_dropdownNav["cust-search"]) _dropdownNav["cust-search"].idx = -1;
    } catch(e) { console.warn("거래처 검색 오류:", e); }
  }, 200);
}

function showCustDropdown() {
  const q = document.getElementById("cust-search").value.trim();
  if (q) onCustSearch(q);
}

function selectCust(code, name) {
  document.getElementById("cust-select").value = code;
  document.getElementById("cust-search").value = `${name} (${code})`;
  document.getElementById("cust-dropdown").style.display = "none";
  const info = document.getElementById("cust-selected-info");
  info.textContent = `✓ 선택됨: ${name} [${code}]`;
  info.style.display = "block";
}

// 클릭 외부 시 드롭다운 닫기
document.addEventListener("click", e => {
  const dd = document.getElementById("cust-dropdown");
  if (dd && !dd.contains(e.target) && e.target.id !== "cust-search") {
    dd.style.display = "none";
  }
});

// ─── 새 발주서 제출 ───
async function submitOrder() {
  const custCode = document.getElementById("cust-select").value;
  // 검색 인풋에서 이름 파싱 (이름 (코드) 형식)
  const searchVal = document.getElementById("cust-search")?.value || "";
  const custName = searchVal.replace(/\s*\([^)]*\)\s*$/, "").trim() || custCode;
  const rawText  = document.getElementById("raw-text").value.trim();

  if (!custCode) { toast("거래처를 선택해주세요.", "error"); return; }
  if (!rawText)  { toast("발주서 내용을 입력해주세요.", "error"); return; }

  showProcessing("주문 라인 추출 중...");

  try {
    updateStep("AI 분석 중...");
    const result = await api.processOrder({ cust_code: custCode, cust_name: custName, raw_text: rawText });
    state.currentOrder = result;
    hideProcessing();
    renderResult(result);
    navigateTo("result");
  } catch (e) {
    hideProcessing();
    toast("처리 실패: " + e.message, "error");
  }
}

// ─── 결과 렌더링 ───
function renderResult(order) {
  const container = document.getElementById("result-container");
  const needsReview = order.lines.some(l => !l.is_confirmed);

  container.innerHTML = `
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <div>
          <div style="font-size:18px;font-weight:700;color:var(--primary)">
            발주서 처리 결과
          </div>
          <div style="font-size:13px;color:var(--gray-600);margin-top:4px">
            주문번호: <strong>${order.order_id}</strong> &nbsp;|&nbsp;
            거래처: <strong>${order.cust_name}</strong> &nbsp;|&nbsp;
            ${statusBadge(order.status)}
          </div>
        </div>
        ${needsReview ? '<span style="color:var(--warning);font-size:13px;font-weight:600">⚠ 검토 필요 항목이 있습니다</span>' : '<span style="color:var(--success);font-size:13px;font-weight:600">✓ 모든 항목 자동 매칭</span>'}
      </div>

      <table class="result-table">
        <thead>
          <tr>
            <th style="width:40px">#</th>
            <th>원문 내용</th>
            <th style="width:200px">상품 선택</th>
            <th style="width:110px">모델명</th>
            <th style="width:80px">수량</th>
            <th style="width:60px">단위</th>
            <th style="width:80px">신뢰도</th>
          </tr>
        </thead>
        <tbody id="result-tbody">
        </tbody>
      </table>
    </div>

    <div style="display:flex;gap:12px;justify-content:flex-end">
      <button class="btn btn-outline" onclick="navigateTo('new_order')">← 돌아가기</button>
      <button class="btn btn-success" onclick="confirmAndSubmit()" id="btn-confirm">
        ✓ 확인 후 ERP 전송
      </button>
    </div>
  `;

  const tbody = document.getElementById("result-tbody");
  order.lines.forEach(line => {
    const row = document.createElement("tr");
    row.id = `line-row-${line.line_no}`;

    // 상품 선택 드롭다운
    const candidateOptions = line.candidates.map(c =>
      `<option value="${c.prod_cd}" ${c.prod_cd === line.selected_cd ? "selected" : ""}>
        [${c.prod_cd}] ${c.prod_name} (${Math.round(c.score * 100)}%)
      </option>`
    ).join("");

    // 후보별 모델명 맵 (prod_cd → model_name) — 드롭다운 변경 시 업데이트용
    const modelMap = {};
    line.candidates.forEach(c => { if (c.model_name) modelMap[c.prod_cd] = c.model_name; });
    // data 속성으로 row에 저장
    row.dataset.modelMap = JSON.stringify(modelMap);

    // 현재 선택된 상품의 모델명
    const initModel = line.model_name || modelMap[line.selected_cd] || "";

    row.innerHTML = `
      <td style="color:var(--gray-400);font-size:12px">${line.line_no}</td>
      <td>
        <div style="font-weight:500">${line.raw_text}</div>
        ${line.qty ? `<div class="raw-text">수량: ${line.qty} ${line.unit || ""}</div>` : ""}
      </td>
      <td>
        <select class="candidate-select" id="sel-${line.line_no}" onchange="onCandidateChange(${line.line_no})">
          ${candidateOptions || '<option value="">-- 매칭 없음 --</option>'}
        </select>
      </td>
      <td id="model-${line.line_no}" style="font-size:12px;color:var(--gray-700);font-weight:500;padding:0 6px">
        ${initModel ? `<span title="${initModel}">${initModel}</span>` : '<span style="color:var(--gray-300)">-</span>'}
      </td>
      <td>
        <input type="number" class="form-control" style="padding:5px 8px"
          id="qty-${line.line_no}" value="${line.qty || ''}" min="0" step="0.1">
      </td>
      <td>
        <input type="text" class="form-control" style="padding:5px 8px"
          id="unit-${line.line_no}" value="${line.unit || ''}" placeholder="EA">
      </td>
      <td>
        ${line.candidates[0] ? confidenceBadge(line.candidates[0].confidence) : '<span class="badge badge-low">없음</span>'}
      </td>
    `;
    tbody.appendChild(row);
  });
}

function onCandidateChange(lineNo) {
  const row = document.getElementById(`line-row-${lineNo}`);
  // 선택 변경 시 row 하이라이트
  row.style.background = "#fffbeb";
  setTimeout(() => row.style.background = "", 800);

  // 모델명 업데이트
  const selEl = document.getElementById(`sel-${lineNo}`);
  const modelEl = document.getElementById(`model-${lineNo}`);
  if (selEl && modelEl) {
    try {
      const modelMap = JSON.parse(row.dataset.modelMap || "{}");
      const model = modelMap[selEl.value] || "";
      modelEl.innerHTML = model
        ? `<span title="${model}">${model}</span>`
        : '<span style="color:var(--gray-300)">-</span>';
    } catch(e) {}
  }
}

// ─── 확인 후 ERP 전송 ───
async function confirmAndSubmit() {
  const order = state.currentOrder;
  if (!order) return;

  const lines = order.lines.map(line => ({
    line_no:  line.line_no,
    prod_cd:  document.getElementById(`sel-${line.line_no}`)?.value || "",
    qty:      parseFloat(document.getElementById(`qty-${line.line_no}`)?.value) || 0,
    unit:     document.getElementById(`unit-${line.line_no}`)?.value || "",
  }));

  // 검증
  const invalid = lines.filter(l => !l.prod_cd || !l.qty);
  if (invalid.length > 0) {
    toast(`${invalid.length}개 라인에 상품코드 또는 수량이 없습니다.`, "error");
    return;
  }

  showProcessing("ERP 전송 중...");

  try {
    // 1. 확인 저장
    await api.confirmOrder({ order_id: order.order_id, lines });
    updateStep("판매 전표 생성 중...");

    // 2. ERP 전송 (로그인한 담당자 코드 포함)
    const user = getCurrentUser ? getCurrentUser() : null;
    const empCd = user ? user.emp_cd : "";
    const result = await api.submitERP(order.order_id, empCd);
    hideProcessing();

    if (result.success) {
      toast(`ERP 전송 완료! 전표번호: ${result.erp_slip_no || "생성됨"}`, "success");
      setTimeout(() => {
        navigateTo("history");
        loadHistory();
      }, 1500);
    } else {
      toast("ERP 전송 실패: " + result.message, "error");
    }
  } catch (e) {
    hideProcessing();
    toast("오류: " + e.message, "error");
  }
}

// ─── 이력 로드 ───
async function loadHistory() {
  try {
    const res = await api.listOrders(30);
    const container = document.getElementById("history-list");
    if (!res.orders.length) {
      container.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:32px">처리된 발주서가 없습니다.</p>';
      return;
    }
    container.innerHTML = res.orders.map(o => `
      <div class="order-item" onclick="viewOrder('${o.order_id}')">
        <div>
          <div class="order-cust">${o.cust_name || o.cust_code}</div>
          <div class="order-id">주문번호: ${o.order_id}</div>
        </div>
        <div style="display:flex;align-items:center;gap:16px">
          ${statusBadge(o.status)}
          <div class="order-date">${o.created_at?.slice(0, 16) || ""}</div>
        </div>
      </div>
    `).join("");
  } catch (e) {
    toast("이력 로드 실패: " + e.message, "error");
  }
}

async function viewOrder(orderId) {
  try {
    const res = await api.getOrder(orderId);
    renderOrderDetail(res);
  } catch (e) {
    toast("조회 실패: " + e.message, "error");
  }
}

function renderOrderDetail(data) {
  const order = data.order;
  const lines = data.lines || [];
  const submissions = data.submissions || [];
  const container = document.getElementById("history-list");

  // ERP 전송 이력
  let erpHtml = "";
  if (submissions.length) {
    erpHtml = submissions.map(s => `
      <div style="display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--gray-100);font-size:12px">
        <span style="font-size:14px">${s.success ? "✅" : "❌"}</span>
        <div style="flex:1">
          <span style="font-weight:500">${s.success ? "전송 성공" : "전송 실패"}</span>
          ${s.erp_slip_no ? `<span style="margin-left:6px;color:var(--primary);font-weight:600">전표: ${s.erp_slip_no}</span>` : ""}
        </div>
        <span style="font-size:11px;color:var(--gray-400)">${s.submitted_at?.slice(0, 16) || ""}</span>
      </div>
    `).join("");
  } else {
    erpHtml = '<p style="color:var(--gray-400);font-size:13px">ERP 전송 이력 없음</p>';
  }

  // 라인 테이블
  const linesHtml = lines.length ? `
    <table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px">
      <thead>
        <tr style="border-bottom:2px solid var(--gray-200);color:var(--gray-500)">
          <th style="text-align:left;padding:5px 6px;width:32px">#</th>
          <th style="text-align:left;padding:5px 6px">원문 내용</th>
          <th style="text-align:left;padding:5px 6px;width:140px">선택 상품코드</th>
          <th style="text-align:center;padding:5px 6px;width:60px">수량</th>
          <th style="text-align:center;padding:5px 6px;width:45px">단위</th>
          <th style="text-align:center;padding:5px 6px;width:50px">상태</th>
        </tr>
      </thead>
      <tbody>
        ${lines.map(l => {
          const selCand = (l.candidates || []).find(c => c.was_selected);
          const selectedName = selCand ? selCand.prod_name : "";
          return `
          <tr style="border-bottom:1px solid var(--gray-100)">
            <td style="padding:4px 6px;color:var(--gray-400)">${l.line_no}</td>
            <td style="padding:4px 6px">
              <div style="font-weight:500">${l.raw_text || ""}</div>
              ${selectedName ? '<div style="font-size:11px;color:var(--gray-400);margin-top:1px">' + selectedName + '</div>' : ""}
            </td>
            <td style="padding:4px 6px;font-family:monospace;font-size:11px;color:var(--primary)">${l.selected_cd || '<span style="color:var(--danger)">미선택</span>'}</td>
            <td style="padding:4px 6px;text-align:center">${l.qty || "-"}</td>
            <td style="padding:4px 6px;text-align:center">${l.unit || "-"}</td>
            <td style="padding:4px 6px;text-align:center">${l.is_confirmed ? '<span style="color:var(--success);font-weight:600">확인</span>' : '<span style="color:var(--warning)">미확인</span>'}</td>
          </tr>`;
        }).join("")}
      </tbody>
    </table>
  ` : '<p style="color:var(--gray-400);font-size:12px">라인 데이터 없음</p>';

  const rawText = order.raw_text || "";

  container.innerHTML = `
    <div style="margin-bottom:10px">
      <button class="btn btn-outline btn-sm" onclick="loadHistory()">← 목록으로</button>
    </div>

    <div class="card" style="margin-bottom:12px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <div>
          <span style="font-size:15px;font-weight:700;color:var(--primary)">주문 상세</span>
          <span style="font-size:12px;color:var(--gray-500);margin-left:10px">
            주문번호: <strong>${order.order_id}</strong>
          </span>
        </div>
        ${statusBadge(order.status)}
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;font-size:12px;padding:10px 14px;background:var(--gray-50);border-radius:6px;margin-bottom:12px">
        <div>
          <span style="color:var(--gray-400);font-size:11px">거래처</span>
          <div style="font-weight:600;margin-top:1px">${order.cust_name || "-"} <span style="color:var(--gray-400);font-weight:400;font-size:11px">(${order.cust_code || "-"})</span></div>
        </div>
        <div>
          <span style="color:var(--gray-400);font-size:11px">처리일시</span>
          <div style="font-weight:600;margin-top:1px">${order.created_at?.slice(0, 16) || "-"}</div>
        </div>
        <div>
          <span style="color:var(--gray-400);font-size:11px">라인 수</span>
          <div style="font-weight:600;margin-top:1px">${lines.length}건</div>
        </div>
        <div>
          <span style="color:var(--gray-400);font-size:11px">최종 수정</span>
          <div style="font-weight:600;margin-top:1px">${order.updated_at?.slice(0, 16) || "-"}</div>
        </div>
      </div>

      ${rawText ? `
      <div>
        <div style="font-size:12px;color:var(--gray-400);margin-bottom:6px;font-weight:600">발주서 원문</div>
        <div style="font-size:12px;color:var(--gray-600);background:#f7fafc;padding:10px 14px;border-radius:6px;white-space:pre-wrap;max-height:120px;overflow-y:auto;border:1px solid var(--gray-100)">${rawText}</div>
      </div>` : ""}
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="card-title">주문 라인 (${lines.length}건)</div>
      ${linesHtml}
    </div>

    <div class="card">
      <div class="card-title">ERP 전송 이력</div>
      ${erpHtml}
    </div>
  `;
}

// ─── 탭 전환 (새 발주서) ───
function switchTab(tab) {
  state.currentTab = tab;
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelector(`[data-tab="${tab}"]`).classList.add("active");
  document.getElementById("tab-text").style.display  = tab === "text"  ? "block" : "none";
  document.getElementById("tab-image").style.display = tab === "image" ? "block" : "none";
}

// ─── 사이드바 네비게이션 (클릭 + 드래그 정렬) ───
function initSidebarNav() {
  const nav = document.getElementById("sidebar-nav");
  if (!nav) return;

  // localStorage에서 저장된 순서 복원
  const saved = localStorage.getItem("navOrder");
  if (saved) {
    try {
      const order = JSON.parse(saved);
      const items = Array.from(nav.querySelectorAll(".nav-item"));
      const map = {};
      items.forEach(el => { map[el.dataset.page] = el; });
      order.forEach(pageId => {
        if (map[pageId]) nav.appendChild(map[pageId]);
      });
    } catch (e) { /* 무시 */ }
  }

  // 클릭 이벤트
  nav.querySelectorAll(".nav-item").forEach(item => {
    item.addEventListener("click", (e) => {
      if (e.target.classList.contains("drag-handle")) return;
      const page = item.dataset.page;
      navigateTo(page);
      if (page === "history") loadHistory();
      if (page === "sale_order") initSODropzone();
    });
  });

  // 드래그앤드롭 정렬
  let dragEl = null;

  nav.addEventListener("dragstart", (e) => {
    const item = e.target.closest(".nav-item");
    if (!item) return;
    dragEl = item;
    item.classList.add("dragging");
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", item.dataset.page);
  });

  nav.addEventListener("dragend", (e) => {
    if (dragEl) dragEl.classList.remove("dragging");
    nav.querySelectorAll(".nav-item").forEach(n => n.classList.remove("drag-over"));
    dragEl = null;
  });

  nav.addEventListener("dragover", (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    const target = e.target.closest(".nav-item");
    if (!target || target === dragEl) return;
    nav.querySelectorAll(".nav-item").forEach(n => n.classList.remove("drag-over"));
    target.classList.add("drag-over");
  });

  nav.addEventListener("dragleave", (e) => {
    const target = e.target.closest(".nav-item");
    if (target) target.classList.remove("drag-over");
  });

  nav.addEventListener("drop", (e) => {
    e.preventDefault();
    const target = e.target.closest(".nav-item");
    if (!target || !dragEl || target === dragEl) return;
    target.classList.remove("drag-over");

    // 위치 계산: 드롭 대상의 위/아래 반 기준
    const rect = target.getBoundingClientRect();
    const midY = rect.top + rect.height / 2;
    if (e.clientY < midY) {
      nav.insertBefore(dragEl, target);
    } else {
      nav.insertBefore(dragEl, target.nextSibling);
    }

    // localStorage에 순서 저장
    const order = Array.from(nav.querySelectorAll(".nav-item")).map(n => n.dataset.page);
    localStorage.setItem("navOrder", JSON.stringify(order));
  });
}

// ─── 초기화 ───
document.addEventListener("DOMContentLoaded", () => {
  // 네비게이션 (클릭 + 드래그 정렬)
  initSidebarNav();

  // 탭 버튼
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // 초기 데이터 로드
  loadCustomers();
  navigateTo("new_order");

  // 드래그앤드롭 업로드
  const dropzone = document.getElementById("image-dropzone");
  if (dropzone) {
    dropzone.addEventListener("dragover", e => { e.preventDefault(); dropzone.classList.add("drag-over"); });
    dropzone.addEventListener("dragleave", () => dropzone.classList.remove("drag-over"));
    dropzone.addEventListener("drop", e => {
      e.preventDefault();
      dropzone.classList.remove("drag-over");
      const file = e.dataTransfer.files[0];
      if (file) handleImageFile(file);
    });
    dropzone.addEventListener("click", () => document.getElementById("image-input").click());
  }
});

function handleImageFile(file) {
  // 파일 유형 검증
  const allowed = ["image/jpeg", "image/png", "image/gif", "image/webp", "application/pdf"];
  if (!allowed.includes(file.type) && !file.name.match(/\.(jpg|jpeg|png|gif|webp|pdf)$/i)) {
    toast("JPG, PNG, GIF, WebP, PDF 파일만 지원합니다.", "error");
    return;
  }
  if (file.size > 10 * 1024 * 1024) {
    toast("파일 크기가 10MB를 초과합니다.", "error");
    return;
  }

  // 드롭존에 미리보기 표시
  const dropzone = document.getElementById("image-dropzone");
  const isPDF = file.type === "application/pdf" || file.name.toLowerCase().endsWith(".pdf");

  if (isPDF) {
    dropzone.innerHTML = `
      <div style="font-size:36px">📄</div>
      <p style="font-weight:600;margin:6px 0 2px">${file.name}</p>
      <p style="font-size:12px;color:var(--gray-400)">${(file.size / 1024).toFixed(0)} KB · PDF</p>
      <button class="btn btn-outline btn-sm" style="margin-top:8px" onclick="clearImageFile()">✕ 다시 선택</button>`;
  } else {
    const url = URL.createObjectURL(file);
    dropzone.innerHTML = `
      <img src="${url}" style="max-height:160px;max-width:100%;border-radius:6px;object-fit:contain">
      <p style="font-size:12px;color:var(--gray-500);margin-top:6px">${file.name} · ${(file.size / 1024).toFixed(0)} KB</p>
      <button class="btn btn-outline btn-sm" style="margin-top:4px" onclick="clearImageFile()">✕ 다시 선택</button>`;
  }

  // 분석 버튼 추가 (또는 자동 시작 버튼)
  const analyzeBtn = document.getElementById("btn-analyze-image");
  if (analyzeBtn) analyzeBtn.remove();
  const btn = document.createElement("button");
  btn.id = "btn-analyze-image";
  btn.className = "btn btn-primary";
  btn.style.cssText = "margin-top:12px;display:block;width:100%";
  btn.textContent = "🔍 AI 분석 시작 →";
  btn.onclick = () => submitImageOrder(file);
  dropzone.after(btn);
}

function clearImageFile() {
  const dropzone = document.getElementById("image-dropzone");
  dropzone.innerHTML = `
    <div class="upload-icon">📎</div>
    <p><strong>이미지를 드래그하거나 클릭하여 업로드</strong></p>
    <p style="font-size:12px;margin-top:6px;color:var(--gray-400)">JPG, PNG, PDF 지원 · 최대 10MB</p>`;
  const btn = document.getElementById("btn-analyze-image");
  if (btn) btn.remove();
  document.getElementById("image-input").value = "";
}

async function submitImageOrder(file) {
  const custCode = document.getElementById("cust-select").value;
  const searchVal = document.getElementById("cust-search")?.value || "";
  const custName = searchVal.replace(/\s*\([^)]*\)\s*$/, "").trim() || custCode;

  if (!custCode) {
    toast("먼저 거래처를 선택해주세요.", "error");
    return;
  }

  showProcessing("이미지 분석 중...");

  try {
    updateStep("Claude Vision이 이미지를 읽는 중...");
    const formData = new FormData();
    formData.append("cust_code", custCode);
    formData.append("cust_name", custName);
    formData.append("file", file);

    const result = await api.processImage(formData);
    state.currentOrder = result;
    hideProcessing();
    renderResult(result);
    navigateTo("result");
    clearImageFile();
  } catch (e) {
    hideProcessing();
    toast("OCR 처리 실패: " + e.message, "error");
  }
}

// ─── HTML 이스케이프 ───
function escapeHtml(text) {
  if (!text) return "";
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}


/* =============================================
   재고 조회
   ============================================= */

// ─── 재고 조회 자동완성 ───
let _invAutoTimer = null;
let _invAutoIdx = -1;       // 키보드 선택 인덱스
let _invAutoResults = [];   // 현재 자동완성 결과

async function onInvAutocomplete(query) {
  const dd = document.getElementById("inv-autocomplete-dropdown");
  _invAutoIdx = -1;

  if (!query || query.trim().length < 1) {
    dd.style.display = "none";
    _invAutoResults = [];
    return;
  }

  clearTimeout(_invAutoTimer);
  _invAutoTimer = setTimeout(async () => {
    try {
      const res = await api.inventoryAutocomplete(query.trim(), 15);
      _invAutoResults = res.results || [];

      if (!_invAutoResults.length) {
        dd.innerHTML = `<div style="padding:12px 14px;color:#a0aec0;font-size:13px">검색 결과 없음</div>`;
        dd.style.display = "block";
        return;
      }

      dd.innerHTML = _invAutoResults.map((p, i) => `
        <div class="inv-ac-item" data-idx="${i}"
          onclick="selectInvProduct(${i})"
          onmouseover="highlightInvItem(${i})"
          style="padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid #f7fafc;
                 display:flex;align-items:center;gap:10px;transition:background .1s">
          <span style="flex-shrink:0;font-family:monospace;font-size:11px;color:var(--gray-500);
                       background:var(--gray-50);padding:2px 6px;border-radius:4px;min-width:60px;text-align:center">
            ${escapeHtml(p.prod_cd)}
          </span>
          <span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            <strong>${escapeHtml(p.prod_name)}</strong>
          </span>
          ${p.model ? `<span style="font-size:11px;color:var(--gray-400);flex-shrink:0;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(p.model)}</span>` : ''}
        </div>
      `).join("");
      dd.style.display = "block";
    } catch (e) {
      console.warn("자동완성 오류:", e);
    }
  }, 180);
}

function highlightInvItem(idx) {
  _invAutoIdx = idx;
  document.querySelectorAll(".inv-ac-item").forEach((el, i) => {
    el.style.background = i === idx ? "#ebf8ff" : "";
  });
}

function selectInvProduct(idx) {
  const p = _invAutoResults[idx];
  if (!p) return;
  const input = document.getElementById("inv-search-input");
  // 모델명이 있으면 모델명, 없으면 품명으로 채움
  input.value = p.model || p.prod_name || p.prod_cd;
  document.getElementById("inv-autocomplete-dropdown").style.display = "none";
  _invAutoResults = [];
  _invAutoIdx = -1;
  // 선택 후 바로 재고 조회 실행
  searchInventory();
}

function onInvSearchKeydown(event) {
  const dd = document.getElementById("inv-autocomplete-dropdown");
  const items = _invAutoResults;

  if (dd.style.display === "none" || !items.length) {
    if (event.key === "Enter") searchInventory();
    return;
  }

  if (event.key === "ArrowDown") {
    event.preventDefault();
    _invAutoIdx = Math.min(_invAutoIdx + 1, items.length - 1);
    highlightInvItem(_invAutoIdx);
    // 스크롤 따라가기
    const el = dd.querySelector(`[data-idx="${_invAutoIdx}"]`);
    if (el) el.scrollIntoView({ block: "nearest" });
  } else if (event.key === "ArrowUp") {
    event.preventDefault();
    _invAutoIdx = Math.max(_invAutoIdx - 1, 0);
    highlightInvItem(_invAutoIdx);
    const el = dd.querySelector(`[data-idx="${_invAutoIdx}"]`);
    if (el) el.scrollIntoView({ block: "nearest" });
  } else if (event.key === "Enter") {
    event.preventDefault();
    if (_invAutoIdx >= 0 && _invAutoIdx < items.length) {
      selectInvProduct(_invAutoIdx);
    } else {
      dd.style.display = "none";
      searchInventory();
    }
  } else if (event.key === "Escape") {
    dd.style.display = "none";
    _invAutoIdx = -1;
  }
}

// 드롭다운 외부 클릭 시 닫기
document.addEventListener("click", (e) => {
  const dd = document.getElementById("inv-autocomplete-dropdown");
  const input = document.getElementById("inv-search-input");
  if (dd && input && !dd.contains(e.target) && e.target !== input) {
    dd.style.display = "none";
  }
});

async function searchInventory() {
  const query = document.getElementById("inv-search-input").value.trim();
  const whCd = document.getElementById("inv-wh-input").value.trim();
  const container = document.getElementById("inv-results");

  if (!query) {
    toast("검색어를 입력하세요.", "error");
    return;
  }

  container.innerHTML = `
    <div style="text-align:center;padding:40px 0">
      <div class="spinner" style="margin:0 auto 12px"></div>
      <p style="color:var(--gray-500);font-size:13px">ERP 재고 조회 중...</p>
    </div>`;

  try {
    const res = await api.inventorySearch(query, whCd);
    renderInventoryResults(res.results || [], query);
  } catch (e) {
    container.innerHTML = `<p style="color:var(--danger);text-align:center;padding:32px">\u274C 조회 실패: ${escapeHtml(e.message)}</p>`;
    toast("재고 조회 실패: " + e.message, "error");
  }
}

/* =============================================
   견적서입력
   ============================================= */

const soState = {
  currentOrder: null,
  currentTab: "text",
};

// ─── 거래처 검색 (주문서용) ───
let _soCustSearchTimer = null;
async function onSOCustSearch(query) {
  const dd = document.getElementById("so-cust-dropdown");
  if (!query.trim()) { dd.style.display = "none"; return; }
  clearTimeout(_soCustSearchTimer);
  _soCustSearchTimer = setTimeout(async () => {
    try {
      const data = await api.get(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const matches = data.customers || [];
      if (!matches.length) {
        dd.innerHTML = `<div style="padding:10px 14px;color:#a0aec0;font-size:13px">검색 결과 없음</div>`;
      } else {
        dd.innerHTML = matches.map(c =>
          `<div onclick="selectSOCust('${c.cust_code.replace(/'/g,"\\'")}','${c.cust_name.replace(/'/g,"\\'")}');"
            style="padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid #f7fafc"
            onmouseover="this.style.background='#ebf8ff'" onmouseout="this.style.background=''">
            <strong>${c.cust_name}</strong>
            <span style="color:#a0aec0;margin-left:6px;font-size:12px">${c.cust_code}</span>
          </div>`
        ).join("");
      }
      dd.style.display = "block";
      if (_dropdownNav["so-cust-search"]) _dropdownNav["so-cust-search"].idx = -1;
    } catch(e) { console.warn("거래처 검색 오류:", e); }
  }, 200);
}

function showSOCustDropdown() {
  const q = document.getElementById("so-cust-search").value.trim();
  if (q) onSOCustSearch(q);
}

function selectSOCust(code, name) {
  document.getElementById("so-cust-select").value = code;
  document.getElementById("so-cust-search").value = `${name} (${code})`;
  document.getElementById("so-cust-dropdown").style.display = "none";
  const info = document.getElementById("so-cust-selected-info");
  info.textContent = `✓ 선택됨: ${name} [${code}]`;
  info.style.display = "block";
}

// 클릭 외부 시 드롭다운 닫기
document.addEventListener("click", e => {
  const dd = document.getElementById("so-cust-dropdown");
  if (dd && !dd.contains(e.target) && e.target.id !== "so-cust-search") {
    dd.style.display = "none";
  }
});

// ─── 탭 전환 ───
function switchSOTab(tab) {
  soState.currentTab = tab;
  document.querySelectorAll("[data-so-tab]").forEach(b => b.classList.remove("active"));
  const btn = document.querySelector(`[data-so-tab="${tab}"]`);
  if (btn) btn.classList.add("active");
  document.getElementById("so-tab-text").style.display  = tab === "text"  ? "block" : "none";
  document.getElementById("so-tab-image").style.display = tab === "image" ? "block" : "none";
}

// ─── 드롭존 초기화 ───
let _soDropzoneInitialized = false;
function initSODropzone() {
  if (_soDropzoneInitialized) return;
  const dropzone = document.getElementById("so-image-dropzone");
  if (!dropzone) return;
  dropzone.addEventListener("dragover", e => { e.preventDefault(); dropzone.classList.add("drag-over"); });
  dropzone.addEventListener("dragleave", () => dropzone.classList.remove("drag-over"));
  dropzone.addEventListener("drop", e => {
    e.preventDefault();
    dropzone.classList.remove("drag-over");
    const file = e.dataTransfer.files[0];
    if (file) handleSOImageFile(file);
  });
  dropzone.addEventListener("click", () => document.getElementById("so-image-input").click());
  _soDropzoneInitialized = true;
}

// ─── 이미지 파일 처리 ───
function handleSOImageFile(file) {
  const allowed = ["image/jpeg", "image/png", "image/gif", "image/webp", "application/pdf"];
  if (!allowed.includes(file.type) && !file.name.match(/\.(jpg|jpeg|png|gif|webp|pdf)$/i)) {
    toast("JPG, PNG, GIF, WebP, PDF 파일만 지원합니다.", "error");
    return;
  }
  if (file.size > 10 * 1024 * 1024) {
    toast("파일 크기가 10MB를 초과합니다.", "error");
    return;
  }

  const dropzone = document.getElementById("so-image-dropzone");
  const isPDF = file.type === "application/pdf" || file.name.toLowerCase().endsWith(".pdf");

  if (isPDF) {
    dropzone.innerHTML = `
      <div style="font-size:36px">📄</div>
      <p style="font-weight:600;margin:6px 0 2px">${file.name}</p>
      <p style="font-size:12px;color:var(--gray-400)">${(file.size / 1024).toFixed(0)} KB · PDF</p>
      <button class="btn btn-outline btn-sm" style="margin-top:8px" onclick="clearSOImageFile()">✕ 다시 선택</button>`;
  } else {
    const url = URL.createObjectURL(file);
    dropzone.innerHTML = `
      <img src="${url}" style="max-height:160px;max-width:100%;border-radius:6px;object-fit:contain">
      <p style="font-size:12px;color:var(--gray-500);margin-top:6px">${file.name} · ${(file.size / 1024).toFixed(0)} KB</p>
      <button class="btn btn-outline btn-sm" style="margin-top:4px" onclick="clearSOImageFile()">✕ 다시 선택</button>`;
  }

  // 분석 버튼
  const old = document.getElementById("btn-so-analyze-image");
  if (old) old.remove();
  const btn = document.createElement("button");
  btn.id = "btn-so-analyze-image";
  btn.className = "btn btn-primary";
  btn.style.cssText = "margin-top:12px;display:block;width:100%";
  btn.textContent = "🔍 AI 분석 시작 →";
  btn.onclick = () => submitSOImageOrder(file);
  dropzone.after(btn);
}

function clearSOImageFile() {
  const dropzone = document.getElementById("so-image-dropzone");
  dropzone.innerHTML = `
    <div class="upload-icon">📎</div>
    <p><strong>이미지를 드래그하거나 클릭하여 업로드</strong></p>
    <p style="font-size:12px;margin-top:6px;color:var(--gray-400)">JPG, PNG, PDF 지원 · 최대 10MB</p>`;
  const btn = document.getElementById("btn-so-analyze-image");
  if (btn) btn.remove();
  document.getElementById("so-image-input").value = "";
}

// ─── 주문서 제출 (텍스트) ───
async function submitSaleOrder() {
  const custCode = document.getElementById("so-cust-select").value;
  const searchVal = document.getElementById("so-cust-search")?.value || "";
  const custName = searchVal.replace(/\s*\([^)]*\)\s*$/, "").trim() || custCode;
  const rawText  = document.getElementById("so-raw-text").value.trim();

  if (!custCode) { toast("거래처를 선택해주세요.", "error"); return; }
  if (!rawText)  { toast("발주서 내용을 입력해주세요.", "error"); return; }

  showProcessing("주문 라인 추출 중...");

  try {
    updateStep("AI 분석 중...");
    const result = await api.processSaleOrder({ cust_code: custCode, cust_name: custName, raw_text: rawText });
    soState.currentOrder = result;
    hideProcessing();
    renderSOResult(result);
    navigateTo("so_result");
  } catch (e) {
    hideProcessing();
    toast("처리 실패: " + e.message, "error");
  }
}

// ─── 주문서 이미지 제출 ───
async function submitSOImageOrder(file) {
  const custCode = document.getElementById("so-cust-select").value;
  const searchVal = document.getElementById("so-cust-search")?.value || "";
  const custName = searchVal.replace(/\s*\([^)]*\)\s*$/, "").trim() || custCode;

  if (!custCode) {
    toast("먼저 거래처를 선택해주세요.", "error");
    return;
  }

  showProcessing("이미지 분석 중...");

  try {
    updateStep("Claude Vision이 이미지를 읽는 중...");
    const formData = new FormData();
    formData.append("cust_code", custCode);
    formData.append("cust_name", custName);
    formData.append("file", file);

    const result = await api.processSaleOrderImage(formData);
    soState.currentOrder = result;
    hideProcessing();
    renderSOResult(result);
    navigateTo("so_result");
    clearSOImageFile();
  } catch (e) {
    hideProcessing();
    toast("OCR 처리 실패: " + e.message, "error");
  }
}

// ─── 주문서 결과 렌더링 ───
function renderSOResult(order) {
  const container = document.getElementById("so-result-container");
  const needsReview = order.lines.some(l => !l.is_confirmed);

  container.innerHTML = `
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <div>
          <div style="font-size:18px;font-weight:700;color:var(--primary)">
            📑 견적서 처리 결과
          </div>
          <div style="font-size:13px;color:var(--gray-600);margin-top:4px">
            주문번호: <strong>${order.order_id}</strong> &nbsp;|&nbsp;
            거래처: <strong>${order.cust_name}</strong> &nbsp;|&nbsp;
            ${statusBadge(order.status)}
          </div>
        </div>
        ${needsReview ? '<span style="color:var(--warning);font-size:13px;font-weight:600">⚠ 검토 필요 항목이 있습니다</span>' : '<span style="color:var(--success);font-size:13px;font-weight:600">✓ 모든 항목 자동 매칭</span>'}
      </div>

      <table class="result-table">
        <thead>
          <tr>
            <th style="width:40px">#</th>
            <th>원문 내용</th>
            <th style="width:200px">상품 선택</th>
            <th style="width:110px">모델명</th>
            <th style="width:80px">수량</th>
            <th style="width:60px">단위</th>
            <th style="width:80px">신뢰도</th>
          </tr>
        </thead>
        <tbody id="so-result-tbody">
        </tbody>
      </table>
    </div>

    <div style="display:flex;gap:12px;justify-content:flex-end">
      <button class="btn btn-outline" onclick="navigateTo('sale_order')">← 돌아가기</button>
      <button class="btn btn-success" onclick="confirmAndSubmitSO()" id="btn-so-confirm">
        ✓ 확인 후 ERP 견적서 전송
      </button>
    </div>
  `;

  const tbody = document.getElementById("so-result-tbody");
  order.lines.forEach(line => {
    const row = document.createElement("tr");
    row.id = `so-line-row-${line.line_no}`;

    const candidateOptions = line.candidates.map(c =>
      `<option value="${c.prod_cd}" ${c.prod_cd === line.selected_cd ? "selected" : ""}>
        [${c.prod_cd}] ${c.prod_name} (${Math.round(c.score * 100)}%)
      </option>`
    ).join("");

    const modelMap = {};
    line.candidates.forEach(c => { if (c.model_name) modelMap[c.prod_cd] = c.model_name; });
    row.dataset.modelMap = JSON.stringify(modelMap);

    const initModel = line.model_name || modelMap[line.selected_cd] || "";

    row.innerHTML = `
      <td style="color:var(--gray-400);font-size:12px">${line.line_no}</td>
      <td>
        <div style="font-weight:500">${line.raw_text}</div>
        ${line.qty ? `<div class="raw-text">수량: ${line.qty} ${line.unit || ""}</div>` : ""}
      </td>
      <td>
        <select class="candidate-select" id="so-sel-${line.line_no}" onchange="onSOCandidateChange(${line.line_no})">
          ${candidateOptions || '<option value="">-- 매칭 없음 --</option>'}
        </select>
      </td>
      <td id="so-model-${line.line_no}" style="font-size:12px;color:var(--gray-700);font-weight:500;padding:0 6px">
        ${initModel ? `<span title="${initModel}">${initModel}</span>` : '<span style="color:var(--gray-300)">-</span>'}
      </td>
      <td>
        <input type="number" class="form-control" style="padding:5px 8px"
          id="so-qty-${line.line_no}" value="${line.qty || ''}" min="0" step="0.1">
      </td>
      <td>
        <input type="text" class="form-control" style="padding:5px 8px"
          id="so-unit-${line.line_no}" value="${line.unit || ''}" placeholder="EA">
      </td>
      <td>
        ${line.candidates[0] ? confidenceBadge(line.candidates[0].confidence) : '<span class="badge badge-low">없음</span>'}
      </td>
    `;
    tbody.appendChild(row);
  });
}

function onSOCandidateChange(lineNo) {
  const row = document.getElementById(`so-line-row-${lineNo}`);
  row.style.background = "#fffbeb";
  setTimeout(() => row.style.background = "", 800);
  const selEl = document.getElementById(`so-sel-${lineNo}`);
  const modelEl = document.getElementById(`so-model-${lineNo}`);
  if (selEl && modelEl) {
    try {
      const modelMap = JSON.parse(row.dataset.modelMap || "{}");
      const model = modelMap[selEl.value] || "";
      modelEl.innerHTML = model
        ? `<span title="${model}">${model}</span>`
        : '<span style="color:var(--gray-300)">-</span>';
    } catch(e) {}
  }
}

// ─── 주문서 확인 후 ERP 전송 ───
async function confirmAndSubmitSO() {
  const order = soState.currentOrder;
  if (!order) return;

  const lines = order.lines.map(line => ({
    line_no:  line.line_no,
    prod_cd:  document.getElementById(`so-sel-${line.line_no}`)?.value || "",
    qty:      parseFloat(document.getElementById(`so-qty-${line.line_no}`)?.value) || 0,
    unit:     document.getElementById(`so-unit-${line.line_no}`)?.value || "",
  }));

  const invalid = lines.filter(l => !l.prod_cd || !l.qty);
  if (invalid.length > 0) {
    toast(`${invalid.length}개 라인에 상품코드 또는 수량이 없습니다.`, "error");
    return;
  }

  showProcessing("ERP 견적서 전송 중...");

  try {
    await api.confirmSaleOrder({ order_id: order.order_id, lines });
    updateStep("견적서 생성 중...");

    const user = getCurrentUser ? getCurrentUser() : null;
    const empCd = user ? user.emp_cd : "";
    const result = await api.submitSaleOrderERP(order.order_id, empCd);
    hideProcessing();

    if (result.success) {
      toast(`ERP 견적서 전송 완료! 전표번호: ${result.erp_slip_no || "생성됨"}`, "success");
      setTimeout(() => {
        loadSOHistory();
        navigateTo("so_history");
      }, 1500);
    } else {
      toast("ERP 전송 실패: " + result.message, "error");
    }
  } catch (e) {
    hideProcessing();
    toast("오류: " + e.message, "error");
  }
}

// ─── 주문서 이력 로드 ───
async function loadSOHistory() {
  try {
    const res = await api.listSaleOrders(30);
    const container = document.getElementById("so-history-list");
    if (!res.orders.length) {
      container.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:32px">처리된 견적서가 없습니다.</p>';
      return;
    }
    container.innerHTML = res.orders.map(o => `
      <div class="order-item" onclick="viewSOOrder('${o.order_id}')">
        <div>
          <div class="order-cust">${o.cust_name || o.cust_code}</div>
          <div class="order-id">주문번호: ${o.order_id}</div>
        </div>
        <div style="display:flex;align-items:center;gap:16px">
          ${statusBadge(o.status)}
          <div class="order-date">${o.created_at?.slice(0, 16) || ""}</div>
        </div>
      </div>
    `).join("");
  } catch (e) {
    toast("이력 로드 실패: " + e.message, "error");
  }
}

async function viewSOOrder(orderId) {
  try {
    const res = await api.getSaleOrder(orderId);
    renderSOOrderDetail(res);
  } catch (e) {
    toast("조회 실패: " + e.message, "error");
  }
}

function renderSOOrderDetail(data) {
  const order = data.order;
  const lines = data.lines || [];
  const submissions = data.submissions || [];
  const container = document.getElementById("so-history-list");

  let erpHtml = "";
  if (submissions.length) {
    erpHtml = submissions.map(s => `
      <div style="display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--gray-100);font-size:12px">
        <span style="font-size:14px">${s.success ? "✅" : "❌"}</span>
        <div style="flex:1">
          <span style="font-weight:500">${s.success ? "전송 성공" : "전송 실패"}</span>
          ${s.erp_slip_no ? `<span style="margin-left:6px;color:var(--primary);font-weight:600">전표: ${s.erp_slip_no}</span>` : ""}
        </div>
        <span style="font-size:11px;color:var(--gray-400)">${s.submitted_at?.slice(0, 16) || ""}</span>
      </div>
    `).join("");
  } else {
    erpHtml = '<p style="color:var(--gray-400);font-size:13px">ERP 전송 이력 없음</p>';
  }

  const linesHtml = lines.length ? `
    <table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px">
      <thead>
        <tr style="border-bottom:2px solid var(--gray-200);color:var(--gray-500)">
          <th style="text-align:left;padding:5px 6px;width:32px">#</th>
          <th style="text-align:left;padding:5px 6px">원문 내용</th>
          <th style="text-align:left;padding:5px 6px;width:140px">선택 상품코드</th>
          <th style="text-align:center;padding:5px 6px;width:60px">수량</th>
          <th style="text-align:center;padding:5px 6px;width:45px">단위</th>
          <th style="text-align:center;padding:5px 6px;width:50px">상태</th>
        </tr>
      </thead>
      <tbody>
        ${lines.map(l => {
          const selCand = (l.candidates || []).find(c => c.was_selected);
          const selectedName = selCand ? selCand.prod_name : "";
          return `
          <tr style="border-bottom:1px solid var(--gray-100)">
            <td style="padding:4px 6px;color:var(--gray-400)">${l.line_no}</td>
            <td style="padding:4px 6px">
              <div style="font-weight:500">${l.raw_text || ""}</div>
              ${selectedName ? '<div style="font-size:11px;color:var(--gray-400);margin-top:1px">' + selectedName + '</div>' : ""}
            </td>
            <td style="padding:4px 6px;font-family:monospace;font-size:11px;color:var(--primary)">${l.selected_cd || '<span style="color:var(--danger)">미선택</span>'}</td>
            <td style="padding:4px 6px;text-align:center">${l.qty || "-"}</td>
            <td style="padding:4px 6px;text-align:center">${l.unit || "-"}</td>
            <td style="padding:4px 6px;text-align:center">${l.is_confirmed ? '<span style="color:var(--success);font-weight:600">확인</span>' : '<span style="color:var(--warning)">미확인</span>'}</td>
          </tr>`;
        }).join("")}
      </tbody>
    </table>
  ` : '<p style="color:var(--gray-400);font-size:12px">라인 데이터 없음</p>';

  const rawText = order.raw_text || "";

  container.innerHTML = `
    <div style="margin-bottom:10px">
      <button class="btn btn-outline btn-sm" onclick="loadSOHistory()">← 목록으로</button>
    </div>

    <div class="card" style="margin-bottom:12px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <div>
          <span style="font-size:15px;font-weight:700;color:var(--primary)">견적서 상세</span>
          <span style="font-size:12px;color:var(--gray-500);margin-left:10px">
            주문번호: <strong>${order.order_id}</strong>
          </span>
        </div>
        ${statusBadge(order.status)}
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;font-size:12px;padding:10px 14px;background:var(--gray-50);border-radius:6px;margin-bottom:12px">
        <div>
          <span style="color:var(--gray-400);font-size:11px">거래처</span>
          <div style="font-weight:600;margin-top:1px">${order.cust_name || "-"} <span style="color:var(--gray-400);font-weight:400;font-size:11px">(${order.cust_code || "-"})</span></div>
        </div>
        <div>
          <span style="color:var(--gray-400);font-size:11px">처리일시</span>
          <div style="font-weight:600;margin-top:1px">${order.created_at?.slice(0, 16) || "-"}</div>
        </div>
        <div>
          <span style="color:var(--gray-400);font-size:11px">라인 수</span>
          <div style="font-weight:600;margin-top:1px">${lines.length}건</div>
        </div>
        <div>
          <span style="color:var(--gray-400);font-size:11px">최종 수정</span>
          <div style="font-weight:600;margin-top:1px">${order.updated_at?.slice(0, 16) || "-"}</div>
        </div>
      </div>

      ${rawText ? `
      <div>
        <div style="font-size:12px;color:var(--gray-400);margin-bottom:6px;font-weight:600">발주서 원문</div>
        <div style="font-size:12px;color:var(--gray-600);background:#f7fafc;padding:10px 14px;border-radius:6px;white-space:pre-wrap;max-height:120px;overflow-y:auto;border:1px solid var(--gray-100)">${rawText}</div>
      </div>` : ""}
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="card-title">주문 라인 (${lines.length}건)</div>
      ${linesHtml}
    </div>

    <div class="card">
      <div class="card-title">ERP 전송 이력</div>
      ${erpHtml}
    </div>
  `;
}


function renderInventoryResults(results, query) {
  const container = document.getElementById("inv-results");

  if (!results.length) {
    container.innerHTML = `<p style="color:var(--gray-400);text-align:center;padding:32px">'${escapeHtml(query)}'에 해당하는 품목이 없습니다.</p>`;
    return;
  }

  let html = `<div style="font-size:12px;color:var(--gray-500);margin-bottom:8px">${results.length}개 품목 조회됨</div>`;

  results.forEach(r => {
    const inv = r.inventory || {};
    const items = inv.data || [];
    const totalQty = inv.total_qty || 0;
    const success = inv.success;

    // 품목 헤더 + 테이블을 컴팩트하게
    html += `
      <div style="border:1px solid var(--gray-200);border-radius:8px;margin-bottom:10px;overflow:hidden">
        <div style="padding:8px 14px;background:var(--gray-50);border-bottom:1px solid var(--gray-200);display:flex;justify-content:space-between;align-items:center">
          <div style="min-width:0;overflow:hidden">
            <span style="font-weight:700;font-size:13px;color:var(--primary)">${escapeHtml(r.prod_name)}</span>
            <span style="margin-left:8px;font-size:11px;color:var(--gray-400);font-family:monospace">${escapeHtml(r.prod_cd)}</span>
            ${r.model ? `<span style="margin-left:6px;font-size:11px;color:var(--gray-500)">[${escapeHtml(r.model)}]</span>` : ""}
          </div>
          <div style="text-align:right;white-space:nowrap;margin-left:12px">
            ${success
              ? `<span style="font-size:16px;font-weight:700;color:${totalQty > 0 ? 'var(--success)' : 'var(--danger)'}">${totalQty.toLocaleString()}</span>
                 <span style="font-size:11px;color:var(--gray-500);margin-left:3px">총 재고</span>`
              : `<span style="font-size:12px;color:var(--danger)">조회 실패</span>`
            }
          </div>
        </div>`;

    if (success && items.length > 0) {
      html += `
        <table style="width:100%;border-collapse:collapse;font-size:12px">
          <thead>
            <tr style="border-bottom:1px solid var(--gray-200);background:#fafbfc">
              <th style="text-align:left;padding:4px 14px;color:var(--gray-500);font-weight:600;width:100px">창고코드</th>
              <th style="text-align:left;padding:4px 14px;color:var(--gray-500);font-weight:600">창고명</th>
              <th style="text-align:right;padding:4px 14px;color:var(--gray-500);font-weight:600;width:100px">재고수량</th>
            </tr>
          </thead>
          <tbody>
            ${items.map(it => `
            <tr style="border-bottom:1px solid var(--gray-100)">
              <td style="padding:3px 14px;font-family:monospace;font-size:11px;color:var(--gray-500)">${escapeHtml(it.wh_cd)}</td>
              <td style="padding:3px 14px">${escapeHtml(it.wh_name)}</td>
              <td style="padding:3px 14px;text-align:right;font-weight:600;color:${it.qty > 0 ? 'var(--success)' : it.qty < 0 ? 'var(--danger)' : 'var(--gray-400)'}">
                ${it.qty.toLocaleString()}
              </td>
            </tr>`).join("")}
          </tbody>
        </table>`;
    } else if (success && items.length === 0) {
      html += `<div style="padding:10px;text-align:center;color:var(--gray-400);font-size:12px">재고 데이터 없음</div>`;
    } else {
      html += `<div style="padding:10px;text-align:center;color:var(--danger);font-size:12px">${escapeHtml(inv.error || '조회 실패')}</div>`;
    }

    html += `</div>`;
  });

  container.innerHTML = html;
}


// ═══════════════════════════════════════════
//  자료검색 (Google Drive 문서 브라우저)
// ═══════════════════════════════════════════
let _docSearchState = { categoriesLoaded: false, currentPage: 0 };
let _docSearchTimer = null;

async function initDocSearchPage() {
  if (!_docSearchState.categoriesLoaded) {
    const ok = await loadDocCategories();
    if (ok) _docSearchState.categoriesLoaded = true;
  }
}

async function loadDocCategories() {
  const sel = document.getElementById("doc-category-select");
  try {
    console.log("[자료검색] 카테고리 로드 시작...");
    const data = await api.driveCategories();
    console.log("[자료검색] 카테고리 응답:", JSON.stringify(data));
    const cats = data.categories || [];
    let html = '<option value="">전체</option>';
    cats.forEach(c => {
      if (c.category) {
        html += `<option value="${escapeHtml(c.category)}">${escapeHtml(c.category)} (${c.count})</option>`;
      }
    });
    sel.innerHTML = html;
    console.log("[자료검색] 카테고리 로드 완료:", cats.length, "개");
    if (!cats.length) {
      document.getElementById("doc-search-results").innerHTML =
        '<p style="color:var(--gray-400);text-align:center;padding:40px 0">동기화된 자료가 없습니다. 🔄 동기화 버튼을 클릭하세요.</p>';
    }
    return true;
  } catch (e) {
    console.error("카테고리 로드 실패:", e);
    toast("카테고리 로드 실패: " + e.message, "error");
    return false;
  }
}

function onDocSearchInput() {
  clearTimeout(_docSearchTimer);
  _docSearchTimer = setTimeout(() => loadDocuments(), 300);
}

async function loadDocuments(page = 0) {
  const category = document.getElementById("doc-category-select").value;
  const query = document.getElementById("doc-search-input").value.trim();
  const el = document.getElementById("doc-search-results");
  const statusEl = document.getElementById("doc-search-status");
  const pagEl = document.getElementById("doc-search-pagination");
  const limit = 100;
  const offset = page * limit;

  _docSearchState.currentPage = page;
  el.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:30px">로딩 중...</p>';

  try {
    const data = await api.driveDocuments(category, query, limit, offset);
    const docs = data.documents || [];
    const total = data.total || 0;

    statusEl.textContent = `총 ${total}건${query ? ` (검색: "${query}")` : ""}${category ? ` [${category}]` : ""}`;

    if (!docs.length) {
      el.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:30px">검색 결과가 없습니다.</p>';
      pagEl.innerHTML = "";
      return;
    }

    // 테이블 렌더링
    el.innerHTML = `
      <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead>
          <tr style="border-bottom:2px solid var(--gray-200);color:var(--gray-500);position:sticky;top:0;background:#fff;z-index:1">
            <th style="text-align:left;padding:5px 8px;width:90px">카테고리</th>
            <th style="text-align:left;padding:5px 8px">파일명</th>
            <th style="text-align:left;padding:5px 8px;width:160px">폴더 경로</th>
          </tr>
        </thead>
        <tbody>
          ${docs.map(d => {
            const catColor = _docCatColor(d.category);
            const name = escapeHtml(d.file_name);
            const highlighted = query ? _highlightText(name, query) : name;
            return `
            <tr style="border-bottom:1px solid var(--gray-100)">
              <td style="padding:4px 8px">
                <span style="font-size:11px;padding:1px 7px;border-radius:10px;background:${catColor.bg};color:${catColor.text}">${escapeHtml(d.category)}</span>
              </td>
              <td style="padding:4px 8px;word-break:break-all"><a href="${d.file_url}" target="_blank" style="color:var(--text-primary);text-decoration:none;cursor:pointer" title="Google Drive에서 열기" onmouseover="this.style.color='var(--accent)';this.style.textDecoration='underline'" onmouseout="this.style.color='var(--text-primary)';this.style.textDecoration='none'">${highlighted}</a></td>
              <td style="padding:4px 8px;font-size:11px;color:var(--gray-500)">${escapeHtml(d.folder_path || "")}</td>
            </tr>`;
          }).join("")}
        </tbody>
      </table>`;

    // 페이지네이션
    const totalPages = Math.ceil(total / limit);
    if (totalPages > 1) {
      let pagHtml = '<div style="display:flex;gap:4px;justify-content:center;align-items:center">';
      if (page > 0) pagHtml += `<button class="btn btn-outline btn-sm" onclick="loadDocuments(${page - 1})">← 이전</button>`;
      pagHtml += `<span style="font-size:12px;color:var(--gray-500);margin:0 8px">${page + 1} / ${totalPages}</span>`;
      if (page < totalPages - 1) pagHtml += `<button class="btn btn-outline btn-sm" onclick="loadDocuments(${page + 1})">다음 →</button>`;
      pagHtml += '</div>';
      pagEl.innerHTML = pagHtml;
    } else {
      pagEl.innerHTML = "";
    }
  } catch (e) {
    el.innerHTML = `<p style="color:var(--danger);text-align:center;padding:30px">로드 실패: ${escapeHtml(e.message)}</p>`;
    statusEl.textContent = "";
    pagEl.innerHTML = "";
  }
}

function _docCatColor(cat) {
  const map = {
    "데이터시트":  { bg: "#ebf8ff", text: "#2b6cb0" },
    "Fluke":      { bg: "#fefcbf", text: "#975a16" },
    "KC인증서":    { bg: "#c6f6d5", text: "#276749" },
    "ROHS":       { bg: "#fed7e2", text: "#97266d" },
    "Test리포트":  { bg: "#e9d8fd", text: "#553c9a" },
    "UL":         { bg: "#feebc8", text: "#9c4221" },
  };
  return map[cat] || { bg: "#edf2f7", text: "#4a5568" };
}

function _highlightText(text, query) {
  if (!query) return text;
  const keywords = query.split(/\s+/).filter(Boolean);
  let result = text;
  keywords.forEach(kw => {
    const regex = new RegExp(`(${kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "gi");
    result = result.replace(regex, '<mark style="background:#fef08a;padding:0 1px;border-radius:2px">$1</mark>');
  });
  return result;
}

async function docSearchSync() {
  const btn = document.getElementById("doc-sync-btn");
  btn.disabled = true;
  btn.textContent = "동기화 중...";
  const statusEl = document.getElementById("doc-search-status");
  statusEl.textContent = "⏳ Google Drive 동기화 중... (시간이 걸릴 수 있습니다)";

  try {
    // Drive 폴더 소스 찾기
    const sources = await api.materialsSources();
    const driveSources = (sources.sources || []).filter(s => s.source_type === "drive_folder" && s.is_active);
    if (!driveSources.length) {
      statusEl.textContent = "❌ 등록된 Drive 폴더가 없습니다.";
      return;
    }
    // 각 소스 동기화
    let totalFiles = 0;
    for (const src of driveSources) {
      const res = await api.materialsSyncOne(src.id);
      totalFiles += res.files_synced || 0;
    }
    statusEl.textContent = `✅ 동기화 완료: ${totalFiles}개 파일`;
    toast(`자료 동기화 완료: ${totalFiles}개 파일`, "success");
    // 카테고리 새로고침
    _docSearchState.categoriesLoaded = false;
    await loadDocCategories();
    loadDocuments(0);
  } catch (e) {
    statusEl.textContent = `❌ 동기화 실패: ${e.message}`;
    toast("동기화 실패: " + e.message, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = "🔄 동기화";
  }
}


// ═══════════════════════════════════════════
//  단가표 조회 (Google Sheets 뷰어)
// ═══════════════════════════════════════════
let _psState = { vendorsLoaded: false, currentPage: 0, activeTab: "", tabs: [] };

async function initPriceSheetPage() {
  if (!_psState.vendorsLoaded) {
    const ok = await loadPSVendors();
    if (ok) _psState.vendorsLoaded = true;
  }
}

async function loadPSVendors() {
  const sel = document.getElementById("ps-vendor-select");
  try {
    console.log("[단가표] 거래처 로드 시작...");
    const data = await api.priceSheetVendors();
    console.log("[단가표] 거래처 응답:", JSON.stringify(data));
    const vendors = data.vendors || [];
    let html = '<option value="">-- 거래처 선택 --</option>';
    vendors.forEach(v => {
      const info = v.row_count > 0 ? ` (${v.row_count}행)` : " (미동기화)";
      html += `<option value="${v.source_id}">${escapeHtml(v.vendor || v.name)}${info}</option>`;
    });
    sel.innerHTML = html;
    console.log("[단가표] 거래처 로드 완료:", vendors.length, "개");
    return true;
  } catch (e) {
    console.error("거래처 로드 실패:", e);
    toast("거래처 로드 실패: " + e.message, "error");
    return false;
  }
}

let _psSearchTimer = null;
function onPsSearchInput() {
  clearTimeout(_psSearchTimer);
  _psSearchTimer = setTimeout(() => loadPriceSheet(0), 300);
}

function onVendorChange() {
  document.getElementById("ps-search-input").value = "";
  _psState.activeTab = "";  // 탭 리셋
  document.getElementById("ps-tab-bar").style.display = "none";
  document.getElementById("ps-tab-bar").innerHTML = "";
  loadPriceSheet(0);
}

function onTabClick(tab) {
  _psState.activeTab = tab;
  _renderTabBar();  // 활성 상태 업데이트
  loadPriceSheet(0);
}

function _renderTabBar() {
  const bar = document.getElementById("ps-tab-bar");
  const tabs = _psState.tabs || [];
  if (tabs.length <= 1) {
    bar.style.display = "none";
    return;
  }
  bar.style.display = "block";
  const active = _psState.activeTab || "";
  let html = '';
  // "전체" 탭
  const allActive = !active;
  html += `<button onclick="onTabClick('')" style="
    display:inline-block;padding:6px 14px;margin-right:2px;border:none;cursor:pointer;
    font-size:13px;font-weight:${allActive ? '600' : '400'};
    color:${allActive ? 'var(--primary)' : 'var(--gray-500)'};
    background:${allActive ? '#fff' : 'transparent'};
    border-bottom:2px solid ${allActive ? 'var(--primary)' : 'transparent'};
    margin-bottom:-2px;transition:all .15s
  ">전체</button>`;
  tabs.forEach(t => {
    const isActive = active === t.tab;
    html += `<button onclick="onTabClick('${escapeHtml(t.tab)}')" style="
      display:inline-block;padding:6px 14px;margin-right:2px;border:none;cursor:pointer;
      font-size:13px;font-weight:${isActive ? '600' : '400'};
      color:${isActive ? 'var(--primary)' : 'var(--gray-500)'};
      background:${isActive ? '#fff' : 'transparent'};
      border-bottom:2px solid ${isActive ? 'var(--primary)' : 'transparent'};
      margin-bottom:-2px;transition:all .15s
    ">${escapeHtml(t.tab)} <span style="font-size:11px;color:var(--gray-400)">(${t.row_count})</span></button>`;
  });
  bar.innerHTML = html;
}

async function loadPriceSheet(page = 0) {
  const sourceId = document.getElementById("ps-vendor-select").value;
  const query = document.getElementById("ps-search-input").value.trim();
  // 검색어가 있으면 전체 탭에서 검색 (탭 필터 무시)
  const tab = query ? "" : (_psState.activeTab || "");
  const el = document.getElementById("ps-results");
  const statusEl = document.getElementById("ps-status");
  const pagEl = document.getElementById("ps-pagination");

  if (!sourceId) {
    el.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:40px 0">거래처를 선택하면 단가표가 표시됩니다.</p>';
    statusEl.textContent = "";
    pagEl.innerHTML = "";
    document.getElementById("ps-tab-bar").style.display = "none";
    return;
  }

  const limit = 200;
  const offset = page * limit;
  _psState.currentPage = page;

  el.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:30px">로딩 중...</p>';

  try {
    const data = await api.priceSheetData(sourceId, query, tab, limit, offset);

    if (data.error) {
      el.innerHTML = `<p style="color:var(--danger);text-align:center;padding:30px">${escapeHtml(data.error)}</p>`;
      return;
    }

    const rows = data.rows || [];
    const headers = data.headers || [];
    const total = data.total || 0;

    // 탭 바 업데이트 (첫 로드 시 또는 탭 데이터 변경 시)
    const tabs = data.tabs || [];
    if (tabs.length > 0) {
      _psState.tabs = tabs;
      _renderTabBar();
    }

    const activeTabLabel = _psState.activeTab ? ` [${_psState.activeTab}]` : "";
    const searchScope = query && _psState.activeTab ? " (전체 탭 검색)" : "";
    statusEl.textContent = `${data.source_name || ""}${query ? "" : activeTabLabel} — 총 ${total}행${query ? ` (검색: "${query}"${searchScope})` : ""}`;

    if (!rows.length) {
      el.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:30px">데이터가 없습니다. 동기화를 먼저 실행해주세요.</p>';
      pagEl.innerHTML = "";
      return;
    }

    // 불필요하거나 HTML/긴 텍스트 포함 컬럼 제외
    const _hideColumns = new Set(["상세DB", "상세요약", "썸네일", "링크", "주요사양", "품질보증기준", "AS책임자", "AS전화번호"]);
    const displayHeaders = headers.filter(h => h && h.trim() && !h.trim().match(/^\d+$/) && !_hideColumns.has(h.trim()));

    let html = `<table style="width:100%;border-collapse:collapse;font-size:12px;min-width:${Math.max(800, displayHeaders.length * 100)}px">
      <thead>
        <tr style="border-bottom:2px solid var(--gray-200);color:var(--gray-500);position:sticky;top:0;background:#fff;z-index:1">
          <th style="padding:6px 8px;text-align:center;width:40px;font-size:11px">#</th>`;

    displayHeaders.forEach(h => {
      html += `<th style="padding:6px 8px;text-align:left;white-space:nowrap;max-width:200px;overflow:hidden;text-overflow:ellipsis" title="${escapeHtml(h)}">${escapeHtml(h)}</th>`;
    });
    html += '</tr></thead><tbody>';

    rows.forEach((row, idx) => {
      const rd = row.row_data || {};
      const rc = row.row_colors || {};  // 셀별 배경색 {"컬럼명": "#hex"}
      // 행 전체 배경색 결정 (첫 번째 색상 값 사용)
      const rowBgValues = Object.values(rc);
      const rowBg = rowBgValues.length > 0 ? rowBgValues[0] : "";
      // _row 키가 있으면 행 전체 색상, 아니면 첫 번째 색상 사용
      const rowBgColor = rc["_row"] || rowBg;
      html += `<tr style="border-bottom:1px solid var(--gray-100)${rowBgColor ? `;background-color:${rowBgColor}40` : ''}">
        <td style="padding:5px 8px;text-align:center;color:var(--gray-400);font-size:11px">${offset + idx + 1}</td>`;
      displayHeaders.forEach(h => {
        let val = rd[h] || "";
        if (val.length > 100) val = val.substring(0, 100) + "…";
        const isPrice = /^\d{1,3}(,\d{3})*$/.test(val.replace(/원$/, ""));
        const cellColor = rc[h] || "";
        let style = isPrice
          ? "text-align:right;font-weight:600;color:var(--primary)"
          : "text-align:left";
        if (cellColor) style += `;background-color:${cellColor}50`;
        let display = escapeHtml(val);
        if (query) display = _highlightText(display, query);
        html += `<td style="padding:5px 8px;${style};max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(val)}">${display || '<span style="color:var(--gray-300)">-</span>'}</td>`;
      });
      html += '</tr>';
    });

    html += '</tbody></table>';
    el.innerHTML = html;

    const totalPages = Math.ceil(total / limit);
    if (totalPages > 1) {
      let pagHtml = '<div style="display:flex;gap:4px;justify-content:center;align-items:center">';
      if (page > 0) pagHtml += `<button class="btn btn-outline btn-sm" onclick="loadPriceSheet(${page - 1})">← 이전</button>`;
      pagHtml += `<span style="font-size:12px;color:var(--gray-500);margin:0 8px">${page + 1} / ${totalPages}</span>`;
      if (page < totalPages - 1) pagHtml += `<button class="btn btn-outline btn-sm" onclick="loadPriceSheet(${page + 1})">다음 →</button>`;
      pagHtml += '</div>';
      pagEl.innerHTML = pagHtml;
    } else {
      pagEl.innerHTML = "";
    }
  } catch (e) {
    el.innerHTML = `<p style="color:var(--danger);text-align:center;padding:30px">로드 실패: ${escapeHtml(e.message)}</p>`;
    statusEl.textContent = "";
    pagEl.innerHTML = "";
  }
}

async function priceSheetSyncAll() {
  const btn = document.getElementById("ps-sync-btn");
  const statusEl = document.getElementById("ps-status");
  btn.disabled = true;
  btn.textContent = "동기화 중...";
  statusEl.textContent = "⏳ 전체 시트 동기화 중... (시간이 걸릴 수 있습니다)";

  try {
    const res = await api.materialsSyncAll();
    const sheets = res.sheets || {};
    statusEl.textContent = `✅ 동기화 완료: ${sheets.success_count || 0}/${sheets.total_sources || 0}개 시트, 총 ${sheets.total_rows || 0}행`;
    toast(`단가표 동기화 완료: ${sheets.total_rows || 0}행`, "success");
    _psState.vendorsLoaded = false;
    await loadPSVendors();
    // 현재 선택된 거래처가 있으면 새로고침
    if (document.getElementById("ps-vendor-select").value) {
      loadPriceSheet(0);
    }
  } catch (e) {
    statusEl.textContent = `❌ 동기화 실패: ${e.message}`;
    toast("동기화 실패: " + e.message, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = "🔄 전체 동기화";
  }
}


/* =============================================
   발주서 학습 (Training) 페이지
   ============================================= */

// ─── 전역 상태 ───
let _trState = {
  initialized: false,
  custCode: "",
  custName: "",
  previewData: null,  // 파싱된 엑셀 데이터
  poInputMode: "text",  // "text" or "image"
  poImageBase64: "",
  poImageType: "",
  poImageName: "",
};

async function initTrainingPage() {
  if (!_trState.initialized) {
    _trState.initialized = true;
  }
  loadTrainingStats();
  loadTrainingPairs();
}

// ─── 거래처 검색 (학습용) ───
let _trCustTimer = null;
async function onTrainingCustSearch(query) {
  const dd = document.getElementById("tr-cust-dropdown");
  if (!query.trim()) { dd.style.display = "none"; return; }
  clearTimeout(_trCustTimer);
  _trCustTimer = setTimeout(async () => {
    try {
      const data = await api.get(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const matches = data.customers || [];
      if (!matches.length) {
        dd.innerHTML = '<div style="padding:10px 14px;color:#a0aec0;font-size:13px">검색 결과 없음</div>';
      } else {
        dd.innerHTML = matches.map(c =>
          `<div onclick="selectTrainingCust('${c.cust_code.replace(/'/g,"\\'")}','${c.cust_name.replace(/'/g,"\\'")}')"
            style="padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid #f7fafc"
            onmouseover="this.style.background='#ebf8ff'" onmouseout="this.style.background=''">
            <strong>${c.cust_name}</strong>
            <span style="color:#a0aec0;margin-left:6px;font-size:12px">${c.cust_code}</span>
          </div>`
        ).join("");
      }
      dd.style.display = "block";
      if (_dropdownNav["tr-cust-search"]) _dropdownNav["tr-cust-search"].idx = -1;
    } catch (e) {
      console.error("거래처 검색 실패:", e);
    }
  }, 300);
}

function showTrainingCustDropdown() {
  const dd = document.getElementById("tr-cust-dropdown");
  const q = document.getElementById("tr-cust-search").value.trim();
  if (q && dd.innerHTML) dd.style.display = "block";
}

function selectTrainingCust(code, name) {
  _trState.custCode = code;
  _trState.custName = name;
  document.getElementById("tr-cust-code").value = code;
  document.getElementById("tr-cust-name").value = name;
  document.getElementById("tr-cust-search").value = name;
  document.getElementById("tr-cust-selected").textContent = `${name} (${code})`;
  document.getElementById("tr-cust-selected").style.display = "block";
  document.getElementById("tr-cust-dropdown").style.display = "none";
  _checkTrainingSaveReady();
}

// ─── 발주서 입력 방식 전환 (텍스트/이미지) ───
function switchPoInputTab(mode) {
  _trState.poInputMode = mode;
  const textArea = document.getElementById("tr-po-text-area");
  const imageArea = document.getElementById("tr-po-image-area");
  const tabText = document.getElementById("tr-po-tab-text");
  const tabImage = document.getElementById("tr-po-tab-image");
  if (mode === "text") {
    textArea.style.display = "";
    imageArea.style.display = "none";
    tabText.style.background = "var(--primary)";
    tabText.style.color = "#fff";
    tabImage.style.background = "var(--gray-200)";
    tabImage.style.color = "var(--gray-600)";
  } else {
    textArea.style.display = "none";
    imageArea.style.display = "";
    tabImage.style.background = "var(--primary)";
    tabImage.style.color = "#fff";
    tabText.style.background = "var(--gray-200)";
    tabText.style.color = "var(--gray-600)";
  }
}

// ─── 발주서 이미지 업로드 처리 ───
function handlePoImageDrop(e) {
  e.preventDefault();
  const dz = document.getElementById("tr-po-image-dropzone");
  dz.style.borderColor = "var(--gray-300)";
  dz.style.background = "#fafbfc";
  const files = e.dataTransfer.files;
  if (files.length > 0) loadPoImage(files[0]);
}

function handlePoImageSelect(input) {
  if (input.files.length > 0) loadPoImage(input.files[0]);
}

function loadPoImage(file) {
  const allowed = [".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf"];
  const ext = (file.name || "").toLowerCase().match(/\.\w+$/);
  if (!ext || !allowed.includes(ext[0])) {
    toast("지원하지 않는 파일 형식입니다. (jpg, png, gif, webp, pdf)", "error");
    return;
  }
  if (file.size > 10 * 1024 * 1024) {
    toast("파일 크기가 10MB를 초과합니다.", "error");
    return;
  }

  const reader = new FileReader();
  reader.onload = (e) => {
    const dataUrl = e.target.result;
    // data:image/png;base64,xxxx... → base64 부분 추출
    const base64 = dataUrl.split(",")[1];
    const mimeMatch = dataUrl.match(/^data:([^;]+);/);
    const mime = mimeMatch ? mimeMatch[1] : "image/png";

    _trState.poImageBase64 = base64;
    _trState.poImageType = mime;
    _trState.poImageName = file.name;

    // 미리보기 표시
    const preview = document.getElementById("tr-po-image-preview");
    const thumb = document.getElementById("tr-po-image-thumb");
    const info = document.getElementById("tr-po-image-info");
    const dropzone = document.getElementById("tr-po-image-dropzone");

    if (mime === "application/pdf") {
      thumb.src = "";
      thumb.style.display = "none";
      info.innerHTML = `📄 <strong>${escapeHtml(file.name)}</strong> (${(file.size / 1024).toFixed(1)}KB) — PDF 파일`;
    } else {
      thumb.src = dataUrl;
      thumb.style.display = "";
      info.innerHTML = `📷 <strong>${escapeHtml(file.name)}</strong> (${(file.size / 1024).toFixed(1)}KB)`;
    }
    preview.style.display = "";
    dropzone.style.display = "none";
  };
  reader.readAsDataURL(file);
}

function clearPoImage() {
  _trState.poImageBase64 = "";
  _trState.poImageType = "";
  _trState.poImageName = "";
  document.getElementById("tr-po-image-preview").style.display = "none";
  document.getElementById("tr-po-image-dropzone").style.display = "";
  document.getElementById("tr-po-image-file").value = "";
}

// ─── 엑셀 파일 처리 ───
function handleTrainingFileDrop(e) {
  e.preventDefault();
  const dz = document.getElementById("tr-excel-dropzone");
  dz.style.borderColor = "var(--gray-300)";
  dz.style.background = "#fafbfc";
  const files = e.dataTransfer.files;
  if (files.length > 0) previewTrainingExcel(files[0]);
}

function handleTrainingFileSelect(input) {
  if (input.files.length > 0) previewTrainingExcel(input.files[0]);
}

async function previewTrainingExcel(file) {
  const dz = document.getElementById("tr-excel-dropzone");
  const preview = document.getElementById("tr-preview");
  const titleEl = document.getElementById("tr-preview-title");
  const tableEl = document.getElementById("tr-preview-table");

  dz.innerHTML = `<div style="font-size:14px;color:var(--primary)">파싱 중... ${escapeHtml(file.name)}</div>`;

  const formData = new FormData();
  formData.append("file", file);

  try {
    const data = await api.trainingPreviewExcel(formData);

    if (!data.items || data.items.length === 0) {
      toast("엑셀에서 품목 데이터를 찾을 수 없습니다.", "error");
      clearTrainingPreview();
      return;
    }

    _trState.previewData = data;

    dz.innerHTML = `
      <div style="font-size:14px;font-weight:500;color:var(--primary)">${escapeHtml(file.name)}</div>
      <div style="font-size:12px;color:var(--gray-500);margin-top:4px">${data.total_items}개 품목 파싱 완료</div>
    `;

    titleEl.textContent = `파싱 결과: ${data.total_items}개 품목${data.vendor ? ` (${data.vendor})` : ""}`;

    let html = '<table style="width:100%;border-collapse:collapse">';
    html += '<thead><tr style="background:#f7fafc;border-bottom:1px solid var(--gray-200)">';
    html += '<th style="padding:4px 6px;text-align:left">품목코드</th>';
    html += '<th style="padding:4px 6px;text-align:left">품명</th>';
    html += '<th style="padding:4px 6px;text-align:left">모델명</th>';
    html += '<th style="padding:4px 6px;text-align:right">수량</th>';
    html += '<th style="padding:4px 6px;text-align:right">단가</th>';
    html += '<th style="padding:4px 6px;text-align:right">공급가</th>';
    html += '</tr></thead><tbody>';

    data.items.forEach((item, idx) => {
      html += `<tr style="border-bottom:1px solid var(--gray-100)${idx % 2 ? ';background:#fafbfc' : ''}">`;
      html += `<td style="padding:3px 6px;font-weight:600;color:var(--primary)">${escapeHtml(item.item_code)}</td>`;
      html += `<td style="padding:3px 6px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(item.product_name)}">${escapeHtml(item.product_name)}</td>`;
      html += `<td style="padding:3px 6px">${escapeHtml(item.model_name)}</td>`;
      html += `<td style="padding:3px 6px;text-align:right">${item.qty || "-"}</td>`;
      html += `<td style="padding:3px 6px;text-align:right">${item.unit_price ? Number(item.unit_price).toLocaleString() : "-"}</td>`;
      html += `<td style="padding:3px 6px;text-align:right">${item.supply_price ? Number(item.supply_price).toLocaleString() : "-"}</td>`;
      html += '</tr>';
    });

    html += '</tbody></table>';
    tableEl.innerHTML = html;
    preview.style.display = "block";

    _checkTrainingSaveReady();
    toast(`${data.total_items}개 품목 파싱 완료`, "success");

  } catch (e) {
    toast("엑셀 파싱 실패: " + e.message, "error");
    clearTrainingPreview();
  }
}

function clearTrainingPreview() {
  _trState.previewData = null;
  document.getElementById("tr-preview").style.display = "none";
  document.getElementById("tr-excel-file").value = "";
  document.getElementById("tr-excel-dropzone").innerHTML = `
    <div style="font-size:28px;margin-bottom:8px">📊</div>
    <div style="font-size:14px;font-weight:500;color:var(--gray-600)">판매전표 엑셀 파일을 드래그하거나 클릭하세요</div>
    <div style="font-size:12px;color:var(--gray-400);margin-top:4px">.xlsx, .xls, .xlsm 지원 (최대 10MB)</div>
  `;
  _checkTrainingSaveReady();
}

function _checkTrainingSaveReady() {
  const ready = _trState.custCode && _trState.previewData && _trState.previewData.items.length > 0;
  document.getElementById("tr-save-btn").disabled = !ready;
}

// ─── 학습 데이터 저장 ───
async function saveTrainingData() {
  if (!_trState.custCode || !_trState.previewData) {
    toast("거래처와 판매전표 엑셀을 먼저 입력해주세요.", "error");
    return;
  }

  const btn = document.getElementById("tr-save-btn");
  btn.disabled = true;
  btn.textContent = "저장 중...";

  try {
    const body = {
      cust_code: _trState.custCode,
      cust_name: _trState.custName,
      raw_po_text: _trState.poInputMode === "text" ? document.getElementById("tr-raw-po").value.trim() : "",
      raw_po_image_base64: _trState.poInputMode === "image" ? _trState.poImageBase64 : "",
      raw_po_image_type: _trState.poInputMode === "image" ? _trState.poImageType : "",
      items: _trState.previewData.items,
      memo: document.getElementById("tr-memo").value.trim(),
    };

    const result = await api.trainingSaveJson(body);
    toast(result.message || "학습 데이터 저장 완료", "success");

    // 폼 초기화
    document.getElementById("tr-raw-po").value = "";
    document.getElementById("tr-memo").value = "";
    clearPoImage();
    switchPoInputTab("text");
    clearTrainingPreview();

    // 목록 새로고침
    loadTrainingPairs();
    loadTrainingStats();

  } catch (e) {
    toast("저장 실패: " + e.message, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = "학습 데이터 저장";
    _checkTrainingSaveReady();
  }
}

// ─── 통계 로드 ───
async function loadTrainingStats() {
  try {
    const data = await api.trainingStats();
    const el = document.getElementById("training-stats");
    if (data.total_pairs === 0) {
      el.textContent = "등록된 학습 데이터 없음";
    } else {
      el.innerHTML = `
        <span style="font-weight:600;color:var(--primary)">${data.total_pairs}</span>건 매칭 /
        <span style="font-weight:600;color:var(--primary)">${data.total_items}</span>개 품목 /
        <span style="font-weight:600">${data.vendors.length}</span>개 거래처
      `;
    }
  } catch (e) {
    console.error("학습 통계 로드 실패:", e);
  }
}

// ─── 학습 데이터 목록 ───
async function loadTrainingPairs() {
  const el = document.getElementById("training-list");
  try {
    const data = await api.trainingPairs("", 50);
    const pairs = data.pairs || [];

    if (!pairs.length) {
      el.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:30px">등록된 학습 데이터가 없습니다.<br>위에서 원본 발주서 + 판매전표 엑셀을 매칭하여 등록하세요.</p>';
      return;
    }

    let html = '<table style="width:100%;border-collapse:collapse;font-size:12px">';
    html += '<thead><tr style="border-bottom:2px solid var(--gray-200);color:var(--gray-500)">';
    html += '<th style="padding:5px 8px;text-align:left">거래처</th>';
    html += '<th style="padding:5px 8px;text-align:center;width:50px">품목</th>';
    html += '<th style="padding:5px 8px;text-align:left">발주서 원문</th>';
    html += '<th style="padding:5px 8px;text-align:left">메모</th>';
    html += '<th style="padding:5px 8px;text-align:left;width:85px">등록일</th>';
    html += '<th style="padding:5px 8px;text-align:center;width:100px"></th>';
    html += '</tr></thead><tbody>';

    pairs.forEach(p => {
      let rawSnippet;
      if (p.has_image) {
        rawSnippet = '📷 <span style="color:var(--accent)">이미지</span>';
      } else if (p.raw_po_text) {
        rawSnippet = escapeHtml(p.raw_po_text.substring(0, 50)) + (p.raw_po_text.length > 50 ? "…" : "");
      } else {
        rawSnippet = '<span style="color:var(--gray-300)">-</span>';
      }
      html += `<tr style="border-bottom:1px solid var(--gray-100)">`;
      html += `<td style="padding:4px 8px;font-weight:600">${escapeHtml(p.cust_name)}</td>`;
      html += `<td style="padding:4px 8px;text-align:center"><span style="background:var(--primary);color:#fff;border-radius:10px;padding:1px 7px;font-size:11px;font-weight:600">${p.item_count}</span></td>`;
      html += `<td style="padding:4px 8px;color:var(--gray-600);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${rawSnippet}</td>`;
      html += `<td style="padding:4px 8px;color:var(--gray-500)">${escapeHtml(p.memo || "-")}</td>`;
      html += `<td style="padding:4px 8px;color:var(--gray-500);font-size:11px">${(p.created_at || "").slice(0, 10)}</td>`;
      html += `<td style="padding:4px 8px;text-align:center;white-space:nowrap">
        <button class="btn btn-outline btn-sm" style="font-size:11px;padding:2px 8px;margin-right:2px" onclick="viewTrainingDetail(${p.id})">보기</button>
        <button class="btn btn-sm" style="font-size:11px;padding:2px 8px;background:#e53e3e;color:#fff;border:none" onclick="deleteTrainingPair(${p.id})">삭제</button>
      </td>`;
      html += '</tr>';
    });

    html += '</tbody></table>';
    el.innerHTML = html;

  } catch (e) {
    el.innerHTML = `<p style="color:var(--danger);text-align:center;padding:20px">로드 실패: ${escapeHtml(e.message)}</p>`;
  }
}

// ─── 상세 보기 ───
async function viewTrainingDetail(pairId) {
  try {
    const data = await api.trainingPairDetail(pairId);
    if (!data || !data.items) {
      toast("데이터를 찾을 수 없습니다.", "error");
      return;
    }

    let html = `<div style="padding:16px">`;
    html += `<h3 style="margin:0 0 8px;font-size:16px">${escapeHtml(data.cust_name)} (${escapeHtml(data.cust_code)})</h3>`;

    if (data.has_image) {
      html += `<div style="margin-bottom:12px"><strong>발주서 이미지:</strong><br>
        <img src="/api/training/pairs/${pairId}/image" style="max-width:100%;max-height:300px;border-radius:6px;border:1px solid var(--gray-200);margin-top:4px" onerror="this.style.display='none'">
      </div>`;
    }
    if (data.raw_po_text) {
      html += `<div style="margin-bottom:12px"><strong>발주서 원문:</strong><pre style="background:#f7fafc;padding:10px;border-radius:6px;font-size:12px;max-height:150px;overflow:auto;white-space:pre-wrap">${escapeHtml(data.raw_po_text)}</pre></div>`;
    }

    html += `<div style="margin-bottom:8px"><strong>매칭된 품목 (${data.items.length}건):</strong></div>`;
    html += '<table style="width:100%;border-collapse:collapse;font-size:12px">';
    html += '<thead><tr style="background:#f7fafc;border-bottom:1px solid var(--gray-200)">';
    html += '<th style="padding:4px 6px;text-align:left">품목코드</th>';
    html += '<th style="padding:4px 6px;text-align:left">품명</th>';
    html += '<th style="padding:4px 6px;text-align:left">모델명</th>';
    html += '<th style="padding:4px 6px;text-align:right">수량</th>';
    html += '<th style="padding:4px 6px;text-align:right">단가</th>';
    html += '</tr></thead><tbody>';

    data.items.forEach(item => {
      html += '<tr style="border-bottom:1px solid var(--gray-100)">';
      html += `<td style="padding:3px 6px;font-weight:600;color:var(--primary)">${escapeHtml(item.item_code)}</td>`;
      html += `<td style="padding:3px 6px">${escapeHtml(item.product_name)}</td>`;
      html += `<td style="padding:3px 6px">${escapeHtml(item.model_name)}</td>`;
      html += `<td style="padding:3px 6px;text-align:right">${item.qty || "-"}</td>`;
      html += `<td style="padding:3px 6px;text-align:right">${item.unit_price ? Number(item.unit_price).toLocaleString() : "-"}</td>`;
      html += '</tr>';
    });

    html += '</tbody></table></div>';

    // 모달로 표시
    const modal = document.createElement("div");
    modal.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9999;display:flex;align-items:center;justify-content:center";
    modal.onclick = (e) => { if (e.target === modal) modal.remove(); };

    const content = document.createElement("div");
    content.style.cssText = "background:#fff;border-radius:12px;max-width:800px;width:90%;max-height:80vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,0.3)";
    content.innerHTML = html + `<div style="padding:0 16px 16px;text-align:right"><button class="btn btn-outline btn-sm" onclick="this.closest('div[style*=fixed]').remove()">닫기</button></div>`;

    modal.appendChild(content);
    document.body.appendChild(modal);

  } catch (e) {
    toast("상세 조회 실패: " + e.message, "error");
  }
}

// ─── 삭제 ───
async function deleteTrainingPair(pairId) {
  if (!confirm("이 학습 데이터를 삭제하시겠습니까?")) return;
  try {
    await api.trainingDeletePair(pairId);
    toast("삭제 완료", "success");
    loadTrainingPairs();
    loadTrainingStats();
  } catch (e) {
    toast("삭제 실패: " + e.message, "error");
  }
}

// 거래처 드롭다운 외부 클릭 시 닫기
document.addEventListener("click", (e) => {
  const dd = document.getElementById("tr-cust-dropdown");
  const search = document.getElementById("tr-cust-search");
  if (dd && search && !search.contains(e.target) && !dd.contains(e.target)) {
    dd.style.display = "none";
  }
  // 대량학습 거래처 드롭다운 닫기
  const bdd = document.getElementById("bulk-cust-dropdown");
  const bs = document.getElementById("bulk-cust-search");
  if (bdd && bs && !bs.contains(e.target) && !bdd.contains(e.target)) {
    bdd.style.display = "none";
  }
});


/* =============================================
   대량 학습 (Bulk Training)
   ============================================= */
const _bulkState = {
  sessionId: null,
  custCode: "",
  custName: "",
  poFiles: [],
  excelFile: null,
  excelItems: [],
  extractionResults: [],
  matchData: null,
};

function openBulkTrainingModal() {
  // 상태 초기화
  Object.assign(_bulkState, {
    sessionId: null, custCode: "", custName: "",
    poFiles: [], excelFile: null, excelItems: [],
    extractionResults: [], matchData: null,
  });
  document.getElementById("bulk-cust-search").value = "";
  document.getElementById("bulk-po-file-list").innerHTML = "";
  document.getElementById("bulk-excel-info").innerHTML = "";
  document.getElementById("bulk-start-btn").disabled = true;
  // 모든 step 숨기고 step1만 표시
  document.querySelectorAll(".bulk-step").forEach(s => s.style.display = "none");
  document.getElementById("bulk-step-1").style.display = "block";
  document.getElementById("bulk-step-indicator").textContent = "1 / 4";
  document.getElementById("bulk-training-modal").style.display = "block";
}

function closeBulkTrainingModal() {
  document.getElementById("bulk-training-modal").style.display = "none";
}

function _checkBulkReady() {
  const ready = _bulkState.custCode && _bulkState.poFiles.length > 0 && _bulkState.excelFile;
  document.getElementById("bulk-start-btn").disabled = !ready;
}

// 거래처 검색 (기존 로직 재사용)
let _bulkCustTimer = null;
function onBulkCustSearch(q) {
  clearTimeout(_bulkCustTimer);
  if (!q || q.length < 1) {
    document.getElementById("bulk-cust-dropdown").style.display = "none";
    return;
  }
  _bulkCustTimer = setTimeout(async () => {
    try {
      const data = await api.get(`/api/customers/?q=${encodeURIComponent(q)}`);
      const list = data.customers || [];
      const dd = document.getElementById("bulk-cust-dropdown");
      const inner = dd.querySelector("div");
      if (!list.length) { dd.style.display = "none"; return; }
      inner.innerHTML = list.map(c =>
        `<div style="padding:8px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid #f3f4f6"
              onmouseover="this.style.background='#f3f4f6'" onmouseout="this.style.background=''"
              onclick="selectBulkCust('${c.cust_code}','${escapeHtml(c.cust_name)}')">
          <strong>${escapeHtml(c.cust_name)}</strong> <span style="color:#9ca3af">(${c.cust_code})</span>
        </div>`
      ).join("");
      dd.style.display = "block";
    } catch(e) { console.error(e); }
  }, 300);
}

function selectBulkCust(code, name) {
  _bulkState.custCode = code;
  _bulkState.custName = name;
  document.getElementById("bulk-cust-search").value = `${name} (${code})`;
  document.getElementById("bulk-cust-dropdown").style.display = "none";
  _checkBulkReady();
}

// 발주서 이미지 파일 선택
function onBulkPoFiles(files) {
  const arr = Array.from(files).filter(f => f.type.startsWith("image/"));
  _bulkState.poFiles = arr;
  document.getElementById("bulk-po-file-list").innerHTML = arr.length
    ? `<strong>${arr.length}개 파일 선택됨:</strong> ` + arr.map(f => escapeHtml(f.name)).join(", ")
    : "";
  _checkBulkReady();
}

function onBulkPoDrop(e) {
  e.preventDefault();
  e.currentTarget.style.borderColor = "#d1d5db";
  if (e.dataTransfer.files.length) onBulkPoFiles(e.dataTransfer.files);
}

// 엑셀 파일 선택
function onBulkExcelFile(file) {
  if (!file) return;
  _bulkState.excelFile = file;
  document.getElementById("bulk-excel-info").innerHTML =
    `<strong>📊 ${escapeHtml(file.name)}</strong> (${(file.size/1024).toFixed(0)}KB)`;
  _checkBulkReady();
}

function onBulkExcelDrop(e) {
  e.preventDefault();
  e.currentTarget.style.borderColor = "#d1d5db";
  if (e.dataTransfer.files.length) onBulkExcelFile(e.dataTransfer.files[0]);
}

// Step 2: AI 추출 시작
async function startBulkExtraction() {
  // Step 1 → Step 2
  document.getElementById("bulk-step-1").style.display = "none";
  document.getElementById("bulk-step-2").style.display = "block";
  document.getElementById("bulk-step-indicator").textContent = "2 / 4";

  const logEl = document.getElementById("bulk-extraction-log");
  const progFill = document.getElementById("bulk-progress-fill");
  const progText = document.getElementById("bulk-progress-text");
  logEl.innerHTML = "";

  const addLog = (msg, color = "#374151") => {
    logEl.innerHTML += `<div style="color:${color};margin-bottom:4px">${msg}</div>`;
    logEl.scrollTop = logEl.scrollHeight;
  };

  // 1) 세션 생성 + 엑셀 파싱
  addLog("📊 엑셀 파싱 중...");
  try {
    const fd = new FormData();
    fd.append("file", _bulkState.excelFile);
    fd.append("cust_code", _bulkState.custCode);
    fd.append("cust_name", _bulkState.custName);
    const sessionResult = await api.bulkCreateSession(fd);
    _bulkState.sessionId = sessionResult.session_id;
    _bulkState.excelItems = sessionResult.excel_items || [];
    addLog(`✅ 엑셀 파싱 완료: ${sessionResult.total_items}건`, "#059669");
  } catch (e) {
    addLog(`❌ 엑셀 파싱 실패: ${e.message}`, "#dc2626");
    return;
  }

  // 2) 발주서 이미지 순차 추출
  const total = _bulkState.poFiles.length;
  _bulkState.extractionResults = [];

  for (let i = 0; i < total; i++) {
    const file = _bulkState.poFiles[i];
    const pct = Math.round(((i) / total) * 100);
    progFill.style.width = pct + "%";
    progFill.textContent = pct + "%";
    progText.textContent = `${i + 1} / ${total} 처리 중: ${file.name}`;

    addLog(`🔍 [${i+1}/${total}] ${file.name} 추출 중...`);

    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("session_id", _bulkState.sessionId);
      const result = await api.bulkExtractPo(fd);
      _bulkState.extractionResults.push(result);

      if (result.status === "success") {
        addLog(`  ✅ 날짜: ${result.order_date || "?"}, ${result.items?.length || 0}건 추출`, "#059669");
      } else {
        addLog(`  ⚠️ 추출 실패 (건너뜀)`, "#d97706");
      }
    } catch (e) {
      addLog(`  ❌ 오류: ${e.message}`, "#dc2626");
    }
  }

  progFill.style.width = "100%";
  progFill.textContent = "100%";
  progText.textContent = `추출 완료! 매칭 분석 중...`;

  // 3) 매칭 제안 요청
  addLog("🔗 매칭 분석 중...");
  try {
    const matchResult = await api.bulkSuggestMatches(_bulkState.sessionId);
    _bulkState.matchData = matchResult;
    addLog(`✅ 매칭 완료: ${matchResult.total_matched}건 자동 매칭됨`, "#059669");

    setTimeout(() => showBulkMatchResults(), 500);
  } catch (e) {
    addLog(`❌ 매칭 분석 실패: ${e.message}`, "#dc2626");
  }
}

// Step 3: 매칭 결과 표시
function showBulkMatchResults() {
  document.getElementById("bulk-step-2").style.display = "none";
  document.getElementById("bulk-step-3").style.display = "block";
  document.getElementById("bulk-step-indicator").textContent = "3 / 4";

  const data = _bulkState.matchData;
  if (!data) return;

  document.getElementById("bulk-match-summary").innerHTML =
    `발주서 ${data.extractions?.length || 0}건 분석 | 자동 매칭 ${data.total_matched}건 | 미매칭 엑셀 ${data.unmatched_excel_count}건`;

  const container = document.getElementById("bulk-match-results");
  let html = "";

  (data.extractions || []).forEach((ext, extIdx) => {
    html += `<div style="border:1px solid #e5e7eb;border-radius:8px;margin-bottom:12px;overflow:hidden">`;
    html += `<div style="background:#f9fafb;padding:10px 14px;font-size:13px;font-weight:600;border-bottom:1px solid #e5e7eb">
      📄 ${escapeHtml(ext.filename)} <span style="color:#6b7280;font-weight:400">날짜: ${ext.order_date || "?"}</span>
    </div>`;

    if (ext.matches && ext.matches.length) {
      html += `<table style="width:100%;font-size:12px;border-collapse:collapse">
        <thead><tr style="background:#f3f4f6">
          <th style="padding:6px 10px;text-align:left;width:30px">✓</th>
          <th style="padding:6px 10px;text-align:left">발주서 품명</th>
          <th style="padding:6px 10px;text-align:left">수량</th>
          <th style="padding:6px 10px;text-align:left">→</th>
          <th style="padding:6px 10px;text-align:left">엑셀 품명</th>
          <th style="padding:6px 10px;text-align:left">품목코드</th>
          <th style="padding:6px 10px;text-align:left">신뢰도</th>
        </tr></thead><tbody>`;

      ext.matches.forEach((m, mIdx) => {
        const conf = m.confidence || 0;
        const confColor = conf >= 90 ? "#059669" : conf >= 70 ? "#d97706" : "#dc2626";
        const checked = conf >= 70 ? "checked" : "";
        const exItem = m.excel_item;
        const poItem = m.po_item || {};

        html += `<tr style="border-bottom:1px solid #f3f4f6" data-ext-idx="${extIdx}" data-match-idx="${mIdx}">
          <td style="padding:6px 10px"><input type="checkbox" ${checked} class="bulk-match-check" data-ext="${extIdx}" data-match="${mIdx}"></td>
          <td style="padding:6px 10px">${escapeHtml(poItem.product_hint || "")}</td>
          <td style="padding:6px 10px">${poItem.qty || ""} ${poItem.unit || ""}</td>
          <td style="padding:6px 10px;color:#9ca3af">→</td>
          <td style="padding:6px 10px">${exItem ? escapeHtml(exItem.product_name || exItem.model_name || "") : '<span style="color:#dc2626">미매칭</span>'}</td>
          <td style="padding:6px 10px;font-family:monospace">${exItem ? escapeHtml(exItem.item_code || "") : ""}</td>
          <td style="padding:6px 10px;font-weight:600;color:${confColor}">${conf}%</td>
        </tr>`;
      });

      html += `</tbody></table>`;
    } else {
      html += `<div style="padding:12px;font-size:12px;color:#9ca3af">추출된 항목이 없습니다.</div>`;
    }

    html += `</div>`;
  });

  container.innerHTML = html;
}

// Step 4: 저장
async function saveBulkMatches() {
  const data = _bulkState.matchData;
  if (!data) return;

  const confirmations = [];

  (data.extractions || []).forEach((ext, extIdx) => {
    const matches = [];
    (ext.matches || []).forEach((m, mIdx) => {
      const cb = document.querySelector(`.bulk-match-check[data-ext="${extIdx}"][data-match="${mIdx}"]`);
      if (cb && cb.checked && m.excel_item && m.excel_item.item_code) {
        matches.push({
          po_item: m.po_item,
          excel_item: m.excel_item,
        });
      }
    });

    if (matches.length > 0) {
      confirmations.push({
        extraction_id: ext.extraction_id,
        matches: matches,
      });
    }
  });

  if (!confirmations.length) {
    toast("저장할 매칭이 없습니다.", "warning");
    return;
  }

  document.getElementById("bulk-save-btn").disabled = true;
  document.getElementById("bulk-save-btn").textContent = "저장 중...";

  try {
    const result = await api.bulkConfirm({
      session_id: _bulkState.sessionId,
      confirmations: confirmations,
    });

    // Step 4: 완료
    document.getElementById("bulk-step-3").style.display = "none";
    document.getElementById("bulk-step-4").style.display = "block";
    document.getElementById("bulk-step-indicator").textContent = "4 / 4";
    document.getElementById("bulk-result-summary").innerHTML =
      `<strong>${result.saved_pairs}건</strong>의 학습 데이터가 저장되었습니다.<br>총 <strong>${result.saved_items}개</strong> 품목이 학습되었습니다.`;

    toast(`대량 학습 완료: ${result.saved_pairs}건 저장`, "success");
  } catch (e) {
    toast("저장 실패: " + e.message, "error");
    document.getElementById("bulk-save-btn").disabled = false;
    document.getElementById("bulk-save-btn").textContent = "✅ 확인된 매칭 저장";
  }
}


/* =============================================
   오더리스트 (해외 발주 현황)
   ============================================= */
let olCurrentPage = 1;
let _olAutoTimer = null;
let _olAutoIdx = -1;
let _olAutoResults = [];

async function syncOrderList() {
  const btn = document.getElementById("ol-sync-btn");
  btn.disabled = true;
  btn.textContent = "동기화 중...";
  try {
    const data = await api.orderlistSync();
    if (data.success) {
      toast(`오더리스트 동기화 완료: ${data.total_items}건`, "success");
      loadOrderListTabs();
      loadOrderList();
    } else {
      toast(`동기화 실패: ${data.error || ""}`, "error");
    }
  } catch (e) {
    toast(`동기화 오류: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = "🔄 동기화";
  }
}

async function loadOrderListTabs() {
  try {
    const tabs = await api.orderlistTabs();
    const sel = document.getElementById("ol-tab-select");
    sel.innerHTML = '<option value="">전체</option>';
    tabs.forEach(t => {
      sel.innerHTML += `<option value="${t.sheet_tab}">${t.sheet_tab} (${t.item_count}건)</option>`;
    });
  } catch (e) { /* ignore */ }
}

// ─── 오더리스트 자동완성 ───
async function onOlSearchInput(query) {
  const dd = document.getElementById("ol-autocomplete-dropdown");
  _olAutoIdx = -1;

  if (!query || query.trim().length < 1) {
    dd.style.display = "none";
    _olAutoResults = [];
    return;
  }

  clearTimeout(_olAutoTimer);
  _olAutoTimer = setTimeout(async () => {
    try {
      const res = await api.orderlistAutocomplete(query.trim(), 15);
      _olAutoResults = res.results || [];

      if (!_olAutoResults.length) {
        dd.innerHTML = `<div style="padding:12px 14px;color:#a0aec0;font-size:13px">검색 결과 없음</div>`;
        dd.style.display = "block";
        return;
      }

      dd.innerHTML = _olAutoResults.map((p, i) => `
        <div class="ol-ac-item" data-idx="${i}"
          onclick="selectOlItem(${i})"
          onmouseover="highlightOlItem(${i})"
          style="padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid #f7fafc;
                 display:flex;align-items:center;gap:10px;transition:background .1s">
          <span style="flex-shrink:0;background:var(--primary);color:#fff;border-radius:4px;padding:1px 5px;font-size:10px">
            ${escapeHtml(p.sheet_tab || '')}
          </span>
          <span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            <strong style="color:var(--primary)">${escapeHtml(p.model_name || '')}</strong>
            ${p.description ? `<span style="color:var(--gray-500);font-size:11px;margin-left:4px">${escapeHtml(p.description.substring(0, 40))}</span>` : ''}
          </span>
          <span style="font-size:11px;color:var(--gray-400);flex-shrink:0">
            ${p.qty ? p.qty.toLocaleString() + (p.unit || 'PCS') : ''}
          </span>
        </div>
      `).join("");
      dd.style.display = "block";
    } catch (e) {
      console.warn("오더리스트 자동완성 오류:", e);
    }
  }, 180);
}

function highlightOlItem(idx) {
  _olAutoIdx = idx;
  document.querySelectorAll(".ol-ac-item").forEach((el, i) => {
    el.style.background = i === idx ? "#ebf8ff" : "";
  });
}

function selectOlItem(idx) {
  const p = _olAutoResults[idx];
  if (!p) return;
  const input = document.getElementById("ol-search-input");
  input.value = p.model_name || p.category || p.order_no || "";
  document.getElementById("ol-autocomplete-dropdown").style.display = "none";
  _olAutoResults = [];
  _olAutoIdx = -1;
  loadOrderList(1);
}

function onOlSearchKeydown(event) {
  const dd = document.getElementById("ol-autocomplete-dropdown");
  const items = _olAutoResults;

  if (dd.style.display === "none" || !items.length) {
    if (event.key === "Enter") { event.preventDefault(); loadOrderList(1); }
    return;
  }

  if (event.key === "ArrowDown") {
    event.preventDefault();
    _olAutoIdx = Math.min(_olAutoIdx + 1, items.length - 1);
    highlightOlItem(_olAutoIdx);
    const el = dd.querySelector(`[data-idx="${_olAutoIdx}"]`);
    if (el) el.scrollIntoView({ block: "nearest" });
  } else if (event.key === "ArrowUp") {
    event.preventDefault();
    _olAutoIdx = Math.max(_olAutoIdx - 1, 0);
    highlightOlItem(_olAutoIdx);
    const el = dd.querySelector(`[data-idx="${_olAutoIdx}"]`);
    if (el) el.scrollIntoView({ block: "nearest" });
  } else if (event.key === "Enter") {
    event.preventDefault();
    if (_olAutoIdx >= 0 && _olAutoIdx < items.length) {
      selectOlItem(_olAutoIdx);
    } else {
      dd.style.display = "none";
      loadOrderList(1);
    }
  } else if (event.key === "Escape") {
    dd.style.display = "none";
    _olAutoIdx = -1;
  }
}

// 드롭다운 외부 클릭 시 닫기
document.addEventListener("click", (e) => {
  const dd = document.getElementById("ol-autocomplete-dropdown");
  const input = document.getElementById("ol-search-input");
  if (dd && input && !dd.contains(e.target) && e.target !== input) {
    dd.style.display = "none";
  }
});

async function loadOrderList(page = 1) {
  olCurrentPage = page;
  const tab = document.getElementById("ol-tab-select").value;
  const query = document.getElementById("ol-search-input").value.trim();
  const container = document.getElementById("ol-results");
  const pagination = document.getElementById("ol-pagination");
  const summary = document.getElementById("ol-summary");

  container.innerHTML = '<p style="text-align:center;padding:20px;color:var(--gray-400)">로딩 중...</p>';

  try {
    const data = await api.orderlistData(query, tab, page, 50);

    summary.textContent = `총 ${data.total}건 / ${data.total_pages}페이지`;

    if (!data.items || data.items.length === 0) {
      container.innerHTML = '<p style="text-align:center;padding:30px;color:var(--gray-400)">데이터가 없습니다. 동기화를 먼저 실행해주세요.</p>';
      pagination.innerHTML = "";
      return;
    }

    // 테이블 렌더링
    let html = `<table class="result-table" style="width:100%;font-size:12px">
      <thead><tr>
        <th style="width:60px">탭</th>
        <th style="width:100px">주문번호</th>
        <th>카테고리</th>
        <th style="min-width:180px">모델명</th>
        <th>설명</th>
        <th style="width:60px;text-align:right">수량</th>
        <th style="width:50px">단위</th>
        <th style="width:120px">주문일</th>
      </tr></thead><tbody>`;

    data.items.forEach(item => {
      const orderNoBadge = `<span style="background:#edf2f7;border-radius:4px;padding:1px 5px;font-size:11px;font-weight:500">${item.order_no || "-"}</span>`;
      const dateCell = (item.order_date || "").substring(0, 20);

      html += `<tr>
        <td><span style="background:var(--primary);color:#fff;border-radius:4px;padding:1px 5px;font-size:10px">${item.sheet_tab}</span></td>
        <td>${orderNoBadge}</td>
        <td style="color:var(--gray-600);font-size:11px">${item.category || ""}</td>
        <td><strong style="color:var(--primary)">${item.model_name || ""}</strong></td>
        <td style="font-size:11px;color:var(--gray-600)">${item.description || ""}</td>
        <td style="text-align:right;font-weight:600">${item.qty ? item.qty.toLocaleString() : ""}</td>
        <td style="font-size:11px">${item.unit || ""}</td>
        <td style="font-size:11px">${dateCell}</td>
      </tr>`;
    });

    html += "</tbody></table>";
    container.innerHTML = html;

    // 페이지네이션
    if (data.total_pages > 1) {
      let pgHtml = "";
      const start = Math.max(1, page - 3);
      const end = Math.min(data.total_pages, page + 3);
      if (page > 1) pgHtml += `<button class="btn btn-outline btn-sm" onclick="loadOrderList(${page-1})">‹</button> `;
      for (let i = start; i <= end; i++) {
        pgHtml += `<button class="btn ${i === page ? 'btn-primary' : 'btn-outline'} btn-sm" onclick="loadOrderList(${i})" style="min-width:32px">${i}</button> `;
      }
      if (page < data.total_pages) pgHtml += `<button class="btn btn-outline btn-sm" onclick="loadOrderList(${page+1})">›</button>`;
      pagination.innerHTML = pgHtml;
    } else {
      pagination.innerHTML = "";
    }

  } catch (e) {
    container.innerHTML = `<p style="color:red;text-align:center;padding:20px">오류: ${e.message}</p>`;
  }
}

// 오더리스트 페이지 진입 시 자동 로드
(function() {
  const olNav = document.querySelector('[data-page="orderlist"]');
  if (olNav) {
    olNav.addEventListener("click", () => {
      loadOrderListTabs();
      loadOrderList();
    });
  }
})();

// ═══════════════════════════════════════════════
//  택배조회 기능
// ═══════════════════════════════════════════════

function initShippingPage() {
  // 오늘 날짜를 기본값으로 설정
  const today = new Date().toISOString().slice(0, 10);
  const dailyDate = document.getElementById("ship-daily-date");
  if (dailyDate && !dailyDate.value) dailyDate.value = today;

  // 통계 로드
  loadShippingStats();
  loadSchedulerStatus();
}

async function loadShippingStats() {
  try {
    const data = await api.shippingStats();
    const badge = document.getElementById("ship-stats-badge");
    if (badge) {
      const whParts = (data.by_warehouse || []).map(w => `${w.warehouse} ${w.cnt}건`).join(" / ");
      badge.textContent = `총 ${data.total || 0}건` + (whParts ? ` (${whParts})` : "");
    }
  } catch (e) { console.error("택배 통계 오류:", e); }
}

function switchShipTab(tabId) {
  document.querySelectorAll(".ship-tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".ship-tab-panel").forEach(p => p.classList.remove("active"));
  const btn = document.querySelector(`[data-ship-tab="${tabId}"]`);
  if (btn) btn.classList.add("active");
  const panel = document.getElementById("ship-panel-" + tabId);
  if (panel) panel.classList.add("active");
}

// ── 받는사람 검색 ──
async function searchShipments(page = 1) {
  const q = document.getElementById("ship-search-name").value.trim();
  const dateEl = document.getElementById("ship-search-date");
  const date = dateEl ? dateEl.value.replace(/-/g, "") : "";
  const wh = document.getElementById("ship-search-wh").value;

  if (!q && !date) {
    alert("받는사람 이름 또는 날짜를 입력하세요.");
    return;
  }

  const container = document.getElementById("ship-search-result");
  container.innerHTML = '<p style="color:#6b7280">검색 중...</p>';

  try {
    const data = await api.shippingSearch(q, date, wh, page, 50);
    container.innerHTML = renderShipmentTable(data.items, data.total, data.page, data.total_pages, "searchShipments");
  } catch (e) {
    container.innerHTML = `<p style="color:#dc2626">검색 오류: ${e.message}</p>`;
  }
}

// ── 일별 조회 ──
async function loadDailyShipments() {
  const dateEl = document.getElementById("ship-daily-date");
  const date = dateEl ? dateEl.value : "";
  const wh = document.getElementById("ship-daily-wh").value;

  if (!date) { alert("날짜를 선택하세요."); return; }

  const sumEl = document.getElementById("ship-daily-summary");
  const resEl = document.getElementById("ship-daily-result");
  resEl.innerHTML = '<p style="color:#6b7280">조회 중...</p>';

  try {
    const data = await api.shippingDaily(date.replace(/-/g, ""), wh);
    // 요약
    const sumParts = (data.summary || []).map(s =>
      `<span style="display:inline-block;padding:4px 12px;background:${s.warehouse === '용산' ? '#dbeafe' : '#dcfce7'};color:${s.warehouse === '용산' ? '#1d4ed8' : '#166534'};border-radius:16px;font-size:13px;font-weight:600;margin-right:8px">${s.warehouse} ${s.cnt}건</span>`
    );
    sumEl.innerHTML = `<div style="margin-bottom:8px">
      <span style="font-weight:700;font-size:15px">📅 ${formatTakeDt(data.date)} 발송내역</span>
      <span style="margin-left:12px;font-size:14px;color:#374151">총 <b>${data.total}</b>건</span>
      <span style="margin-left:8px">${sumParts.join("")}</span>
    </div>`;
    resEl.innerHTML = renderShipmentTable(data.items, data.total);
  } catch (e) {
    resEl.innerHTML = `<p style="color:#dc2626">조회 오류: ${e.message}</p>`;
  }
}

// ── 운송장 추적 (로젠 공식 배송조회 페이지) ──
async function trackShipment() {
  const input = document.getElementById("ship-track-no").value.trim();
  if (!input) { alert("운송장번호를 입력하세요."); return; }

  // 숫자만 추출
  const slipNo = input.replace(/\D/g, "");
  if (slipNo.length < 10) { alert("유효한 운송장번호를 입력하세요."); return; }

  const container = document.getElementById("ship-track-result");

  // DB에서 간단 정보 조회 (비동기, 빠르게)
  let dbInfoHtml = "";
  try {
    const searchData = await api.get(`/api/shipping/search?q=&page=1&page_size=1&warehouse=`);
    // slip_no로 직접 DB 검색
    const resp = await fetch(`/api/shipping/track-info?slip_no=${slipNo}`, {
      headers: { "Authorization": `Bearer ${api._token || localStorage.getItem("token")}` }
    });
    if (resp.ok) {
      const info = await resp.json();
      if (info && info.rcv_name) {
        const whColor = info.warehouse === "용산" ? "#1d4ed8" : "#166534";
        const whBg = info.warehouse === "용산" ? "#dbeafe" : "#dcfce7";
        dbInfoHtml = `<div style="padding:10px 14px;background:#f0f9ff;border:1px solid #dbeafe;border-radius:8px;margin-bottom:12px;font-size:13px">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
            <span style="font-weight:700;font-size:15px">📦 ${slipNo}</span>
            <span style="display:inline-block;padding:2px 8px;background:${whBg};color:${whColor};border-radius:10px;font-size:11px;font-weight:600">${info.warehouse || ""}</span>
            <span style="color:${getStatusColor(info.status)};font-weight:600">${info.status || ""}</span>
          </div>
          <div><b>받는분:</b> ${info.rcv_name} &nbsp;|&nbsp; <b>물품:</b> ${info.goods_nm || "-"} &nbsp;|&nbsp; <b>접수일:</b> ${formatTakeDt(info.take_dt)}</div>
          <div style="color:#6b7280">${info.rcv_addr1 || ""}</div>
        </div>`;
      }
    }
  } catch (e) { /* DB 조회 실패해도 iframe은 표시 */ }

  // 로젠 공식 배송조회 iframe + 새창 열기 버튼
  const logenUrl = `https://www.ilogen.com/m/personal/trace/${slipNo}`;
  container.innerHTML = `
    ${dbInfoHtml}
    <div class="card" style="padding:0;overflow:hidden">
      <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#f9fafb;border-bottom:1px solid #e5e7eb">
        <span style="font-size:13px;font-weight:600;color:#374151">🚚 로젠택배 실시간 배송조회</span>
        <button onclick="window.open('${logenUrl}', 'logenTrack', 'width=600,height=700')"
          style="padding:4px 12px;background:#2563eb;color:#fff;border:none;border-radius:4px;font-size:12px;cursor:pointer">
          새창으로 열기 ↗
        </button>
      </div>
      <iframe src="${logenUrl}" style="width:100%;height:520px;border:none" loading="lazy"></iframe>
    </div>`;
}

// 운송장 추적 (팝업 방식 - 일별조회 테이블에서 사용)
function openTrackPopup(slipNo) {
  const clean = slipNo.replace(/\D/g, "");
  window.open(`https://www.ilogen.com/m/personal/trace/${clean}`, "logenTrack", "width=600,height=700");
}

// ── SmartLogen 자동 가져오기 ──
async function autoFetchShipments() {
  const btn = document.getElementById("ship-autofetch-btn");
  const resultDiv = document.getElementById("ship-autofetch-result");
  const warehouse = document.getElementById("ship-autofetch-wh").value;
  const days = parseInt(document.getElementById("ship-autofetch-days").value) || 7;

  btn.disabled = true;
  btn.textContent = "⏳ 가져오는 중...";
  resultDiv.innerHTML = '<div style="color:#6b7280;font-size:13px">SmartLogen에 로그인하여 데이터를 가져오고 있습니다... (최대 30초 소요)</div>';

  try {
    const data = await api.shippingAutoFetch(warehouse, "", "", days);
    if (data.success) {
      const color = data.fetched > 0 ? "#059669" : "#6b7280";
      resultDiv.innerHTML = `<div style="color:${color};font-size:14px;font-weight:600;padding:10px;background:#f0fdf4;border-radius:6px">
        ✅ ${data.message || `${data.fetched}건 조회, ${data.saved}건 저장`}
      </div>`;
      // 검색 탭으로 전환해서 결과 확인
      if (data.fetched > 0) {
        setTimeout(() => {
          switchShipTab("search");
          searchShipments();
        }, 1500);
      }
    } else {
      resultDiv.innerHTML = `<div style="color:#dc2626;font-size:13px;padding:10px;background:#fef2f2;border-radius:6px">❌ ${data.message || "가져오기 실패"}</div>`;
    }
  } catch (e) {
    resultDiv.innerHTML = `<div style="color:#dc2626;font-size:13px;padding:10px;background:#fef2f2;border-radius:6px">❌ 오류: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "🚀 자동 가져오기";
  }
}

// ── 스케줄러 상태 조회 ──
async function loadSchedulerStatus() {
  try {
    const data = await api.get("/api/shipping/scheduler/status");
    const badge = document.getElementById("scheduler-status-badge");
    const info = document.getElementById("scheduler-info");
    if (!badge || !info) return;

    badge.textContent = data.enabled ? "활성" : "비활성";
    badge.style.background = data.enabled ? "#dcfce7" : "#fef2f2";
    badge.style.color = data.enabled ? "#166534" : "#991b1b";

    let html = "";
    if (data.last_run) {
      const dt = new Date(data.last_run);
      html += `마지막 실행: <b>${dt.toLocaleString("ko-KR")}</b>`;
      if (data.last_result) {
        const r = data.last_result;
        html += r.success
          ? ` (${r.fetched}건 조회, ${r.saved}건 저장)`
          : ` (오류: ${r.error || "실패"})`;
      }
      html += " &nbsp;|&nbsp; ";
    }
    if (data.next_run) {
      const ndt = new Date(data.next_run);
      html += `다음 실행: <b>${ndt.toLocaleString("ko-KR")}</b>`;
    }
    info.innerHTML = html || "아직 실행 기록이 없습니다.";
  } catch (e) {
    // 조용히 무시
  }
}

// ── 대량 운송장 동기화 ──
async function syncShipments() {
  const input = document.getElementById("ship-sync-input").value.trim();
  if (!input) { alert("운송장번호를 입력하세요."); return; }

  // 줄바꿈, 쉼표, 공백으로 분리
  const slipNos = input.split(/[\n,\s]+/).map(s => s.trim()).filter(Boolean);
  if (slipNos.length === 0) { alert("유효한 운송장번호가 없습니다."); return; }

  const btn = document.getElementById("ship-sync-btn");
  const resultEl = document.getElementById("ship-sync-result");
  btn.disabled = true;
  btn.textContent = "⏳ 동기화 중...";
  resultEl.innerHTML = `<p style="color:#6b7280">총 ${slipNos.length}건 동기화 중... (50건씩 처리)</p>`;

  try {
    const data = await api.shippingSync(slipNos);
    let html = `<div style="padding:12px;background:#ecfdf5;border-radius:8px;border:1px solid #a7f3d0">
      <div style="font-weight:700;color:#065f46;margin-bottom:6px">✅ 동기화 완료</div>
      <div style="font-size:13px;color:#064e3b">
        요청: <b>${data.total_requested}</b>건 &nbsp;|&nbsp;
        추적 성공: <b>${data.total_tracked}</b>건 &nbsp;|&nbsp;
        DB 저장: <b>${data.total_saved}</b>건
      </div>`;
    if (data.errors && data.errors.length > 0) {
      html += `<div style="margin-top:6px;font-size:12px;color:#dc2626">오류: ${data.errors.join(", ")}</div>`;
    }
    html += `<div style="margin-top:8px;font-size:12px;color:#6b7280">
      💡 이제 "받는사람 검색" 및 "일별 조회" 탭에서 저장된 데이터를 검색할 수 있습니다.
    </div></div>`;
    resultEl.innerHTML = html;
    loadShippingStats();
  } catch (e) {
    resultEl.innerHTML = `<p style="color:#dc2626">동기화 오류: ${e.message}</p>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "🔄 동기화 시작";
  }
}

// ── 엑셀에서 운송장번호 추출 후 동기화 ──
async function syncFromExcel() {
  const fileInput = document.getElementById("ship-sync-file");
  if (!fileInput.files.length) { alert("파일을 선택하세요."); return; }

  const resultEl = document.getElementById("ship-sync-file-result");
  resultEl.innerHTML = '<p style="color:#6b7280">파일 읽는 중...</p>';

  const file = fileInput.files[0];
  const ext = file.name.split('.').pop().toLowerCase();

  try {
    if (ext === 'txt' || ext === 'csv') {
      // 텍스트/CSV: 직접 읽어서 운송장번호 추출
      const text = await file.text();
      const slipNos = text.split(/[\n,\s]+/).map(s => s.trim()).filter(s => /^\d{10,15}$/.test(s));
      if (slipNos.length === 0) {
        resultEl.innerHTML = '<p style="color:#dc2626">유효한 운송장번호를 찾지 못했습니다 (10~15자리 숫자).</p>';
        return;
      }
      document.getElementById("ship-sync-input").value = slipNos.join("\n");
      resultEl.innerHTML = `<p style="color:#059669">${slipNos.length}건의 운송장번호를 추출했습니다. 위 "동기화 시작" 버튼을 눌러주세요.</p>`;
    } else {
      // 엑셀: 서버에 업로드해서 운송장번호 컬럼 추출 후 동기화
      const formData = new FormData();
      formData.append("file", file);
      resultEl.innerHTML = '<p style="color:#6b7280">엑셀 파일 처리 및 동기화 중...</p>';
      const data = await api.postForm("/api/shipping/sync-excel", formData);
      if (data.success) {
        let html = `<div style="padding:12px;background:#ecfdf5;border-radius:8px;border:1px solid #a7f3d0">
          <div style="font-weight:700;color:#065f46;margin-bottom:6px">✅ 엑셀 동기화 완료</div>
          <div style="font-size:13px;color:#064e3b">
            추출: <b>${data.total_extracted}</b>건 &nbsp;|&nbsp;
            추적 성공: <b>${data.total_tracked}</b>건 &nbsp;|&nbsp;
            DB 저장: <b>${data.total_saved}</b>건
          </div></div>`;
        resultEl.innerHTML = html;
        loadShippingStats();
      } else {
        resultEl.innerHTML = `<p style="color:#dc2626">오류: ${data.message}</p>`;
      }
    }
  } catch (e) {
    resultEl.innerHTML = `<p style="color:#dc2626">파일 처리 오류: ${e.message}</p>`;
  }
}

// textarea 운송장번호 개수 실시간 표시
document.addEventListener("DOMContentLoaded", () => {
  const syncInput = document.getElementById("ship-sync-input");
  if (syncInput) {
    syncInput.addEventListener("input", () => {
      const count = syncInput.value.split(/[\n,\s]+/).filter(s => s.trim()).length;
      const el = document.getElementById("ship-sync-count");
      if (el) el.textContent = count > 0 ? `${count}건 입력됨` : "";
    });
  }
});

// ── 수동 등록 ──
async function registerShipment() {
  const slipNo = document.getElementById("ship-reg-slip").value.trim();
  const rcvName = document.getElementById("ship-reg-rcv").value.trim();
  if (!slipNo || !rcvName) { alert("운송장번호와 받는사람은 필수입니다."); return; }

  const dateEl = document.getElementById("ship-reg-date");
  const takeDt = dateEl && dateEl.value ? dateEl.value.replace(/-/g, "") : new Date().toISOString().slice(0, 10).replace(/-/g, "");

  try {
    const data = await api.shippingRegister({
      warehouse: document.getElementById("ship-reg-wh").value,
      slip_no: slipNo,
      rcv_name: rcvName,
      rcv_tel: document.getElementById("ship-reg-rcvtel").value.trim(),
      rcv_addr1: document.getElementById("ship-reg-rcvaddr").value.trim(),
      goods_nm: document.getElementById("ship-reg-goods").value.trim(),
      take_dt: takeDt,
      memo: document.getElementById("ship-reg-memo").value.trim(),
    });
    const resEl = document.getElementById("ship-reg-result");
    if (data.success) {
      resEl.innerHTML = '<p style="color:#059669;font-weight:600">✅ 등록 완료</p>';
      document.getElementById("ship-reg-slip").value = "";
      document.getElementById("ship-reg-rcv").value = "";
      loadShippingStats();
    } else {
      resEl.innerHTML = `<p style="color:#dc2626">❌ ${data.message}</p>`;
    }
  } catch (e) {
    document.getElementById("ship-reg-result").innerHTML = `<p style="color:#dc2626">오류: ${e.message}</p>`;
  }
}

// ── 엑셀 업로드 ──
async function uploadShipExcel() {
  const fileInput = document.getElementById("ship-upload-file");
  if (!fileInput.files.length) { alert("파일을 선택하세요."); return; }

  const wh = document.getElementById("ship-upload-wh").value;
  const formData = new FormData();
  formData.append("file", fileInput.files[0]);
  formData.append("warehouse", wh);

  const resEl = document.getElementById("ship-upload-result");
  resEl.innerHTML = '<p style="color:#6b7280">업로드 중...</p>';

  try {
    // postForm with query param
    const resp = await fetch(`/api/shipping/upload-excel?warehouse=${encodeURIComponent(wh)}`, {
      method: "POST",
      headers: { "Authorization": `Bearer ${api.getToken()}` },
      body: formData,
    });
    const data = await resp.json();
    if (data.success) {
      resEl.innerHTML = `<p style="color:#059669;font-weight:600">✅ ${data.inserted}건 등록 완료 (스킵: ${data.skipped || 0}건)</p>
        <p style="font-size:12px;color:#6b7280">인식 컬럼: ${(data.columns_found || []).join(", ")}</p>`;
      loadShippingStats();
    } else {
      resEl.innerHTML = `<p style="color:#dc2626">❌ ${data.message}</p>`;
    }
  } catch (e) {
    resEl.innerHTML = `<p style="color:#dc2626">업로드 오류: ${e.message}</p>`;
  }
}

// ── 유틸 함수 ──
function renderShipmentTable(items, total, page, totalPages, paginationFn) {
  if (!items || items.length === 0) {
    return '<p style="color:#6b7280;text-align:center;padding:20px">조회 결과가 없습니다.</p>';
  }
  let html = `<div class="card" style="overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-size:13px;min-width:700px">
      <thead><tr style="background:#f9fafb;border-bottom:2px solid #e5e7eb">
        <th style="padding:8px 10px;text-align:left">창고</th>
        <th style="padding:8px 10px;text-align:left">운송장번호</th>
        <th style="padding:8px 10px;text-align:left">받는사람</th>
        <th style="padding:8px 10px;text-align:left">연락처</th>
        <th style="padding:8px 10px;text-align:left">주소</th>
        <th style="padding:8px 10px;text-align:left">물품명</th>
        <th style="padding:8px 10px;text-align:left">접수일</th>
        <th style="padding:8px 10px;text-align:center"><span title="집하 전에는 추적이 되지 않습니다">추적</span></th>
      </tr></thead><tbody>`;

  html += `<tr><td colspan="8" style="padding:4px 10px;font-size:11px;color:#9ca3af;background:#fefce8;border-bottom:1px solid #fde68a">
    <span style="color:#f59e0b">ℹ️</span> 집하 전에는 추적이 되지 않습니다
  </td></tr>`;

  for (const s of items) {
    const whColor = s.warehouse === "용산" ? "#1d4ed8" : "#166534";
    const whBg = s.warehouse === "용산" ? "#dbeafe" : "#dcfce7";
    html += `<tr style="border-bottom:1px solid #f3f4f6">
      <td style="padding:6px 10px"><span style="display:inline-block;padding:2px 8px;background:${whBg};color:${whColor};border-radius:10px;font-size:11px;font-weight:600">${s.warehouse || ""}</span></td>
      <td style="padding:6px 10px;font-family:monospace">${s.slip_no || ""}</td>
      <td style="padding:6px 10px;font-weight:600">${s.rcv_name || ""}</td>
      <td style="padding:6px 10px">${s.rcv_cell || s.rcv_tel || ""}</td>
      <td style="padding:6px 10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${s.rcv_addr1 || ""}">${s.rcv_addr1 || ""}</td>
      <td style="padding:6px 10px">${s.goods_nm || ""}</td>
      <td style="padding:6px 10px">${formatTakeDt(s.take_dt)}</td>
      <td style="padding:6px 10px;text-align:center">
        <button onclick="quickTrack('${s.slip_no}')" style="padding:2px 8px;background:#f3f4f6;border:1px solid #d1d5db;border-radius:4px;font-size:11px;cursor:pointer" title="배송추적">📦</button>
      </td>
    </tr>`;
  }
  html += '</tbody></table>';

  // 페이지네이션
  if (totalPages && totalPages > 1 && paginationFn) {
    html += `<div style="display:flex;justify-content:center;gap:4px;padding:12px 0">`;
    for (let i = 1; i <= totalPages && i <= 10; i++) {
      const active = i === page ? "background:#2563eb;color:#fff;" : "background:#f3f4f6;";
      html += `<button onclick="${paginationFn}(${i})" style="${active}border:none;border-radius:4px;padding:4px 10px;font-size:12px;cursor:pointer">${i}</button>`;
    }
    html += '</div>';
  }

  html += `<div style="text-align:right;padding:8px 10px;font-size:12px;color:#6b7280">총 ${total}건</div></div>`;
  return html;
}

function quickTrack(slipNo) {
  // 바로 팝업으로 로젠 배송조회 열기 (가장 빠름)
  openTrackPopup(slipNo);
}

function formatTakeDt(dt) {
  if (!dt || dt.length < 8) return dt || "";
  const d = dt.replace(/-/g, "");
  return d.slice(0, 4) + "-" + d.slice(4, 6) + "-" + d.slice(6, 8);
}

function formatScanDt(scanDt, scanTm) {
  if (!scanDt) return "";
  let result = scanDt.slice(0, 4) + "-" + scanDt.slice(4, 6) + "-" + scanDt.slice(6, 8);
  if (scanTm && scanTm.length >= 4) {
    result += " " + scanTm.slice(0, 2) + ":" + scanTm.slice(2, 4);
    if (scanTm.length >= 6) result += ":" + scanTm.slice(4, 6);
  }
  return result;
}

function getStatusColor(statNm) {
  if (!statNm) return "#374151";
  if (statNm.includes("완료")) return "#059669";
  if (statNm.includes("배송출발") || statNm.includes("배송중")) return "#2563eb";
  if (statNm.includes("집하") || statNm.includes("접수")) return "#d97706";
  if (statNm.includes("간선")) return "#7c3aed";
  return "#374151";
}

// ═══════════════════════════════════════════════════
//  CS / RMA 관리
// ═══════════════════════════════════════════════════
let _csStatus = "";
let _csPage = 1;
let _csSearch = "";

async function csInit() {
  _csStatus = "";
  _csPage = 1;
  _csSearch = "";
  const searchInput = document.getElementById("cs-search-input");
  if (searchInput) searchInput.value = "";
  document.querySelectorAll(".cs-pipe-tab").forEach(t => t.classList.remove("active"));
  const allTab = document.querySelector('.cs-pipe-tab[data-status=""]');
  if (allTab) allTab.classList.add("active");
  await csLoadTickets();
  csLoadStats();
}

async function csLoadStats() {
  try {
    const d = await api.get("/api/cs/stats");
    const bar = document.getElementById("cs-stats-bar");
    if (!bar) return;
    const sc = d.status_counts || {};
    const items = [
      { label: "전체", num: d.total || 0, color: "#6b7280" },
      { label: "오늘 접수", num: d.today_count || 0, color: "#2563eb" },
      { label: "접수완료", num: sc["접수완료"] || 0, color: "#d97706" },
      { label: "물류수령", num: sc["물류수령"] || 0, color: "#2563eb" },
      { label: "기술인계", num: sc["기술인계"] || 0, color: "#7c3aed" },
      { label: "테스트완료", num: sc["테스트완료"] || 0, color: "#059669" },
      { label: "처리종결", num: sc["처리종결"] || 0, color: "#374151" },
    ];
    bar.innerHTML = items.map(i =>
      `<div class="cs-stat-card"><div class="num" style="color:${i.color}">${i.num}</div><div class="label">${i.label}</div></div>`
    ).join("");
    // 탭 카운트 업데이트
    document.querySelectorAll(".cs-pipe-tab").forEach(tab => {
      const st = tab.getAttribute("data-status");
      const cnt = st ? (sc[st] || 0) : (d.total || 0);
      let cntEl = tab.querySelector(".cnt");
      if (!cntEl) { cntEl = document.createElement("span"); cntEl.className = "cnt"; tab.appendChild(cntEl); }
      cntEl.textContent = cnt;
    });
  } catch(e) { console.error("CS stats error", e); }
}

async function csLoadTickets() {
  const list = document.getElementById("cs-ticket-list");
  if (!list) return;
  list.innerHTML = '<div style="text-align:center;padding:40px;color:#9ca3af">불러오는 중...</div>';
  try {
    const params = new URLSearchParams({ page: _csPage, size: 50 });
    if (_csStatus) params.set("status", _csStatus);
    if (_csSearch) params.set("search", _csSearch);
    const d = await api.get(`/api/cs/tickets?${params}`);
    const tickets = d.tickets || [];
    if (tickets.length === 0) {
      list.innerHTML = '<div style="text-align:center;padding:60px;color:#9ca3af">' +
        (_csSearch ? '검색 결과가 없습니다.' : '등록된 CS 티켓이 없습니다.<br><br><button class="btn btn-primary" onclick="csShowCreateForm()">+ 새 접수</button>') +
        '</div>';
      return;
    }
    list.innerHTML = tickets.map(t => {
      const testBadge = t.test_status ? `<span class="cs-test-badge cs-test-${t.test_status}">${{"정상":"🟢 정상","의심":"🟡 의심","불량":"🔴 불량"}[t.test_status] || t.test_status}</span>` : "";
      const finalBadge = t.final_action ? `<span style="font-size:11px;color:#6b7280;margin-left:6px">${t.final_action}</span>` : "";
      return `<div class="cs-ticket-card" onclick="csShowDetail('${t.ticket_id}')">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap">
          <div style="flex:1;min-width:200px">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
              <span style="font-weight:600;color:#2563eb;font-size:13px">${t.ticket_id}</span>
              <span class="cs-status-badge cs-status-${t.current_status}">${t.current_status}</span>
              ${testBadge}${finalBadge}
            </div>
            <div style="font-size:14px;font-weight:500;color:#111827;margin-bottom:4px">${_esc(t.customer_name)} · ${_esc(t.contact_info)}</div>
            <div style="font-size:13px;color:#374151">${_esc(t.product_name)}${t.serial_number ? " (S/N: "+_esc(t.serial_number)+")" : ""}</div>
            <div style="font-size:12px;color:#6b7280;margin-top:4px;max-width:500px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(t.defect_symptom)}</div>
          </div>
          <div style="text-align:right;font-size:11px;color:#9ca3af;white-space:nowrap">
            ${t.created_at ? t.created_at.slice(0,16) : ""}
          </div>
        </div>
      </div>`;
    }).join("");
    // 페이지네이션
    const totalPages = Math.ceil((d.total || 0) / (d.size || 50));
    const pgEl = document.getElementById("cs-pagination");
    if (pgEl && totalPages > 1) {
      let pgHtml = "";
      for (let i = 1; i <= totalPages && i <= 10; i++) {
        pgHtml += `<button onclick="_csPage=${i};csLoadTickets()" style="padding:4px 12px;border:1px solid ${i===_csPage?'#2563eb':'#d1d5db'};border-radius:6px;background:${i===_csPage?'#2563eb':'#fff'};color:${i===_csPage?'#fff':'#374151'};cursor:pointer;font-size:13px">${i}</button>`;
      }
      pgEl.innerHTML = pgHtml;
    } else if (pgEl) pgEl.innerHTML = "";
  } catch(e) {
    list.innerHTML = `<div style="text-align:center;padding:40px;color:#ef4444">오류: ${e.message || e}</div>`;
  }
}

function csSwitchTab(el, status) {
  document.querySelectorAll(".cs-pipe-tab").forEach(t => t.classList.remove("active"));
  el.classList.add("active");
  _csStatus = status;
  _csPage = 1;
  csLoadTickets();
}

function csSearch() {
  _csSearch = (document.getElementById("cs-search-input")?.value || "").trim();
  _csPage = 1;
  csLoadTickets();
}

function _esc(s) { if (!s) return ""; const d = document.createElement("div"); d.textContent = s; return d.innerHTML; }

// ── CS 모달 ──
function csCloseModal() {
  document.getElementById("cs-modal-overlay").style.display = "none";
}

function csShowCreateForm() {
  const modal = document.getElementById("cs-modal-content");
  modal.innerHTML = `
    <div class="cs-modal-header" style="display:flex;justify-content:space-between;align-items:center">
      <h3 style="margin:0;font-size:18px">📋 새 CS/RMA 접수</h3>
      <button onclick="csCloseModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:#6b7280">&times;</button>
    </div>
    <div class="cs-modal-body">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        <div>
          <div class="cs-field-label">고객명 *</div>
          <input type="text" id="cs-f-name" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="홍길동">
        </div>
        <div>
          <div class="cs-field-label">연락처 *</div>
          <input type="text" id="cs-f-contact" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="010-1234-5678">
        </div>
        <div>
          <div class="cs-field-label">모델명 / 제품명 *</div>
          <input type="text" id="cs-f-product" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="제품명 입력">
        </div>
        <div>
          <div class="cs-field-label">시리얼 번호</div>
          <input type="text" id="cs-f-serial" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="선택 입력">
        </div>
      </div>
      <div style="margin-top:12px">
        <div class="cs-field-label">불량 증상 * <span style="color:#ef4444;font-size:11px">(기술팀에게 전달됩니다. 최대한 상세히 작성)</span></div>
        <textarea id="cs-f-symptom" rows="4" style="width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;resize:vertical;font-family:inherit" placeholder="고객이 설명한 증상을 정확하게 기록해주세요.\n예: USB 포트 3번에 기기 연결 시 인식 안됨. 다른 포트는 정상."></textarea>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px">
        <div>
          <div class="cs-field-label">택배사</div>
          <input type="text" id="cs-f-courier" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="로젠, CJ 등">
        </div>
        <div>
          <div class="cs-field-label">송장번호</div>
          <input type="text" id="cs-f-tracking" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="택배 송장번호">
        </div>
      </div>
      <div style="margin-top:12px">
        <div class="cs-field-label">메모</div>
        <input type="text" id="cs-f-memo" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px" placeholder="기타 참고사항">
      </div>
      <div style="margin-top:20px;display:flex;justify-content:flex-end;gap:8px">
        <button onclick="csCloseModal()" style="padding:8px 20px;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;font-size:14px">취소</button>
        <button onclick="csSubmitCreate()" class="btn btn-primary" style="padding:8px 24px;font-size:14px">접수 완료</button>
      </div>
    </div>`;
  document.getElementById("cs-modal-overlay").style.display = "block";
}

async function csSubmitCreate() {
  const name = document.getElementById("cs-f-name")?.value.trim();
  const contact = document.getElementById("cs-f-contact")?.value.trim();
  const product = document.getElementById("cs-f-product")?.value.trim();
  const symptom = document.getElementById("cs-f-symptom")?.value.trim();
  if (!name || !contact || !product || !symptom) {
    alert("고객명, 연락처, 제품명, 불량 증상은 필수입니다.");
    return;
  }
  try {
    const res = await api.post("/api/cs/tickets", {
      customer_name: name,
      contact_info: contact,
      product_name: product,
      serial_number: document.getElementById("cs-f-serial")?.value.trim() || "",
      defect_symptom: symptom,
      courier: document.getElementById("cs-f-courier")?.value.trim() || "",
      tracking_no: document.getElementById("cs-f-tracking")?.value.trim() || "",
      memo: document.getElementById("cs-f-memo")?.value.trim() || "",
    });
    alert(res.message || "접수 완료");
    csCloseModal();
    csInit();
  } catch(e) {
    alert("접수 실패: " + (e.message || e));
  }
}

// ── 상세 보기 ──
async function csShowDetail(ticketId) {
  const modal = document.getElementById("cs-modal-content");
  modal.innerHTML = '<div style="padding:40px;text-align:center;color:#9ca3af">불러오는 중...</div>';
  document.getElementById("cs-modal-overlay").style.display = "block";
  try {
    const d = await api.get(`/api/cs/tickets/${ticketId}`);
    const t = d.ticket;
    const tr = d.test_result;
    const files = d.files || [];
    const logs = d.logs || [];

    // 상태 단계별 진행 바
    const steps = ["접수완료", "물류수령", "기술인계", "테스트완료", "처리종결"];
    const curIdx = steps.indexOf(t.current_status);
    const stepsHtml = steps.map((s, i) => {
      const done = i <= curIdx;
      const active = i === curIdx;
      return `<div style="flex:1;text-align:center">
        <div style="width:28px;height:28px;border-radius:50%;margin:0 auto 4px;line-height:28px;font-size:12px;font-weight:600;${done?'background:#2563eb;color:#fff':'background:#e5e7eb;color:#9ca3af'};${active?'box-shadow:0 0 0 3px rgba(37,99,235,0.3)':''}">${i+1}</div>
        <div style="font-size:11px;color:${done?'#2563eb':'#9ca3af'};font-weight:${active?600:400}">${s}</div>
      </div>`;
    }).join('<div style="flex:0.5;display:flex;align-items:center;padding-bottom:18px"><div style="height:2px;width:100%;background:${curIdx>=i?"#2563eb":"#e5e7eb"}"></div></div>'.replace(/\${.*?}/g,'#e5e7eb'));

    // 액션 버튼
    let actionHtml = "";
    if (t.current_status === "접수완료") {
      actionHtml = `<button onclick="csAction('${t.ticket_id}','receive')" class="btn btn-primary" style="font-size:13px">📦 택배 수령</button>`;
    } else if (t.current_status === "물류수령") {
      actionHtml = `<button onclick="csAction('${t.ticket_id}','handover')" class="btn btn-primary" style="font-size:13px">🔬 기술 담당자 인계</button>`;
    } else if (t.current_status === "기술인계") {
      actionHtml = `<button onclick="csShowTestForm('${t.ticket_id}')" class="btn btn-primary" style="font-size:13px">🧪 테스트 결과 입력</button>`;
    } else if (t.current_status === "테스트완료") {
      actionHtml = `<div style="display:flex;gap:6px;flex-wrap:wrap">
        <button onclick="csResolve('${t.ticket_id}','교환발송')" style="padding:6px 16px;border:1px solid #2563eb;border-radius:6px;background:#eff6ff;color:#2563eb;cursor:pointer;font-size:13px;font-weight:500">🔄 교환 발송</button>
        <button onclick="csResolve('${t.ticket_id}','환불처리')" style="padding:6px 16px;border:1px solid #dc2626;border-radius:6px;background:#fef2f2;color:#dc2626;cursor:pointer;font-size:13px;font-weight:500">💰 환불 처리</button>
        <button onclick="csResolve('${t.ticket_id}','정상반송')" style="padding:6px 16px;border:1px solid #059669;border-radius:6px;background:#ecfdf5;color:#059669;cursor:pointer;font-size:13px;font-weight:500">📤 정상품 반송</button>
      </div>`;
    }

    // 테스트 결과
    let testHtml = "";
    if (tr) {
      testHtml = `<div style="margin-top:16px;padding:14px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px">
        <div style="font-weight:600;font-size:13px;margin-bottom:8px">🧪 기술 테스트 결과</div>
        <div><span class="cs-test-badge cs-test-${tr.test_status}">${{"정상":"🟢 정상","의심":"🟡 의심","불량":"🔴 불량"}[tr.test_status] || tr.test_status}</span></div>
        ${tr.test_comment ? `<div style="margin-top:8px;font-size:13px;color:#374151;line-height:1.5;white-space:pre-wrap">${_esc(tr.test_comment)}</div>` : ""}
      </div>`;
    }

    // 첨부파일
    let filesHtml = "";
    if (files.length > 0) {
      filesHtml = `<div style="margin-top:16px">
        <div style="font-weight:600;font-size:13px;margin-bottom:8px">📎 첨부파일 (${files.length})</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${files.map(f => {
            if (f.file_type === "image") {
              return `<a href="${f.file_url}" target="_blank"><img src="${f.file_url}" style="width:80px;height:80px;object-fit:cover;border-radius:6px;border:1px solid #e5e7eb"></a>`;
            } else if (f.file_type === "video") {
              return `<a href="${f.file_url}" target="_blank" style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;background:#f3f4f6;border-radius:6px;font-size:12px;color:#374151;text-decoration:none">🎬 ${_esc(f.file_name)}</a>`;
            }
            return `<a href="${f.file_url}" target="_blank" style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;background:#f3f4f6;border-radius:6px;font-size:12px;color:#374151;text-decoration:none">📄 ${_esc(f.file_name)}</a>`;
          }).join("")}
        </div>
      </div>`;
    }

    // 타임라인
    const timelineHtml = logs.length > 0 ? `<div style="margin-top:16px">
      <div style="font-weight:600;font-size:13px;margin-bottom:10px">📜 처리 이력</div>
      <div class="cs-timeline">
        ${logs.map(l => `<div class="cs-timeline-item">
          <div class="cs-timeline-dot done"></div>
          <div style="font-size:13px;font-weight:500;color:#374151">${_esc(l.action_type)}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:2px">${_esc(l.detail)}</div>
          <div class="cs-timeline-time">${l.created_at || ""} · ${_esc(l.actor_name)}</div>
        </div>`).join("")}
      </div>
    </div>` : "";

    modal.innerHTML = `
      <div class="cs-modal-header" style="display:flex;justify-content:space-between;align-items:center">
        <div>
          <span style="font-weight:600;color:#2563eb;font-size:15px">${t.ticket_id}</span>
          <span class="cs-status-badge cs-status-${t.current_status}" style="margin-left:8px">${t.current_status}</span>
          ${t.final_action ? `<span style="margin-left:8px;font-size:12px;color:#6b7280">${t.final_action}</span>` : ""}
        </div>
        <button onclick="csCloseModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:#6b7280">&times;</button>
      </div>
      <div class="cs-modal-body">
        <!-- 진행 단계 -->
        <div style="display:flex;align-items:flex-start;margin-bottom:20px;gap:0">${stepsHtml}</div>

        <!-- 고객 정보 -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px 16px">
          <div><div class="cs-field-label">고객명</div><div class="cs-field-value">${_esc(t.customer_name)}</div></div>
          <div><div class="cs-field-label">연락처</div><div class="cs-field-value">${_esc(t.contact_info)}</div></div>
          <div><div class="cs-field-label">제품명</div><div class="cs-field-value">${_esc(t.product_name)}</div></div>
          <div><div class="cs-field-label">시리얼 번호</div><div class="cs-field-value">${_esc(t.serial_number) || "-"}</div></div>
          ${t.courier ? `<div><div class="cs-field-label">택배사</div><div class="cs-field-value">${_esc(t.courier)}</div></div>` : ""}
          ${t.tracking_no ? `<div><div class="cs-field-label">송장번호</div><div class="cs-field-value">${_esc(t.tracking_no)}</div></div>` : ""}
        </div>

        <!-- 불량 증상 (강조) -->
        <div style="margin-top:8px">
          <div class="cs-field-label">불량 증상</div>
          <div class="cs-symptom-box">${_esc(t.defect_symptom)}</div>
        </div>

        ${testHtml}
        ${filesHtml}

        <!-- 파일 업로드 -->
        ${t.current_status !== "처리종결" ? `<div style="margin-top:16px">
          <label style="display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border:1px dashed #d1d5db;border-radius:6px;cursor:pointer;font-size:12px;color:#6b7280">
            📎 파일 첨부
            <input type="file" accept="image/*,video/*,.pdf" style="display:none" onchange="csUploadFile('${t.ticket_id}',this)">
          </label>
        </div>` : ""}

        <!-- 메모 추가 -->
        ${t.current_status !== "처리종결" ? `<div style="margin-top:12px;display:flex;gap:6px">
          <input type="text" id="cs-memo-input" placeholder="메모 추가..." style="flex:1;padding:6px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          <button onclick="csAddMemo('${t.ticket_id}')" style="padding:6px 14px;background:#f3f4f6;border:1px solid #d1d5db;border-radius:6px;cursor:pointer;font-size:13px">추가</button>
        </div>` : ""}

        <!-- 액션 버튼 -->
        ${actionHtml ? `<div style="margin-top:16px;padding-top:16px;border-top:1px solid #e5e7eb">${actionHtml}</div>` : ""}

        ${timelineHtml}
      </div>`;
  } catch(e) {
    modal.innerHTML = `<div style="padding:40px;text-align:center;color:#ef4444">오류: ${e.message || e}</div>`;
  }
}

// ── 상태 변경 액션 ──
async function csAction(ticketId, action) {
  const memo = document.getElementById("cs-memo-input")?.value.trim() || "";
  try {
    const res = await api.put(`/api/cs/tickets/${ticketId}/${action}`, { memo });
    alert(res.message || "처리 완료");
    csShowDetail(ticketId);
    csLoadStats();
    csLoadTickets();
  } catch(e) { alert("오류: " + (e.message || e)); }
}

// ── 테스트 결과 폼 ──
function csShowTestForm(ticketId) {
  const modal = document.getElementById("cs-modal-content");
  const existing = modal.innerHTML;
  // 모달 내부에 테스트 폼 삽입
  const formHtml = `
    <div id="cs-test-form" style="margin-top:16px;padding:16px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px">
      <div style="font-weight:600;font-size:14px;margin-bottom:12px">🧪 테스트 결과 입력</div>
      <div style="display:flex;gap:12px;margin-bottom:12px">
        <label style="display:flex;align-items:center;gap:4px;cursor:pointer"><input type="radio" name="cs-test-status" value="정상"> 🟢 정상</label>
        <label style="display:flex;align-items:center;gap:4px;cursor:pointer"><input type="radio" name="cs-test-status" value="의심"> 🟡 의심</label>
        <label style="display:flex;align-items:center;gap:4px;cursor:pointer"><input type="radio" name="cs-test-status" value="불량" checked> 🔴 불량</label>
      </div>
      <textarea id="cs-test-comment" rows="3" style="width:100%;padding:10px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;font-family:inherit;resize:vertical" placeholder="작동 테스트 결과 및 소견 (예: USB 포트 3번 물리적 파손 확인)"></textarea>
      <div style="margin-top:10px;display:flex;justify-content:flex-end;gap:8px">
        <button onclick="document.getElementById('cs-test-form').remove()" style="padding:6px 14px;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;font-size:13px">취소</button>
        <button onclick="csSubmitTest('${ticketId}')" class="btn btn-primary" style="padding:6px 18px;font-size:13px">테스트 완료 (CS로 이관)</button>
      </div>
    </div>`;
  // 액션 버튼 영역 뒤에 삽입
  const body = modal.querySelector(".cs-modal-body");
  if (body) body.insertAdjacentHTML("beforeend", formHtml);
}

async function csSubmitTest(ticketId) {
  const status = document.querySelector('input[name="cs-test-status"]:checked')?.value;
  const comment = document.getElementById("cs-test-comment")?.value.trim() || "";
  if (!status) { alert("테스트 상태를 선택해주세요."); return; }
  try {
    const res = await api.post(`/api/cs/tickets/${ticketId}/test-result`, {
      test_status: status,
      test_comment: comment,
    });
    alert(res.message || "테스트 결과 등록 완료");
    csShowDetail(ticketId);
    csLoadStats();
    csLoadTickets();
  } catch(e) { alert("오류: " + (e.message || e)); }
}

// ── 최종 처리 ──
async function csResolve(ticketId, action) {
  if (!confirm(`"${action}" 처리하시겠습니까?`)) return;
  const memo = document.getElementById("cs-memo-input")?.value.trim() || "";
  try {
    const res = await api.put(`/api/cs/tickets/${ticketId}/resolve`, { action, memo });
    alert(res.message || "처리 완료");
    csShowDetail(ticketId);
    csLoadStats();
    csLoadTickets();
  } catch(e) { alert("오류: " + (e.message || e)); }
}

// ── 파일 업로드 ──
async function csUploadFile(ticketId, input) {
  const file = input.files[0];
  if (!file) return;
  const formData = new FormData();
  formData.append("file", file);
  try {
    const token = localStorage.getItem("token");
    const res = await fetch(`/api/cs/tickets/${ticketId}/upload`, {
      method: "POST",
      headers: { "Authorization": `Bearer ${token}` },
      body: formData,
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "업로드 실패");
    alert("파일 업로드 완료");
    csShowDetail(ticketId);
  } catch(e) { alert("업로드 오류: " + (e.message || e)); }
}

// ── 메모 추가 ──
async function csAddMemo(ticketId) {
  const memo = document.getElementById("cs-memo-input")?.value.trim();
  if (!memo) { alert("메모 내용을 입력해주세요."); return; }
  try {
    await api.post(`/api/cs/tickets/${ticketId}/memo`, { memo });
    csShowDetail(ticketId);
  } catch(e) { alert("오류: " + (e.message || e)); }
}


// ═══════════════════════════════════════════════════════════════
//  판매 에이전트 (Sales Agent)
// ═══════════════════════════════════════════════════════════════
let _saCurrentFileId = null;
let _saCurrentJobId = null;
let _saCurrentResult = null;
let _saCharts = {};
let _saPollingTimer = null;
let _saWebSocket = null;

const SA_AGENTS = {
  customer:      { name: "거래처 분석", icon: "👥" },
  product:       { name: "품목 관리", icon: "📦" },
  strategy:      { name: "판매전략",   icon: "🎯" },
  future:        { name: "미래전략",   icon: "🔮" },
  partnership:   { name: "파트너십",   icon: "🤝" },
  visualization: { name: "KPI/시각화", icon: "📊" },
};

function saInit() {
  // 초기화: 업로드 탭 표시
  saSwitchTab("upload");
}

function saSwitchTab(tab) {
  document.querySelectorAll(".sa-tab-content").forEach(el => el.style.display = "none");
  document.querySelectorAll(".sa-tab").forEach(el => el.classList.remove("active"));
  const content = document.getElementById("sa-tab-" + tab);
  if (content) content.style.display = "block";
  const btn = document.querySelector(`.sa-tab[data-tab="${tab}"]`);
  if (btn) btn.classList.add("active");
}

function saHandleDrop(e) {
  e.preventDefault();
  e.currentTarget.style.borderColor = "#ccc";
  e.currentTarget.style.background = "#fafbfc";
  const files = e.dataTransfer.files;
  if (files.length > 0) saHandleFile(files[0]);
}

async function saHandleFile(file) {
  if (!file) return;
  if (!file.name.match(/\.xlsx?$/i)) {
    alert("xlsx 또는 xls 파일만 업로드할 수 있습니다.");
    return;
  }

  const area = document.getElementById("sa-upload-area");
  area.innerHTML = `<div style="font-size:32px">⏳</div><div>업로드 중... ${_esc(file.name)}</div>`;

  try {
    const fd = new FormData();
    fd.append("file", file);
    const res = await api.postForm("/api/sales-agent/upload", fd);

    _saCurrentFileId = res.file_id;

    // 파싱 결과 표시
    const summary = res.summary;
    // 거래처명 추출 (customers_preview에서 이름 목록)
    const custNames = (res.customers_preview || []).map(c => c.customer_name || "").filter(Boolean);
    const custDisplay = custNames.length > 0 ? custNames.join(", ") : `${summary.total_customers||0}개`;
    const amt = summary.total_amount || 0;
    document.getElementById("sa-parse-summary").innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px;margin-bottom:12px">
        <div class="sa-kpi-card"><div class="kpi-label">거래 건수</div><div class="kpi-val" style="font-size:15px">${(summary.total_rows||0).toLocaleString()}</div></div>
        <div class="sa-kpi-card"><div class="kpi-label">거래처</div><div class="kpi-val" style="font-size:15px">${_esc(custDisplay)}</div></div>
        <div class="sa-kpi-card"><div class="kpi-label">품목</div><div class="kpi-val" style="font-size:15px">${(summary.total_products||0).toLocaleString()}</div></div>
        <div class="sa-kpi-card"><div class="kpi-label">총 매출액</div><div class="kpi-val" style="font-size:15px">${amt.toLocaleString()}<span style="font-size:11px">원</span></div></div>
      </div>
      <div style="font-size:13px;color:#666">
        📅 기간: ${res.period_start || "?"} ~ ${res.period_end || "?"}<br>
        📄 파일: ${_esc(res.file_name)}
      </div>
    `;
    document.getElementById("sa-parse-result").style.display = "block";

    // 업로드 영역 복원
    area.innerHTML = `
      <div style="font-size:48px;margin-bottom:12px">✅</div>
      <div style="font-size:16px;font-weight:600;color:#10b981">${_esc(file.name)} 업로드 완료</div>
      <div style="font-size:13px;color:#888;margin-top:6px">다른 파일을 업로드하려면 클릭</div>
    `;
  } catch (e) {
    area.innerHTML = `
      <div style="font-size:48px;margin-bottom:12px">❌</div>
      <div style="font-size:16px;font-weight:600;color:#ef4444">업로드 실패</div>
      <div style="font-size:13px;color:#888;margin-top:6px">${_esc(e.message || "알 수 없는 오류")}</div>
    `;
    alert("업로드 실패: " + (e.message || e));
  }
}

async function saStartAnalysis() {
  if (!_saCurrentFileId) { alert("먼저 파일을 업로드해주세요."); return; }

  const btn = document.getElementById("sa-analyze-btn");
  btn.disabled = true;
  btn.textContent = "⏳ 분석 시작 중...";

  try {
    const fd = new FormData();
    fd.append("file_id", _saCurrentFileId);
    const res = await api.postForm("/api/sales-agent/analyze", fd);

    _saCurrentJobId = res.job_id;

    // 진행 상태 UI 표시
    document.getElementById("sa-progress").style.display = "block";
    const statusDiv = document.getElementById("sa-agent-status");
    statusDiv.innerHTML = Object.entries(SA_AGENTS).map(([k, v]) =>
      `<div id="sa-status-${k}"><span class="sa-progress-dot pending"></span>${v.icon} ${v.name}</div>`
    ).join("");

    // WebSocket 실시간 진행 (폴링 fallback 포함)
    _saConnectWebSocket(res.job_id);

  } catch (e) {
    alert("분석 시작 실패: " + (e.message || e));
    btn.disabled = false;
    btn.textContent = "🚀 AI 분석 시작 (6개 에이전트 병렬)";
  }
}

function _saConnectWebSocket(jobId) {
  // WebSocket 연결 시도
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${location.host}/api/sales-agent/ws/${jobId}`;

  try {
    _saWebSocket = new WebSocket(wsUrl);

    _saWebSocket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        _saHandleProgressUpdate(data);
      } catch(e) { console.error("WS parse error:", e); }
    };

    _saWebSocket.onerror = (e) => {
      console.warn("WebSocket 연결 실패, 폴링으로 전환:", e);
      _saWebSocket = null;
      // 폴링 fallback
      if (!_saPollingTimer) {
        _saPollingTimer = setInterval(() => saPollStatus(), 3000);
        saPollStatus();
      }
    };

    _saWebSocket.onclose = () => {
      // 분석 완료 전에 닫히면 폴링으로 전환
      if (_saCurrentJobId && !_saCurrentResult) {
        if (!_saPollingTimer) {
          _saPollingTimer = setInterval(() => saPollStatus(), 3000);
          saPollStatus();
        }
      }
      _saWebSocket = null;
    };
  } catch(e) {
    console.warn("WebSocket 미지원, 폴링 사용:", e);
    _saPollingTimer = setInterval(() => saPollStatus(), 3000);
    saPollStatus();
  }
}

function _saHandleProgressUpdate(data) {
  const fill = document.getElementById("sa-progress-fill");
  if (fill) fill.style.width = (data.progress || 0) + "%";

  // 에이전트별 상태 업데이트
  if (data.agents) {
    Object.entries(data.agents).forEach(([k, status]) => {
      const el = document.getElementById("sa-status-" + k);
      if (el) {
        const dot = el.querySelector(".sa-progress-dot");
        if (dot) dot.className = "sa-progress-dot " + status;
      }
    });
  }

  if (data.status === "completed") {
    _saCleanupProgress();
    saLoadResult(_saCurrentJobId);
  } else if (data.status === "failed") {
    _saCleanupProgress();
    document.getElementById("sa-progress").innerHTML = `
      <div class="card" style="background:#fff0f0;border:1px solid #ffc0c0">
        <h4 style="margin-top:0;color:#ef4444">❌ 분석 실패</h4>
        <p>분석 중 오류가 발생했습니다. 다시 시도해주세요.</p>
      </div>`;
    document.getElementById("sa-analyze-btn").disabled = false;
    document.getElementById("sa-analyze-btn").textContent = "🚀 AI 분석 시작 (6개 에이전트 병렬)";
  }
}

function _saCleanupProgress() {
  if (_saPollingTimer) { clearInterval(_saPollingTimer); _saPollingTimer = null; }
  if (_saWebSocket) { try { _saWebSocket.close(); } catch(e){} _saWebSocket = null; }
}

async function saPollStatus() {
  if (!_saCurrentJobId) return;
  try {
    const res = await api.get(`/api/sales-agent/status/${_saCurrentJobId}`);
    _saHandleProgressUpdate(res);
  } catch (e) {
    console.error("Poll error:", e);
  }
}

async function saLoadResult(jobId) {
  try {
    const result = await api.get(`/api/sales-agent/result/${jobId}`);
    _saCurrentResult = result;

    // 진행 UI 업데이트
    const elapsed = result.elapsed_seconds || 0;
    document.getElementById("sa-progress").innerHTML = `
      <div class="card" style="background:#f0fff4;border:1px solid #b3ffb3">
        <h4 style="margin-top:0;color:#10b981">✅ 분석 완료! (${elapsed.toFixed(1)}초)</h4>
        <p>6개 AI 에이전트 분석이 완료되었습니다. 대시보드와 리포트 탭에서 결과를 확인하세요.</p>
        <button class="btn btn-primary" onclick="saSwitchTab('dashboard')" style="margin-right:8px">📊 대시보드 보기</button>
        <button class="btn btn-outline" onclick="saSwitchTab('reports')">📝 리포트 보기</button>
      </div>`;

    // 대시보드 & 리포트 렌더링
    saRenderDashboard(result);
    saRenderReports(result);

  } catch (e) {
    alert("결과 로드 실패: " + (e.message || e));
  }
}

function saRenderDashboard(result) {
  const agents = result.agents || {};
  const vizResult = agents.visualization?.result || {};
  const engine = result.engine_results || {};

  // KPI 카드 (카운트업 애니메이션)
  const kpiCards = vizResult.kpi_cards || [];
  const kpiHtml = kpiCards.map((k, i) => {
    const trendClass = k.trend === "up" ? "up" : k.trend === "down" ? "down" : "";
    return `<div class="sa-kpi-card sa-animate-count" style="animation-delay:${i*0.1}s">
      <div class="kpi-label">${_esc(k.label)}</div>
      <div class="kpi-val" data-countup="${k.value||0}">0<span style="font-size:12px">${_esc(k.unit||"")}</span></div>
      ${k.change ? `<div class="kpi-change ${trendClass}">${_esc(k.change)}</div>` : ""}
    </div>`;
  }).join("");
  document.getElementById("sa-kpi-cards").innerHTML = kpiHtml;
  // 카운트업 애니메이션
  document.querySelectorAll("[data-countup]").forEach(el => {
    const target = parseInt(el.dataset.countup) || 0;
    const unit = el.querySelector("span")?.outerHTML || "";
    let current = 0;
    const step = Math.max(Math.ceil(target / 40), 1);
    const timer = setInterval(() => {
      current += step;
      if (current >= target) { current = target; clearInterval(timer); }
      el.innerHTML = current.toLocaleString() + unit;
    }, 30);
  });

  // 차트 렌더링
  const charts = vizResult.charts || {};
  _saDestroyCharts();

  if (charts.monthly_revenue?.length) {
    _saCharts.monthly = new Chart(document.getElementById("sa-chart-monthly"), {
      type: "line",
      data: {
        labels: charts.monthly_revenue.map(d => d.month),
        datasets: [{
          label: "매출액", data: charts.monthly_revenue.map(d => d.amount),
          borderColor: "#4A90D9", backgroundColor: "rgba(74,144,217,0.1)", fill: true, tension: 0.3,
        }],
      },
      options: { responsive: true, animation: { duration: 1200 }, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { callback: v => (v/10000).toLocaleString() + "만" } } } },
    });
  }

  if (charts.top_customers?.length) {
    const top10 = charts.top_customers.slice(0, 10);
    _saCharts.customers = new Chart(document.getElementById("sa-chart-customers"), {
      type: "bar",
      data: {
        labels: top10.map(d => d.name?.length > 10 ? d.name.slice(0,10)+"…" : d.name),
        datasets: [{ label: "매출액", data: top10.map(d => d.amount), backgroundColor: "#667eea" }],
      },
      options: { indexAxis: "y", responsive: true, animation: { duration: 1000 }, plugins: { legend: { display: false } }, scales: { x: { ticks: { callback: v => (v/10000).toLocaleString() + "만" } } } },
    });
  }

  if (charts.category_share?.length) {
    const colors = ["#667eea","#764ba2","#f093fb","#4facfe","#43e97b","#fa709a","#fee140","#30cfd0","#a8edea","#fed6e3"];
    _saCharts.category = new Chart(document.getElementById("sa-chart-category"), {
      type: "doughnut",
      data: {
        labels: charts.category_share.map(d => d.category),
        datasets: [{ data: charts.category_share.map(d => d.amount), backgroundColor: colors }],
      },
      options: { responsive: true, animation: { duration: 1000 }, plugins: { legend: { position: "right", labels: { font: { size: 11 } } } } },
    });
  }

  if (charts.monthly_count?.length) {
    _saCharts.count = new Chart(document.getElementById("sa-chart-count"), {
      type: "bar",
      data: {
        labels: charts.monthly_count.map(d => d.month),
        datasets: [{ label: "거래 건수", data: charts.monthly_count.map(d => d.count), backgroundColor: "rgba(118,75,162,0.6)" }],
      },
      options: { responsive: true, animation: { duration: 1000 }, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } },
    });
  }

  // ── Python 엔진 결과 렌더링 ──
  _saRenderEngineResults(engine);

  // ── Mermaid 다이어그램 ──
  _saRenderMermaid(engine, agents);

  // ── 에이전트 요약 카드 ──
  const summariesDiv = document.getElementById("sa-agent-summaries");
  summariesDiv.innerHTML = Object.entries(agents).map(([k, ag]) => {
    const r = ag.result || {};
    return `<div class="sa-agent-card sa-animate-in">
      <h4>${ag.icon} ${ag.name} ${ag.status==="error"?'<span style="color:#ef4444;font-size:12px">오류</span>':''}</h4>
      <div class="agent-summary">${_esc(r.summary || "분석 결과 없음")}</div>
    </div>`;
  }).join("");

  // 권고사항
  const recs = result.top_recommendations || [];
  document.getElementById("sa-recommendations").innerHTML = recs.length
    ? recs.map(r => `<div style="padding:6px 0;border-bottom:1px solid #f0e0c0;font-size:13px">💡 ${_esc(r)}</div>`).join("")
    : '<div style="color:#888">권고사항이 없습니다.</div>';

  document.getElementById("sa-no-data").style.display = "none";
  document.getElementById("sa-dashboard-content").style.display = "block";
}

function _saRenderEngineResults(engine) {
  // RFM 세그먼트 바 차트
  const rfm = engine.rfm || {};
  const segsDiv = document.getElementById("sa-rfm-segments");
  if (segsDiv && rfm.segments) {
    const segColors = {Champions:"#10b981",Loyal:"#3b82f6",Potential:"#8b5cf6","At Risk":"#f59e0b",Hibernating:"#6b7280"};
    const totalCust = Object.values(rfm.segments).reduce((s,v) => s+v.count, 0) || 1;
    segsDiv.innerHTML = Object.entries(rfm.segments).map(([name,data]) => {
      const pct = Math.round(data.count / totalCust * 100);
      return `<div style="margin:6px 0;display:flex;align-items:center;gap:8px">
        <div style="width:100px;font-size:12px;font-weight:600">${name}</div>
        <div class="sa-seg-bar" style="width:${Math.max(pct*3,40)}px;background:${segColors[name]||'#999'}">${data.count}</div>
        <div style="font-size:11px;color:#888">${pct}% / ${data.total_amount.toLocaleString()}원</div>
      </div>`;
    }).join("");
    // RFM 파이 차트
    const rfmCanvas = document.getElementById("sa-chart-rfm-pie");
    if (rfmCanvas) {
      _saCharts.rfmPie = new Chart(rfmCanvas, {
        type: "doughnut",
        data: {
          labels: Object.keys(rfm.segments),
          datasets: [{data: Object.values(rfm.segments).map(s=>s.count), backgroundColor: Object.keys(rfm.segments).map(k=>segColors[k]||"#999")}],
        },
        options: { responsive: true, animation:{duration:1000}, plugins: { legend: { position: "bottom", labels: { font: { size: 10 } } } } },
      });
    }
  }

  // ABC 요약
  const abc = engine.abc || {};
  const abcDiv = document.getElementById("sa-abc-summary");
  if (abcDiv && abc.grade_summary) {
    const gColors = {A:"#10b981",B:"#f59e0b",C:"#6b7280"};
    abcDiv.innerHTML = Object.entries(abc.grade_summary).map(([g,d]) =>
      `<div style="display:inline-flex;align-items:center;gap:6px;margin:4px 8px 4px 0">
        <span class="sa-grade-${g}" style="padding:2px 10px;border-radius:12px;font-size:12px;font-weight:700">${g}</span>
        <span style="font-size:12px">${d.count}개 품목 (${d.pct}%, ${d.amount.toLocaleString()}원)</span>
      </div>`
    ).join("");
    const abcCanvas = document.getElementById("sa-chart-abc");
    if (abcCanvas) {
      _saCharts.abcBar = new Chart(abcCanvas, {
        type: "bar",
        data: {
          labels: Object.keys(abc.grade_summary),
          datasets: [{label: "매출액", data: Object.values(abc.grade_summary).map(d=>d.amount), backgroundColor: Object.keys(abc.grade_summary).map(g=>gColors[g]||"#999")}],
        },
        options: { responsive: true, animation:{duration:800}, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => (v/10000).toLocaleString()+"만" } } } },
      });
    }
  }

  // 이탈 위험
  const churnDiv = document.getElementById("sa-churn-risk");
  if (churnDiv && rfm.churn_risk?.length) {
    churnDiv.innerHTML = rfm.churn_risk.slice(0,8).map(c =>
      `<div style="padding:6px 10px;margin:4px 0;background:${c.risk_level==="높음"?"#fff0f0":"#fffaf0"};border-radius:6px;font-size:12px;display:flex;justify-content:space-between;align-items:center">
        <span><strong>${_esc(c.customer_name)}</strong> <span class="sa-risk-badge sa-risk-${c.risk_level}">${c.risk_level}</span></span>
        <span style="color:#888">${_esc(c.reason||"")}</span>
      </div>`
    ).join("") + (rfm.churn_risk.length > 8 ? `<div style="font-size:11px;color:#888;margin-top:4px">외 ${rfm.churn_risk.length-8}건</div>` : "");
  } else if (churnDiv) {
    churnDiv.innerHTML = '<div style="color:#888;font-size:13px;padding:20px;text-align:center">이탈 위험 거래처가 감지되지 않았습니다.</div>';
  }

  // 3년 시나리오 차트
  const trends = engine.trends || {};
  const scenarioCanvas = document.getElementById("sa-chart-scenario");
  if (scenarioCanvas && trends.growth_scenarios) {
    const gs = trends.growth_scenarios;
    _saCharts.scenario = new Chart(scenarioCanvas, {
      type: "line",
      data: {
        labels: ["현재","1년차","2년차","3년차"],
        datasets: [
          {label:"보수적(10%)", data:[gs.current_annual, gs.conservative.year1, gs.conservative.year2, gs.conservative.year3], borderColor:"#6b7280", borderDash:[5,5], tension:.3},
          {label:"기본(20%)", data:[gs.current_annual, gs.moderate.year1, gs.moderate.year2, gs.moderate.year3], borderColor:"#3b82f6", borderWidth:2, tension:.3},
          {label:"공격적(35%)", data:[gs.current_annual, gs.aggressive.year1, gs.aggressive.year2, gs.aggressive.year3], borderColor:"#10b981", borderWidth:2, tension:.3},
        ],
      },
      options: { responsive: true, animation:{duration:1200}, plugins: { legend: { position: "bottom" } }, scales: { y: { ticks: { callback: v => (v/100000000).toFixed(1)+"억" } } } },
    });
  }

  // CLV 티어
  const clv = engine.clv || {};
  const clvDiv = document.getElementById("sa-clv-tiers");
  if (clvDiv && clv.tier_summary) {
    clvDiv.innerHTML = Object.entries(clv.tier_summary).map(([tier, data]) => {
      const tClass = tier.replace("Tier ","");
      return `<div style="margin:6px 0;padding:8px 12px;background:#f9fafb;border-radius:8px;border-left:4px solid ${tClass==="1"?"#3b82f6":tClass==="2"?"#8b5cf6":"#9ca3af"};display:flex;justify-content:space-between;align-items:center;font-size:13px">
        <div><span class="sa-tier-badge sa-tier-${tClass}">${tier}</span> <strong>${data.count}</strong>개 거래처</div>
        <div>CLV ${data.total_clv.toLocaleString()}원 / ACV ${(data.total_acv||0).toLocaleString()}원</div>
      </div>`;
    }).join("") +
    (clv.clv_results?.length ? `<details style="margin-top:8px"><summary style="font-size:12px;color:#666;cursor:pointer">전체 ${clv.clv_results.length}개 거래처 보기</summary>
      <table class="table" style="font-size:11px;margin-top:6px"><thead><tr><th>거래처</th><th>티어</th><th>CLV</th><th>ACV</th></tr></thead><tbody>
      ${clv.clv_results.slice(0,20).map(r=>`<tr><td>${_esc(r.customer_name)}</td><td><span class="sa-tier-badge sa-tier-${r.tier.replace('Tier ','')}">${r.tier}</span></td><td>${r.clv.toLocaleString()}</td><td>${r.acv.toLocaleString()}</td></tr>`).join("")}
      </tbody></table></details>` : "");
  }

  // 수요 예측
  const forecast = engine.forecast || {};
  const fcDiv = document.getElementById("sa-forecast-table");
  if (fcDiv && forecast.forecast?.length) {
    fcDiv.innerHTML = `<table class="table" style="font-size:12px"><thead><tr><th>품목</th><th>월평균</th><th>트렌드</th><th>3개월 예측</th></tr></thead><tbody>
    ${forecast.forecast.slice(0,10).map(f => {
      const icon = f.trend==="증가"?"📈":f.trend==="감소"?"📉":"➡️";
      return `<tr><td>${_esc(f.product_name)}</td><td>${f.current_monthly_avg.toLocaleString()}</td><td>${icon} ${f.trend}</td><td>${(f.forecast_3m||[]).map(v=>v.toLocaleString()).join(" → ")}</td></tr>`;
    }).join("")}
    </tbody></table>`;
  }
}

function _saRenderMermaid(engine, agents) {
  // 어카운트 스쿼드 구성도
  const clv = engine.clv || {};
  const squadDiv = document.getElementById("sa-mermaid-squad");
  if (squadDiv && clv.tier_summary) {
    const t1 = clv.tier_summary["Tier 1"]?.count || 0;
    const t2 = clv.tier_summary["Tier 2"]?.count || 0;
    const t3 = clv.tier_summary["Tier 3"]?.count || 0;
    const mermaidDef = `graph TD
    A[영업팀 조직] --> T1["Tier 1 전략 거래처<br/>${t1}개사"]
    A --> T2["Tier 2 성장 거래처<br/>${t2}개사"]
    A --> T3["Tier 3 일반 거래처<br/>${t3}개사"]
    T1 --> S1["전담 AM + SE + CS<br/>3인 스쿼드"]
    T2 --> S2["담당 AM + 공유 SE"]
    T3 --> S3["인사이드 세일즈<br/>+ 셀프서비스"]
    style T1 fill:#dbeafe,stroke:#2563eb
    style T2 fill:#e0e7ff,stroke:#4338ca
    style T3 fill:#f3f4f6,stroke:#6b7280`;
    try {
      mermaid.render("sa-mermaid-squad-svg", mermaidDef).then(({svg}) => { squadDiv.innerHTML = svg; });
    } catch(e) { squadDiv.innerHTML = `<pre style="font-size:11px;text-align:left">${mermaidDef}</pre>`; }
  }

  // 분석 프로세스 플로우
  const flowDiv = document.getElementById("sa-mermaid-flow");
  if (flowDiv) {
    const flowDef = `graph LR
    U["xlsx 업로드"] --> P["Python 엔진<br/>RFM/ABC/CLV/예측"]
    P --> C1["Phase 1<br/>4개 에이전트 병렬"]
    C1 --> C2["Phase 2<br/>전략+파트너십<br/>연동 분석"]
    C2 --> D["KPI 대시보드<br/>+ 전략 리포트"]
    style P fill:#306998,color:#fff
    style C1 fill:#d97706,color:#fff
    style C2 fill:#dc2626,color:#fff
    style D fill:#10b981,color:#fff`;
    try {
      mermaid.render("sa-mermaid-flow-svg", flowDef).then(({svg}) => { flowDiv.innerHTML = svg; });
    } catch(e) { flowDiv.innerHTML = `<pre style="font-size:11px;text-align:left">${flowDef}</pre>`; }
  }
}

function _saDestroyCharts() {
  Object.values(_saCharts).forEach(c => { try { c.destroy(); } catch(e){} });
  _saCharts = {};
}

function saRenderReports(result) {
  document.getElementById("sa-no-report").style.display = "none";
  document.getElementById("sa-report-content").style.display = "block";
  saShowReport("customer");
}

function saShowReport(agentKey) {
  document.querySelectorAll(".sa-report-tab").forEach(el => el.classList.remove("active"));
  event?.target?.classList?.add("active");

  const detail = document.getElementById("sa-report-detail");

  // 종합 리포트
  if (agentKey === "summary") {
    detail.innerHTML = _saRenderSummaryReport();
    return;
  }

  const agents = _saCurrentResult?.agents || {};
  const agent = agents[agentKey];
  if (!agent) {
    detail.innerHTML = '<p style="color:#888">리포트가 없습니다.</p>';
    return;
  }

  const res = agent.result || {};
  let html = `<h3>${agent.icon} ${agent.name} 분석 리포트 <span class="sa-engine-badge claude">Claude AI</span></h3>`;
  html += `<p style="color:#666;font-style:italic;margin-bottom:16px">${_esc(res.summary || "")}</p>`;

  if (agentKey === "customer") html += _saRenderCustomerReport(res);
  else if (agentKey === "product") html += _saRenderProductReport(res);
  else if (agentKey === "strategy") html += _saRenderStrategyReport(res);
  else if (agentKey === "future") html += _saRenderFutureReport(res);
  else if (agentKey === "partnership") html += _saRenderPartnershipReport(res);

  const recs = res.recommendations || [];
  if (recs.length) {
    html += `<h4 style="margin-top:20px">💡 핵심 권고사항</h4>`;
    html += recs.map(r => `<div style="padding:6px 0;font-size:13px">• ${_esc(r)}</div>`).join("");
  }

  detail.innerHTML = html;
}

function _saRenderSummaryReport() {
  if (!_saCurrentResult) return '<p>결과가 없습니다.</p>';
  const r = _saCurrentResult;
  const agents = r.agents || {};
  const engine = r.engine_results || {};
  let html = `<h3>📑 종합 분석 리포트</h3>`;
  html += `<p style="color:#666">분석 기간: ${r.period?.start||""} ~ ${r.period?.end||""} / 소요 시간: ${(r.elapsed_seconds||0).toFixed(1)}초</p>`;

  // 엔진 요약
  const rfm = engine.rfm || {};
  const abc = engine.abc || {};
  const clv = engine.clv || {};
  const trends = engine.trends || {};

  html += `<h4>🔢 Python 엔진 정량 분석 요약</h4>`;
  html += `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px;margin-bottom:16px">`;
  if (rfm.segments) {
    const total = Object.values(rfm.segments).reduce((s,v)=>s+v.count,0);
    const champs = rfm.segments.Champions?.count || 0;
    html += `<div class="sa-kpi-card"><div class="kpi-label">분석 거래처</div><div class="kpi-val" style="font-size:20px">${total}</div><div style="font-size:11px;color:#10b981">Champions ${champs}개</div></div>`;
  }
  if (abc.grade_summary) {
    html += `<div class="sa-kpi-card"><div class="kpi-label">A등급 품목</div><div class="kpi-val" style="font-size:20px">${abc.grade_summary.A?.count||0}</div><div style="font-size:11px;color:#10b981">${abc.grade_summary.A?.pct||0}% 매출 비중</div></div>`;
  }
  if (clv.clv_results) {
    html += `<div class="sa-kpi-card"><div class="kpi-label">Tier 1 거래처</div><div class="kpi-val" style="font-size:20px">${clv.tier_summary?.["Tier 1"]?.count||0}</div></div>`;
  }
  if (rfm.churn_risk) {
    html += `<div class="sa-kpi-card"><div class="kpi-label">이탈 위험</div><div class="kpi-val" style="font-size:20px;color:#ef4444">${rfm.churn_risk.length}</div></div>`;
  }
  html += `</div>`;

  // 성장 시나리오
  if (trends.growth_scenarios) {
    const gs = trends.growth_scenarios;
    html += `<h4>📈 3년 성장 시나리오</h4>`;
    html += `<p style="font-size:13px">현재 연매출 <strong>${gs.current_annual?.toLocaleString()||0}원</strong> → 3년 후 기본 시나리오 <strong>${gs.moderate?.year3?.toLocaleString()||0}원</strong> (연 20% 성장)</p>`;
  }

  // 에이전트별 요약
  html += `<h4 style="margin-top:20px">🤖 에이전트별 AI 분석 핵심</h4>`;
  Object.entries(agents).forEach(([k, ag]) => {
    const summary = ag.result?.summary || "분석 결과 없음";
    html += `<div style="padding:8px 12px;margin:6px 0;background:#f9fafb;border-radius:8px;border-left:3px solid #4A90D9;font-size:13px">
      <strong>${ag.icon} ${ag.name}</strong>: ${_esc(summary)}
    </div>`;
  });

  // 전체 권고사항
  const recs = r.top_recommendations || [];
  if (recs.length) {
    html += `<h4 style="margin-top:20px">💡 핵심 권고사항 (전체)</h4>`;
    html += recs.map((rec, i) => `<div style="padding:4px 0;font-size:13px"><strong>${i+1}.</strong> ${_esc(rec)}</div>`).join("");
  }

  return html;
}

// PDF 내보내기 (브라우저 인쇄 기능 활용)
function saExportPDF() {
  const detail = document.getElementById("sa-report-detail");
  if (!detail || !detail.innerHTML.trim()) { alert("리포트를 먼저 확인해주세요."); return; }

  const win = window.open("", "_blank", "width=900,height=700");
  win.document.write(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>판매 AI 에이전트 리포트</title>
    <style>
      body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:40px;color:#333;line-height:1.6}
      h3{color:#1e40af;border-bottom:2px solid #dbeafe;padding-bottom:8px}
      h4{color:#374151;margin-top:20px}
      table{width:100%;border-collapse:collapse;margin:8px 0;font-size:12px}
      th,td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left}
      th{background:#f9fafb;font-weight:600}
      .sa-kpi-card{display:inline-block;border:1px solid #e5e7eb;border-radius:8px;padding:8px 16px;margin:4px;text-align:center}
      .kpi-val{font-size:18px;font-weight:700}
      .kpi-label{font-size:11px;color:#6b7280}
      @media print{body{padding:20px}}
    </style></head><body>
    <h2>🤖 판매 AI 에이전트 분석 리포트</h2>
    <p style="color:#666;font-size:12px">생성일: ${new Date().toLocaleString("ko-KR")} | 분석 소요: ${(_saCurrentResult?.elapsed_seconds||0).toFixed(1)}초</p>
    <hr>
    ${detail.innerHTML}
  </body></html>`);
  win.document.close();
  setTimeout(() => { win.print(); }, 500);
}

function _saRenderCustomerReport(res) {
  let html = "";
  // 세그먼트 전략 (v2)
  const segStrategies = res.segment_strategies || res.strategies || [];
  if (segStrategies.length) {
    html += `<h4>🎯 세그먼트별 전략</h4>`;
    segStrategies.forEach(s => {
      html += `<div style="margin:8px 0;padding:12px;background:#f5f5f5;border-radius:8px;font-size:13px;border-left:3px solid #4A90D9">
        <strong>${_esc(s.segment)}</strong> ${s.count?`(${s.count}개 거래처)`:""}: ${_esc(s.strategy||"")}
        ${s.kpi ? `<div style="font-size:11px;color:#666;margin-top:4px">KPI: ${_esc(s.kpi)}</div>` : ""}
        ${(s.actions||[]).length ? `<ul style="margin:4px 0 0;padding-left:20px">${s.actions.map(a=>`<li>${_esc(a)}</li>`).join("")}</ul>` : ""}
      </div>`;
    });
  }
  // 이탈 위험 액션 (v2)
  const churnActions = res.churn_actions || res.churn_risk || [];
  if (churnActions.length) {
    html += `<h4 style="margin-top:16px">⚠️ 이탈 위험 대응</h4>`;
    html += churnActions.map(c => `<div style="padding:8px 10px;margin:4px 0;background:${c.risk_level==="높음"?"#fff0f0":"#fffaf0"};border-radius:6px;font-size:13px">
      <strong>${_esc(c.customer_name)}</strong> <span class="sa-risk-badge sa-risk-${c.risk_level||'주의'}">${_esc(c.risk_level||"")}</span>
      ${c.action_plan ? `<div style="margin-top:4px">${_esc(c.action_plan)}</div>` : c.reason ? `<div style="margin-top:4px">${_esc(c.reason)}</div>` : ""}
      ${c.timeline ? `<div style="font-size:11px;color:#888">기한: ${_esc(c.timeline)}</div>` : ""}
    </div>`).join("");
  }
  // 기업학적 인사이트 (v2)
  const firmographic = res.firmographic_insights || [];
  if (firmographic.length) {
    html += `<h4 style="margin-top:16px">🏢 기업학적 세분화 인사이트</h4>`;
    firmographic.forEach(f => {
      html += `<div style="margin:6px 0;padding:10px;background:#f0f8ff;border-radius:8px;font-size:13px">
        <strong>${_esc(f.dimension||"")}</strong>: ${_esc(f.insight||"")}
        ${f.recommendation ? `<div style="color:#4A90D9;margin-top:4px">→ ${_esc(f.recommendation)}</div>` : ""}
      </div>`;
    });
  }
  // 포트폴리오 건강도 (v2)
  const health = res.portfolio_health;
  if (health) {
    const score = health.score || 0;
    const color = score >= 70 ? "#10b981" : score >= 50 ? "#f59e0b" : "#ef4444";
    html += `<h4 style="margin-top:16px">❤️ 고객 포트폴리오 건강도</h4>`;
    html += `<div style="display:flex;align-items:center;gap:16px;padding:12px;background:#f9fafb;border-radius:8px">
      <div style="width:60px;height:60px;border-radius:50%;border:4px solid ${color};display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:${color}">${score}</div>
      <div><div style="font-size:14px;font-weight:600">${_esc(health.assessment||"")}</div>
      ${(health.improvement_areas||[]).length ? `<div style="font-size:12px;color:#666;margin-top:4px">개선 영역: ${health.improvement_areas.map(a=>_esc(a)).join(", ")}</div>` : ""}
      </div>
    </div>`;
  }
  return html;
}

function _saRenderProductReport(res) {
  let html = "";
  // 등급별 관리 전략 (v2)
  const gradeStrategies = res.grade_strategies || [];
  if (gradeStrategies.length) {
    html += `<h4>📊 등급별 관리 전략</h4>`;
    gradeStrategies.forEach(gs => {
      const gColor = {A:"#10b981",B:"#f59e0b",C:"#6b7280"}[gs.grade] || "#999";
      html += `<div style="margin:8px 0;padding:12px;background:#f9fafb;border-radius:8px;border-left:4px solid ${gColor};font-size:13px">
        <strong><span class="sa-grade-${gs.grade}" style="padding:2px 8px;border-radius:8px;font-size:11px">${gs.grade}</span> ${_esc(gs.strategy||"")}</strong>
        <div style="margin-top:4px;font-size:12px;color:#666">점검 주기: ${_esc(gs.check_cycle||"")} / 재고 모델: ${_esc(gs.inventory_model||"")}</div>
        ${(gs.actions||[]).length ? `<ul style="margin:4px 0 0;padding-left:20px;font-size:12px">${gs.actions.map(a=>`<li>${_esc(a)}</li>`).join("")}</ul>` : ""}
      </div>`;
    });
  }
  // 예측 인사이트 (v2)
  const insights = res.forecast_insights || [];
  if (insights.length) {
    html += `<h4 style="margin-top:16px">📈 수요 예측 인사이트</h4>`;
    html += `<table class="table" style="font-size:13px"><thead><tr><th>품목</th><th>추세</th><th>해석</th><th>대응</th></tr></thead><tbody>`;
    insights.slice(0,10).forEach(f => {
      const icon = f.trend==="증가"?"📈":f.trend==="감소"?"📉":"➡️";
      html += `<tr><td>${_esc(f.product_name||"")}</td><td>${icon} ${_esc(f.trend||"")}</td><td style="font-size:12px">${_esc(f.interpretation||"")}</td><td style="font-size:12px">${_esc(f.action||"")}</td></tr>`;
    });
    html += `</tbody></table>`;
  }
  // 재고 권고 (v2)
  const invRecs = res.inventory_recommendations || [];
  if (invRecs.length) {
    html += `<h4 style="margin-top:16px">📦 재고 관리 권고</h4>`;
    invRecs.forEach(r => {
      html += `<div style="margin:6px 0;padding:10px;background:#f0f8ff;border-radius:8px;font-size:13px">
        <strong>${_esc(r.product_name||"")}</strong>: ${_esc(r.current_issue||"")} → ${_esc(r.recommendation||"")}
        ${r.expected_benefit ? `<div style="color:#10b981;font-size:12px;margin-top:2px">기대효과: ${_esc(r.expected_benefit)}</div>` : ""}
      </div>`;
    });
  }
  return html;
}

function _saRenderStrategyReport(res) {
  let html = "";
  // 거래처별 전략 (v2: segment 포함)
  const strategies = res.customer_strategies || [];
  if (strategies.length) {
    html += `<h4>🎯 거래처별 맞춤 전략</h4>`;
    html += `<table class="table" style="font-size:12px"><thead><tr><th>거래처</th><th>등급</th><th>세그먼트</th><th>전략</th><th>성장 기대</th></tr></thead><tbody>`;
    strategies.slice(0, 15).forEach(s => {
      html += `<tr><td><strong>${_esc(s.customer_name)}</strong></td><td>${_esc(s.tier||"")}</td><td>${_esc(s.segment||"")}</td><td style="font-size:11px">${_esc(s.strategy||"")}</td><td>${_esc(s.expected_growth||"")}</td></tr>`;
    });
    html += `</tbody></table>`;
  }
  // 교차판매 (v2)
  const cross = res.cross_sell || [];
  if (cross.length) {
    html += `<h4 style="margin-top:16px">🔄 교차판매 기회</h4>`;
    cross.slice(0, 10).forEach(c => {
      html += `<div style="margin:8px 0;padding:10px;background:#f0f8ff;border-radius:8px;font-size:13px">
        <strong>${_esc(c.customer_name)}</strong>: ${(c.current_products||[]).map(p=>_esc(p)).join(", ")} → <strong style="color:#4A90D9">${(c.recommended||[]).map(p=>_esc(p)).join(", ")}</strong>
        ${c.expected_revenue ? ` (예상 ${c.expected_revenue.toLocaleString()}원)` : ""}<br>
        <span style="font-size:12px;color:#666">${_esc(c.reason||"")}</span>
      </div>`;
    });
  }
  // HaaS 후보 (v2: roi_for_customer 포함)
  const haas = res.haas_candidates || [];
  if (haas.length) {
    html += `<h4 style="margin-top:16px">📋 HaaS 전환 후보</h4>`;
    haas.forEach(h => {
      html += `<div style="margin:6px 0;padding:10px;font-size:13px;background:#f5f0ff;border-radius:8px">
        <strong>${_esc(h.customer_name)}</strong> — 월 ${(h.monthly_subscription||0).toLocaleString()}원 / 연 MRR ${(h.annual_mrr||0).toLocaleString()}원
        <div style="font-size:12px;color:#666">${_esc(h.reason||"")}${h.roi_for_customer ? ` | 고객 ROI: ${_esc(h.roi_for_customer)}` : ""}</div>
      </div>`;
    });
  }
  // 파이프라인 (v2)
  const pipe = res.pipeline;
  if (pipe) {
    html += `<h4 style="margin-top:16px">📊 파이프라인 분석</h4>`;
    html += `<div style="padding:12px;background:#f9fafb;border-radius:8px;font-size:13px">
      총 매출: ${(pipe.total_revenue||0).toLocaleString()}원 / Top3 집중도: ${((pipe.top3_concentration||0)*100).toFixed(1)}%
      / 트렌드: ${_esc(pipe.monthly_trend||"")}
      ${pipe.velocity_assessment ? `<div style="margin-top:4px">${_esc(pipe.velocity_assessment)}</div>` : ""}
      ${(pipe.risk_factors||[]).length ? `<div style="margin-top:4px;color:#ef4444">리스크: ${pipe.risk_factors.map(r=>_esc(r)).join(", ")}</div>` : ""}
    </div>`;
  }
  return html;
}

function _saRenderFutureReport(res) {
  let html = "";
  // 트렌드별 전략 (v2)
  const trendStrategies = res.trend_strategies || [];
  if (trendStrategies.length) {
    html += `<h4>🚀 메가트렌드별 영업 전략</h4>`;
    trendStrategies.forEach(t => {
      const lvlColor = {높음:"#10b981",보통:"#f59e0b",낮음:"#6b7280"}[t.opportunity_level] || "#999";
      html += `<div style="margin:8px 0;padding:12px;background:#f0fff4;border-radius:8px;border-left:4px solid ${lvlColor};font-size:13px">
        <strong>${_esc(t.trend||"")}</strong> <span style="color:${lvlColor};font-size:11px;font-weight:600">[기회: ${_esc(t.opportunity_level||"")}]</span>
        <div style="margin-top:4px">${_esc(t.strategy||"")}</div>
        ${(t.target_customers||[]).length ? `<div style="font-size:12px;color:#666;margin-top:4px">타겟 거래처: ${t.target_customers.map(c=>_esc(c)).join(", ")}</div>` : ""}
        ${(t.actions||[]).length ? `<ul style="margin:4px 0 0;padding-left:20px;font-size:12px">${t.actions.map(a=>`<li>${_esc(a)}</li>`).join("")}</ul>` : ""}
        ${t.timeline ? `<div style="font-size:11px;color:#888;margin-top:4px">기한: ${_esc(t.timeline)}</div>` : ""}
      </div>`;
    });
  }
  // 신규 시장 진입 (v2)
  const market = res.market_entry || [];
  if (market.length) {
    html += `<h4 style="margin-top:16px">🌟 신규 시장 진입 전략</h4>`;
    html += `<table class="table" style="font-size:12px"><thead><tr><th>시장</th><th>진입 전략</th><th>필요 역량</th><th>예상 기간</th><th>예상 매출</th></tr></thead><tbody>`;
    market.forEach(m => {
      html += `<tr><td><strong>${_esc(m.market||"")}</strong></td><td style="font-size:11px">${_esc(m.entry_strategy||"")}</td>
        <td style="font-size:11px">${(m.required_capabilities||[]).map(c=>_esc(c)).join(", ")}</td>
        <td>${_esc(m.expected_timeline||"")}</td><td>${m.estimated_revenue ? m.estimated_revenue.toLocaleString()+"원" : "-"}</td></tr>`;
    });
    html += `</tbody></table>`;
  }
  // 시나리오별 실행 전략 (v2)
  const scenarios = res.scenario_strategies || {};
  if (Object.keys(scenarios).length) {
    html += `<h4 style="margin-top:16px">📈 시나리오별 실행 전략</h4>`;
    const labels = {conservative:"🔵 보수적",moderate:"🟢 기본",aggressive:"🔴 공격적"};
    const colors = {conservative:"#3b82f6",moderate:"#10b981",aggressive:"#ef4444"};
    for (const [key, label] of Object.entries(labels)) {
      const s = scenarios[key];
      if (!s) continue;
      html += `<div style="margin:8px 0;padding:12px;background:#f9fafb;border-radius:8px;border-left:4px solid ${colors[key]};font-size:13px">
        <strong>${label}</strong>: ${_esc(s.focus||"")}
        ${(s.key_actions||[]).length ? `<ul style="margin:4px 0 0;padding-left:20px;font-size:12px">${s.key_actions.map(a=>`<li>${_esc(a)}</li>`).join("")}</ul>` : ""}
      </div>`;
    }
  }
  // 역량 강화 로드맵 (v2)
  const roadmap = res.capability_roadmap || [];
  if (roadmap.length) {
    html += `<h4 style="margin-top:16px">🗓️ 기술 역량 강화 로드맵</h4>`;
    html += `<div style="display:flex;gap:8px;flex-wrap:wrap">`;
    roadmap.forEach(r => {
      html += `<div style="flex:1;min-width:180px;padding:12px;background:#f0f8ff;border-radius:8px;font-size:13px;border-top:3px solid #4A90D9">
        <div style="font-weight:700;color:#4A90D9;margin-bottom:4px">${_esc(r.quarter||"")}</div>
        <div style="font-weight:600;margin-bottom:4px">${_esc(r.focus_area||"")}</div>
        ${(r.actions||[]).length ? `<ul style="margin:0;padding-left:16px;font-size:11px">${r.actions.map(a=>`<li>${_esc(a)}</li>`).join("")}</ul>` : ""}
      </div>`;
    });
    html += `</div>`;
  }
  return html;
}

function _saRenderPartnershipReport(res) {
  let html = "";
  // 스쿼드 편성 (v2)
  const squads = res.squad_formation || [];
  if (squads.length) {
    html += `<h4>👥 어카운트 스쿼드 편성</h4>`;
    const tierColors = {"Tier 1":"#10b981","Tier 2":"#f59e0b","Tier 3":"#6b7280"};
    squads.forEach(sq => {
      html += `<div style="margin:8px 0;padding:12px;background:#f9fafb;border-radius:8px;border-left:4px solid ${tierColors[sq.tier]||"#ccc"};font-size:13px">
        <strong style="color:${tierColors[sq.tier]||"#333"}">${_esc(sq.tier||"")}</strong> (${sq.count||0}개 거래처) — <span style="color:#4A90D9">${_esc(sq.structure||"")}</span>
        ${(sq.customers||[]).length ? `<div style="font-size:12px;color:#666;margin-top:4px">대상: ${sq.customers.map(c=>_esc(c)).join(", ")}</div>` : ""}
      </div>`;
    });
  }
  // ABM 타겟 (v2: CLV, 성장잠재력, 전략적 가치 포함)
  const abm = res.abm_targets || [];
  if (abm.length) {
    html += `<h4 style="margin-top:16px">🎯 ABM 2.0 타겟 (Top ${abm.length})</h4>`;
    html += `<table class="table" style="font-size:12px"><thead><tr><th>#</th><th>거래처</th><th>점수</th><th>CLV</th><th>성장잠재력</th><th>전략적 가치</th><th>사유</th></tr></thead><tbody>`;
    abm.slice(0, 15).forEach(a => {
      const gpColor = {높음:"#10b981",보통:"#f59e0b",낮음:"#6b7280"}[a.growth_potential] || "#999";
      html += `<tr><td>${a.rank||""}</td><td><strong>${_esc(a.customer_name||"")}</strong></td><td>${a.score||0}</td>
        <td>${a.clv ? a.clv.toLocaleString()+"원" : "-"}</td>
        <td><span style="color:${gpColor};font-weight:600">${_esc(a.growth_potential||"")}</span></td>
        <td style="font-size:11px">${_esc(a.strategic_value||"")}</td>
        <td style="font-size:11px">${_esc(a.reason||"")}</td></tr>`;
    });
    html += `</tbody></table>`;
  }
  // 관계 강화 프로그램 (v2: timeline 포함)
  const programs = res.relationship_programs || [];
  if (programs.length) {
    html += `<h4 style="margin-top:16px">🤝 관계 강화 프로그램</h4>`;
    programs.slice(0, 10).forEach(p => {
      html += `<div style="margin:6px 0;padding:10px;background:#f5f0ff;border-radius:8px;font-size:13px">
        <strong>${_esc(p.customer_name||"")}</strong>: <span style="color:#6b7280">${_esc(p.current_level||"")}</span> → <span style="color:#4A90D9;font-weight:600">${_esc(p.target_level||"")}</span>
        ${p.timeline ? `<span style="font-size:11px;color:#888;margin-left:8px">기한: ${_esc(p.timeline)}</span>` : ""}
        ${(p.actions||[]).length ? `<ul style="margin:4px 0 0;padding-left:18px;font-size:12px">${p.actions.map(a=>`<li>${_esc(a)}</li>`).join("")}</ul>` : ""}
      </div>`;
    });
  }
  // 디지털 마케팅 (v2)
  const marketing = res.digital_marketing || [];
  if (marketing.length) {
    html += `<h4 style="margin-top:16px">📢 디지털 마케팅 전략</h4>`;
    html += `<table class="table" style="font-size:12px"><thead><tr><th>프로그램</th><th>대상</th><th>빈도</th><th>기대 효과</th></tr></thead><tbody>`;
    marketing.forEach(m => {
      html += `<tr><td><strong>${_esc(m.program||"")}</strong></td><td>${_esc(m.target_audience||"")}</td>
        <td>${_esc(m.frequency||"")}</td><td style="font-size:11px">${_esc(m.expected_outcome||"")}</td></tr>`;
    });
    html += `</tbody></table>`;
  }
  return html;
}

async function saShowHistory() {
  const panel = document.getElementById("sa-history-panel");
  panel.style.display = panel.style.display === "none" ? "block" : "none";
  if (panel.style.display === "none") return;

  try {
    const res = await api.get("/api/sales-agent/history?size=20");
    const jobs = res.jobs || [];
    if (!jobs.length) {
      document.getElementById("sa-history-list").innerHTML = '<p style="color:#888">분석 이력이 없습니다.</p>';
      return;
    }
    document.getElementById("sa-history-list").innerHTML = `
      <table class="table" style="font-size:13px">
        <thead><tr><th>분석 ID</th><th>파일명</th><th>상태</th><th>거래건수</th><th>소요시간</th><th>일시</th><th></th></tr></thead>
        <tbody>${jobs.map(j => `<tr>
          <td><code style="font-size:11px">${_esc(j.job_id)}</code></td>
          <td>${_esc(j.file_name||"")}</td>
          <td>${j.status==="completed"?"✅":"⏳"} ${_esc(j.status)}</td>
          <td>${(j.total_rows||0).toLocaleString()}</td>
          <td>${(j.elapsed_seconds||0).toFixed(1)}초</td>
          <td style="font-size:11px">${_esc((j.created_at||"").slice(0,16))}</td>
          <td>${j.status==="completed"?`<button class="btn btn-sm" onclick="saLoadFromHistory('${_esc(j.job_id)}')">보기</button>`:""}</td>
        </tr>`).join("")}</tbody>
      </table>`;
  } catch (e) {
    document.getElementById("sa-history-list").innerHTML = `<p style="color:#ef4444">이력 조회 실패: ${_esc(e.message||"")}</p>`;
  }
}

async function saLoadFromHistory(jobId) {
  _saCurrentJobId = jobId;
  await saLoadResult(jobId);
  saSwitchTab("dashboard");
}
