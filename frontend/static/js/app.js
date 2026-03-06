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
    settings:     "설정",
  }[pageId] || "";
  // 주문서 페이지 진입 시 드롭존 초기화
  if (pageId === "sale_order") initSODropzone();
  // 자료검색 페이지 진입 시 카테고리 로드
  if (pageId === "doc_search") initDocSearchPage().catch(e => console.error("initDocSearchPage 실패:", e));
  // 단가표 조회 페이지 진입 시 거래처 로드
  if (pageId === "price_sheet") initPriceSheetPage().catch(e => console.error("initPriceSheetPage 실패:", e));
  // 발주서 학습 페이지 진입 시 데이터 로드
  if (pageId === "training") initTrainingPage().catch(e => console.error("initTrainingPage 실패:", e));
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
      const res = await fetch(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const data = await res.json();
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
      const res = await fetch(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const data = await res.json();
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
      const res = await fetch(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const data = await res.json();
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
      const data = await api.customerSearch(q);
      const list = data.results || [];
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

async function syncOrderList() {
  const btn = document.getElementById("ol-sync-btn");
  btn.disabled = true;
  btn.textContent = "동기화 중...";
  try {
    const res = await fetch("/api/orderlist/sync", { method: "POST" });
    const data = await res.json();
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
    const res = await fetch("/api/orderlist/tabs");
    const tabs = await res.json();
    const sel = document.getElementById("ol-tab-select");
    sel.innerHTML = '<option value="">전체</option>';
    tabs.forEach(t => {
      sel.innerHTML += `<option value="${t.sheet_tab}">${t.sheet_tab} (${t.item_count}건)</option>`;
    });
  } catch (e) { /* ignore */ }
}

let _olSearchTimer = null;
function onOlSearchInput() {
  clearTimeout(_olSearchTimer);
  _olSearchTimer = setTimeout(() => loadOrderList(1), 300);
}

async function loadOrderList(page = 1) {
  olCurrentPage = page;
  const tab = document.getElementById("ol-tab-select").value;
  const query = document.getElementById("ol-search-input").value.trim();
  const container = document.getElementById("ol-results");
  const pagination = document.getElementById("ol-pagination");
  const summary = document.getElementById("ol-summary");

  container.innerHTML = '<p style="text-align:center;padding:20px;color:var(--gray-400)">로딩 중...</p>';

  try {
    const params = new URLSearchParams({ page, page_size: 50 });
    if (tab) params.set("tab", tab);
    if (query) params.set("query", query);

    const res = await fetch(`/api/orderlist/data?${params}`);
    const data = await res.json();

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
