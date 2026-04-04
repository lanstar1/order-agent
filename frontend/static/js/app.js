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
  initDropdownKeyNav("po-cust-search", "po-cust-dropdown", "selectPOCust");
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

// ─── 사이드바 그룹 토글 ───
function toggleNavGroup(headerEl) {
  const group = headerEl.closest('.nav-group');
  if (group) group.classList.toggle('open');
}

function navigateTo(pageId) {
  // AI 메일 에이전트는 외부 링크로 열기 (내부 페이지 전환 안 함)
  if (pageId === "mail_agent") {
    window.open("https://mail-dqxh.onrender.com", "_blank");
    return;
  }
  // 스마트스토어는 별도 페이지로 열기
  if (pageId === "smartstore_bridge") {
    window.open("/smartstore", "_blank");
    return;
  }
  // 바코드 ERP Bridge는 별도 페이지로 열기
  if (pageId === "barcode_bridge") {
    window.open("/barcode", "_blank");
    return;
  }
  // 일별 판매현황은 별도 페이지로 열기
  if (pageId === "sales_daily") {
    window.open("/sales-daily", "_blank");
    return;
  }
  document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach(n => n.classList.remove("active"));
  const page = document.getElementById("page-" + pageId);
  if (page) page.classList.add("active");
  const nav = document.querySelector(`[data-page="${pageId}"]`);
  if (nav) {
    nav.classList.add("active");
    // 해당 nav-item이 속한 그룹을 자동으로 열기
    const parentGroup = nav.closest('.nav-group');
    if (parentGroup && !parentGroup.classList.contains('open')) {
      parentGroup.classList.add('open');
    }
  }
  document.getElementById("topbar-title").textContent = {
    dashboard:    "대시보드",
    new_order:    "판매입력",
    history:      "처리 이력",
    sale_order:   "견적서입력",
    so_result:    "견적서 결과",
    so_history:   "견적서 이력",
    purchase:     "구매입력",
    po_result:    "구매입력 결과",
    po_history:   "구매입력 이력",
    inventory:    "재고 조회",
    doc_search:   "자료검색",
    price_sheet:  "단가표 조회",
    training:     "발주서 학습",
    shipping:     "택배조회",
    inventory_monitor: "재고모니터",
    cs_rma:       "CS/RMA",
    aicc:         "AI 상담",
    ai_dashboard: "AI 대시보드",
    settings:     "설정",
    reconcile:    "매입정산",
  }[pageId] || "";
  // AI 상담 페이지 진입 시 초기화
  if (pageId === "aicc") initAiccTab();
  // CS/RMA 페이지 진입 시 초기화
  if (pageId === "cs_rma") csInit();
  // 재고모니터 페이지 진입 시 초기화
  if (pageId === "inventory_monitor") initInventoryMonitor();
  // 택배조회 페이지 진입 시 통계 로드
  if (pageId === "shipping") initShippingPage();
  // 주문서 페이지 진입 시 드롭존 초기화
  if (pageId === "sale_order") initSODropzone();
  // 구매입력 페이지 진입 시 드롭존 초기화
  if (pageId === "purchase") initPODropzone();
  // 매입정산 페이지 진입 시 초기화
  if (pageId === "reconcile") initReconcilePage();
  // 자료검색 페이지 진입 시 카테고리 로드
  if (pageId === "doc_search") initDocSearchPage().catch(e => console.error("initDocSearchPage 실패:", e));
  // 단가표 조회 페이지 진입 시 거래처 로드
  if (pageId === "price_sheet") initPriceSheetPage().catch(e => console.error("initPriceSheetPage 실패:", e));
  // 발주서 학습 페이지 진입 시 데이터 로드
  if (pageId === "training") initTrainingPage().catch(e => console.error("initTrainingPage 실패:", e));
  // AI 대시보드 진입 시 데이터 로드
  if (pageId === "ai_dashboard" && typeof loadDashboard === "function") loadDashboard();
  // 설정 페이지 진입 시 관리자 인증 확인
  if (pageId === "settings" && typeof showAdminOverlay === "function") showAdminOverlay();
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

// ─── 사이드바 네비게이션 (그룹 + 클릭) ───
function initSidebarNav() {
  const nav = document.getElementById("sidebar-nav");
  if (!nav) return;

  // localStorage에서 그룹 열림/닫힘 상태 복원
  const savedGroups = localStorage.getItem("navGroupState");
  if (savedGroups) {
    try {
      const groupState = JSON.parse(savedGroups);
      nav.querySelectorAll(".nav-group").forEach(group => {
        const gid = group.dataset.group;
        if (groupState[gid] !== undefined) {
          group.classList.toggle("open", groupState[gid]);
        }
      });
    } catch (e) { /* 무시 */ }
  }

  // 클릭 이벤트
  nav.querySelectorAll(".nav-item").forEach(item => {
    item.addEventListener("click", (e) => {
      const page = item.dataset.page;
      navigateTo(page);
      if (page === "history") loadHistory();
    });
  });

  // 그룹 토글 시 상태 저장
  nav.querySelectorAll(".nav-group-header").forEach(header => {
    header.addEventListener("click", () => {
      setTimeout(() => {
        const state = {};
        nav.querySelectorAll(".nav-group").forEach(g => {
          state[g.dataset.group] = g.classList.contains("open");
        });
        localStorage.setItem("navGroupState", JSON.stringify(state));
      }, 50);
    });
  });
}

// ─── 초기화 ───
document.addEventListener("DOMContentLoaded", () => {
  // 네비게이션 (클릭 + 드래그 정렬)
  initSidebarNav();

  // 탭 버튼 (data-tab 속성이 있는 판매입력 탭만 처리)
  document.querySelectorAll(".tab-btn[data-tab]").forEach(btn => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // 초기 데이터 로드
  loadCustomers();
  navigateTo("ai_dashboard");

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


// ═══════════════════════════════════════════════════════
//  구매입력 (Purchase Input)
// ═══════════════════════════════════════════════════════

const poState = {
  currentOrder: null,
  currentTab: "text",
};

// ─── 거래처 검색 (구매입력용) ───
let _poCustSearchTimer = null;
async function onPOCustSearch(query) {
  const dd = document.getElementById("po-cust-dropdown");
  if (!query.trim()) { dd.style.display = "none"; return; }
  clearTimeout(_poCustSearchTimer);
  _poCustSearchTimer = setTimeout(async () => {
    try {
      const data = await api.get(`/api/customers/?q=${encodeURIComponent(query.trim())}`);
      const matches = data.customers || [];
      if (!matches.length) {
        dd.innerHTML = `<div style="padding:10px 14px;color:#a0aec0;font-size:13px">검색 결과 없음</div>`;
      } else {
        dd.innerHTML = matches.map(c =>
          `<div onclick="selectPOCust('${c.cust_code.replace(/'/g,"\\'")}','${c.cust_name.replace(/'/g,"\\'")}');"
            style="padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid #f7fafc"
            onmouseover="this.style.background='#ebf8ff'" onmouseout="this.style.background=''">
            <strong>${c.cust_name}</strong>
            <span style="color:#a0aec0;margin-left:6px;font-size:12px">${c.cust_code}</span>
          </div>`
        ).join("");
      }
      dd.style.display = "block";
      if (_dropdownNav["po-cust-search"]) _dropdownNav["po-cust-search"].idx = -1;
    } catch(e) { console.warn("거래처 검색 오류:", e); }
  }, 200);
}

function showPOCustDropdown() {
  const q = document.getElementById("po-cust-search").value.trim();
  if (q) onPOCustSearch(q);
}

function selectPOCust(code, name) {
  document.getElementById("po-cust-select").value = code;
  document.getElementById("po-cust-search").value = `${name} (${code})`;
  document.getElementById("po-cust-dropdown").style.display = "none";
  const info = document.getElementById("po-cust-selected-info");
  info.textContent = `✓ 선택됨: ${name} [${code}]`;
  info.style.display = "block";
}

document.addEventListener("click", e => {
  const dd = document.getElementById("po-cust-dropdown");
  if (dd && !dd.contains(e.target) && e.target.id !== "po-cust-search") {
    dd.style.display = "none";
  }
});

// ─── 탭 전환 ───
function switchPOTab(tab) {
  poState.currentTab = tab;
  document.querySelectorAll("[data-po-tab]").forEach(b => b.classList.remove("active"));
  const btn = document.querySelector(`[data-po-tab="${tab}"]`);
  if (btn) btn.classList.add("active");
  document.getElementById("po-tab-text").style.display  = tab === "text"  ? "block" : "none";
  document.getElementById("po-tab-image").style.display = tab === "image" ? "block" : "none";
}

// ─── 드롭존 초기화 ───
let _poDropzoneInitialized = false;
function initPODropzone() {
  if (_poDropzoneInitialized) return;
  const dropzone = document.getElementById("po-image-dropzone");
  if (!dropzone) return;
  // dragover/drop 이벤트만 처리 (click은 HTML onclick으로 직접 처리)
  dropzone.addEventListener("dragover", e => { e.preventDefault(); dropzone.classList.add("drag-over"); });
  dropzone.addEventListener("dragleave", () => dropzone.classList.remove("drag-over"));
  dropzone.addEventListener("drop", e => {
    e.preventDefault();
    dropzone.classList.remove("drag-over");
    const file = e.dataTransfer.files[0];
    if (file) handlePOImageFile(file);
  });
  _poDropzoneInitialized = true;
}

// ─── 파일 처리 (이미지/PDF/엑셀/CSV) ───
function handlePOImageFile(file) {
  if (!file) return;
  const name = file.name.toLowerCase();
  const isExcel = name.endsWith(".xlsx") || name.endsWith(".xls");
  const isCSV   = name.endsWith(".csv");
  const isPDF   = file.type === "application/pdf" || name.endsWith(".pdf");
  const isImage = file.type.startsWith("image/") || name.match(/\.(jpg|jpeg|png|gif|webp)$/i);

  if (!isExcel && !isCSV && !isPDF && !isImage) {
    toast("지원 형식: 이미지(JPG/PNG), PDF, 엑셀(XLSX/XLS), CSV", "error");
    return;
  }
  if (file.size > 10 * 1024 * 1024) {
    toast("파일 크기가 10MB를 초과합니다.", "error");
    return;
  }

  const dropzone = document.getElementById("po-image-dropzone");

  // 파일 종류별 미리보기
  let previewHtml = "";
  if (isExcel || isCSV) {
    const icon = isCSV ? "📊" : "📗";
    const label = isCSV ? "CSV" : "Excel";
    previewHtml = `
      <div style="font-size:40px">${icon}</div>
      <p style="font-weight:600;margin:6px 0 2px">${escapeHtml(file.name)}</p>
      <p style="font-size:12px;color:var(--gray-400)">${(file.size / 1024).toFixed(0)} KB · ${label}</p>
      <button class="btn btn-outline btn-sm" style="margin-top:8px" onclick="clearPOImageFile()">✕ 다시 선택</button>`;
  } else if (isPDF) {
    previewHtml = `
      <div style="font-size:40px">📄</div>
      <p style="font-weight:600;margin:6px 0 2px">${escapeHtml(file.name)}</p>
      <p style="font-size:12px;color:var(--gray-400)">${(file.size / 1024).toFixed(0)} KB · PDF</p>
      <button class="btn btn-outline btn-sm" style="margin-top:8px" onclick="clearPOImageFile()">✕ 다시 선택</button>`;
  } else {
    const url = URL.createObjectURL(file);
    previewHtml = `
      <img src="${url}" style="max-height:160px;max-width:100%;border-radius:6px;object-fit:contain">
      <p style="font-size:12px;color:var(--gray-500);margin-top:6px">${escapeHtml(file.name)} · ${(file.size / 1024).toFixed(0)} KB</p>
      <button class="btn btn-outline btn-sm" style="margin-top:4px" onclick="clearPOImageFile()">✕ 다시 선택</button>`;
  }

  // onclick은 HTML에 직접 있으므로 여기서 다시 달지 않음 (이미 열려있으므로)
  dropzone.style.cursor = "default";
  dropzone.onclick = null; // 파일 선택 후 재클릭 방지
  dropzone.innerHTML = previewHtml;

  const old = document.getElementById("btn-po-analyze-image");
  if (old) old.remove();
  const btn = document.createElement("button");
  btn.id = "btn-po-analyze-image";
  btn.className = "btn btn-primary";
  btn.style.cssText = "margin-top:12px;display:block;width:100%";
  btn.textContent = "🔍 AI 분석 시작 →";
  btn.onclick = () => submitPOImageOrder(file);
  dropzone.after(btn);
}

function clearPOImageFile() {
  const dropzone = document.getElementById("po-image-dropzone");
  dropzone.style.cursor = "pointer";
  dropzone.onclick = () => document.getElementById("po-image-input").click();
  dropzone.innerHTML = `
    <div class="upload-icon">📎</div>
    <p><strong>파일을 드래그하거나 클릭하여 업로드</strong></p>
    <p style="font-size:12px;margin-top:6px;color:var(--gray-400)">이미지·PDF·엑셀·CSV 지원 · 최대 10MB</p>`;
  const btn = document.getElementById("btn-po-analyze-image");
  if (btn) btn.remove();
  document.getElementById("po-image-input").value = "";
}

// ─── 구매서 제출 (텍스트) ───
async function submitPurchase() {
  const custCode = document.getElementById("po-cust-select").value;
  const searchVal = document.getElementById("po-cust-search")?.value || "";
  const custName = searchVal.replace(/\s*\([^)]*\)\s*$/, "").trim() || custCode;
  const rawText  = document.getElementById("po-raw-text").value.trim();

  if (!custCode) { toast("거래처를 선택해주세요.", "error"); return; }
  if (!rawText)  { toast("구매서 내용을 입력해주세요.", "error"); return; }

  showProcessing("구매 라인 추출 중...");

  try {
    updateStep("AI 분석 중...");
    const result = await api.processPurchase({ cust_code: custCode, cust_name: custName, raw_text: rawText });
    poState.currentOrder = result;
    hideProcessing();
    renderPOResult(result);
    navigateTo("po_result");
  } catch (e) {
    hideProcessing();
    toast("처리 실패: " + e.message, "error");
  }
}

// ─── 구매서 이미지 제출 ───
async function submitPOImageOrder(file) {
  const custCode = document.getElementById("po-cust-select").value;
  const searchVal = document.getElementById("po-cust-search")?.value || "";
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

    const result = await api.processPurchaseImage(formData);
    poState.currentOrder = result;
    hideProcessing();
    renderPOResult(result);
    navigateTo("po_result");
    clearPOImageFile();
  } catch (e) {
    hideProcessing();
    toast("OCR 처리 실패: " + e.message, "error");
  }
}

// ─── 구매서 결과 렌더링 ───
function renderPOResult(order) {
  const container = document.getElementById("po-result-container");
  const needsReview = order.lines.some(l => !l.is_confirmed);

  container.innerHTML = `
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <div>
          <div style="font-size:18px;font-weight:700;color:var(--primary)">
            🛒 구매입력 처리 결과
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
            <th style="width:100px">단가</th>
            <th style="width:80px">신뢰도</th>
          </tr>
        </thead>
        <tbody id="po-result-tbody">
        </tbody>
      </table>
    </div>

    <div style="display:flex;gap:12px;justify-content:flex-end">
      <button class="btn btn-outline" onclick="navigateTo('purchase')">← 돌아가기</button>
      <button class="btn btn-success" onclick="confirmAndSubmitPO()" id="btn-po-confirm">
        ✓ 확인 후 ERP 구매현황 전송
      </button>
    </div>
  `;

  const tbody = document.getElementById("po-result-tbody");
  order.lines.forEach(line => {
    const row = document.createElement("tr");
    row.id = `po-line-row-${line.line_no}`;

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
        <select class="candidate-select" id="po-sel-${line.line_no}" onchange="onPOCandidateChange(${line.line_no})">
          ${candidateOptions || '<option value="">-- 매칭 없음 --</option>'}
        </select>
      </td>
      <td id="po-model-${line.line_no}" style="font-size:12px;color:var(--gray-700);font-weight:500;padding:0 6px">
        ${initModel ? `<span title="${initModel}">${initModel}</span>` : '<span style="color:var(--gray-300)">-</span>'}
      </td>
      <td>
        <input type="number" class="form-control" style="padding:5px 8px"
          id="po-qty-${line.line_no}" value="${line.qty || ''}" min="0" step="0.1">
      </td>
      <td>
        <input type="text" class="form-control" style="padding:5px 8px"
          id="po-unit-${line.line_no}" value="${line.unit || ''}" placeholder="EA">
      </td>
      <td>
        <input type="number" class="form-control" style="padding:5px 8px"
          id="po-price-${line.line_no}" value="${line.price || ''}" min="0" placeholder="단가">
      </td>
      <td>
        ${line.candidates[0] ? confidenceBadge(line.candidates[0].confidence) : '<span class="badge badge-low">없음</span>'}
      </td>
    `;
    tbody.appendChild(row);
  });
}

function onPOCandidateChange(lineNo) {
  const row = document.getElementById(`po-line-row-${lineNo}`);
  row.style.background = "#fffbeb";
  setTimeout(() => row.style.background = "", 800);
  const selEl = document.getElementById(`po-sel-${lineNo}`);
  const modelEl = document.getElementById(`po-model-${lineNo}`);
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

// ─── 구매서 확인 후 ERP 전송 ───
async function confirmAndSubmitPO() {
  const order = poState.currentOrder;
  if (!order) return;

  const lines = order.lines.map(line => ({
    line_no:  line.line_no,
    prod_cd:  document.getElementById(`po-sel-${line.line_no}`)?.value || "",
    qty:      parseFloat(document.getElementById(`po-qty-${line.line_no}`)?.value) || 0,
    unit:     document.getElementById(`po-unit-${line.line_no}`)?.value || "",
    price:    parseFloat(document.getElementById(`po-price-${line.line_no}`)?.value) || 0,
  }));

  const invalid = lines.filter(l => !l.prod_cd || !l.qty);
  if (invalid.length > 0) {
    toast(`${invalid.length}개 라인에 상품코드 또는 수량이 없습니다.`, "error");
    return;
  }

  showProcessing("ERP 구매현황 전송 중...");

  try {
    await api.confirmPurchase({ order_id: order.order_id, lines });
    updateStep("구매현황 생성 중...");

    const user = getCurrentUser ? getCurrentUser() : null;
    const empCd = user ? user.emp_cd : "";
    const result = await api.submitPurchaseERP(order.order_id, empCd);
    hideProcessing();

    if (result.success) {
      toast(`ERP 구매현황 전송 완료! 전표번호: ${result.erp_slip_no || "생성됨"}`, "success");
      setTimeout(() => {
        loadPOHistory();
        navigateTo("po_history");
      }, 1500);
    } else {
      toast("ERP 전송 실패: " + result.message, "error");
    }
  } catch (e) {
    hideProcessing();
    toast("오류: " + e.message, "error");
  }
}

// ─── 구매서 이력 로드 ───
async function loadPOHistory() {
  try {
    const res = await api.listPurchases(30);
    const container = document.getElementById("po-history-list");
    if (!res.orders.length) {
      container.innerHTML = '<p style="color:var(--gray-400);text-align:center;padding:32px">처리된 구매입력이 없습니다.</p>';
      return;
    }
    container.innerHTML = res.orders.map(o => `
      <div class="order-item" onclick="viewPOOrder('${o.order_id}')">
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

async function viewPOOrder(orderId) {
  try {
    const res = await api.getPurchase(orderId);
    renderPOOrderDetail(res);
  } catch (e) {
    toast("조회 실패: " + e.message, "error");
  }
}

function renderPOOrderDetail(data) {
  const order = data.order;
  const lines = data.lines || [];
  const submissions = data.submissions || [];
  const container = document.getElementById("po-history-list");

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
          <th style="text-align:right;padding:5px 6px;width:80px">단가</th>
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
            <td style="padding:4px 6px;text-align:right">${l.price ? l.price.toLocaleString() : "-"}</td>
            <td style="padding:4px 6px;text-align:center">${l.is_confirmed ? '<span style="color:var(--success);font-weight:600">확인</span>' : '<span style="color:var(--warning)">미확인</span>'}</td>
          </tr>`;
        }).join("")}
      </tbody>
    </table>
  ` : '<p style="color:var(--gray-400);font-size:12px">라인 데이터 없음</p>';

  const rawText = order.raw_text || "";

  container.innerHTML = `
    <div style="margin-bottom:10px">
      <button class="btn btn-outline btn-sm" onclick="loadPOHistory()">← 목록으로</button>
    </div>

    <div class="card" style="margin-bottom:12px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <div>
          <span style="font-size:15px;font-weight:700;color:var(--primary)">구매입력 상세</span>
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
        <div style="font-size:12px;color:var(--gray-400);margin-bottom:6px;font-weight:600">구매서 원문</div>
        <div style="font-size:12px;color:var(--gray-600);background:#f7fafc;padding:10px 14px;border-radius:6px;white-space:pre-wrap;max-height:120px;overflow-y:auto;border:1px solid var(--gray-100)">${rawText}</div>
      </div>` : ""}
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="card-title">구매 라인 (${lines.length}건)</div>
      ${linesHtml}
    </div>

    <div class="card">
      <div class="card-title">ERP 전송 이력</div>
      ${erpHtml}
    </div>
  `;
}

// ─── 구매 nav 클릭 시 이력 로드 ───
function onPurchaseNavClick() {
  loadPOHistory();
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
    bar.innerHTML = items.map(i => {
      // "전체"와 "오늘 접수"는 전체 필터로, 나머지는 해당 상태 필터로
      const filterStatus = (i.label === "전체" || i.label === "오늘 접수") ? "" : i.label;
      return `<div class="cs-stat-card" style="cursor:pointer" onclick="csStatCardClick('${filterStatus}')"><div class="num" style="color:${i.color}">${i.num}</div><div class="label">${i.label}</div></div>`;
    }).join("");
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

function csStatCardClick(status) {
  // 대시보드 카드 클릭 → 해당 상태의 탭을 찾아서 활성화
  const tab = document.querySelector(`.cs-pipe-tab[data-status="${status}"]`);
  if (tab) csSwitchTab(tab, status);
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
      actionHtml = `<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        <button onclick="csAction('${t.ticket_id}','handover')" class="btn btn-primary" style="font-size:13px">🔬 기술 담당자 인계</button>
        <button onclick="csQuickResolve('${t.ticket_id}')" style="padding:6px 16px;border:1px solid #f59e0b;border-radius:6px;background:#fffbeb;color:#b45309;cursor:pointer;font-size:13px;font-weight:500">↩️ 단순변심 처리종결</button>
      </div>`;
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
            // DB 저장 파일: /api/cs/files/db/{id} 로 서빙
            const imgSrc = f.file_url && f.file_url.startsWith("/api/cs/files/db/")
              ? f.file_url
              : (f.file_url || `/api/cs/files/db/${f.id}`);
            const viewUrl = imgSrc;
            const delBtn = t.current_status !== "처리종결"
              ? `<button onclick="event.stopPropagation();csDeleteFile(${f.id},'${f.ticket_id}')" title="삭제" style="position:absolute;top:-6px;right:-6px;width:20px;height:20px;border-radius:50%;border:1px solid #d1d5db;background:#fff;color:#dc2626;font-size:12px;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;padding:0;z-index:10">✕</button>`
              : "";
            const dlBtn = `<button onclick="event.stopPropagation();csDownloadFile(${f.id},'${_esc(f.file_name)}')" title="다운로드" style="position:absolute;bottom:-6px;right:-6px;width:20px;height:20px;border-radius:50%;border:1px solid #d1d5db;background:#fff;color:#2563eb;font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;padding:0;z-index:10">⬇</button>`;
            if (f.file_type === "image") {
              return `<div style="position:relative;display:inline-block">${delBtn}${dlBtn}<a href="${viewUrl}" target="_blank"><img src="${imgSrc}" style="width:80px;height:80px;object-fit:cover;border-radius:6px;border:1px solid #e5e7eb" onerror="this.style.display='none';this.nextElementSibling.style.display='inline-flex'"><span style="display:none;width:80px;height:80px;align-items:center;justify-content:center;background:#f3f4f6;border-radius:6px;font-size:24px;border:1px solid #e5e7eb">🖼️</span></a></div>`;
            } else if (f.file_type === "video") {
              return `<div style="position:relative;display:inline-block">${delBtn}${dlBtn}<video src="${imgSrc}" style="width:120px;height:80px;object-fit:cover;border-radius:6px;border:1px solid #e5e7eb;cursor:pointer;background:#000" onclick="window.open('${viewUrl}','_blank')" preload="metadata" muted onerror="this.style.display='none';this.nextElementSibling.style.display='inline-flex'"></video><a href="${viewUrl}" target="_blank" style="display:none;align-items:center;gap:4px;padding:6px 12px;background:#f3f4f6;border-radius:6px;font-size:12px;color:#374151;text-decoration:none">🎬 ${_esc(f.file_name)}</a></div>`;
            }
            return `<div style="position:relative;display:inline-block">${delBtn}${dlBtn}<a href="${viewUrl}" target="_blank" style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;background:#f3f4f6;border-radius:6px;font-size:12px;color:#374151;text-decoration:none">📄 ${_esc(f.file_name)}</a></div>`;
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

        <!-- 삭제 버튼 -->
        <div style="margin-top:20px;padding-top:16px;border-top:1px solid #e5e7eb;text-align:right">
          <button onclick="csDeleteTicket('${t.ticket_id}')" style="padding:6px 16px;border:1px solid #dc2626;border-radius:6px;background:#fef2f2;color:#dc2626;cursor:pointer;font-size:13px;font-weight:500">🗑️ 접수 삭제</button>
        </div>
      </div>`;
  } catch(e) {
    modal.innerHTML = `<div style="padding:40px;text-align:center;color:#ef4444">오류: ${e.message || e}</div>`;
  }
}

// ── 티켓 삭제 ──
async function csDeleteTicket(ticketId) {
  if (!confirm(`정말로 ${ticketId} 접수를 삭제하시겠습니까?\n\n관련된 첨부파일, 테스트 결과, 처리 이력이 모두 삭제됩니다.`)) return;
  try {
    const res = await api.delete(`/api/cs/tickets/${ticketId}`);
    alert(res.message || "삭제 완료");
    csCloseModal();
    csLoadTickets();
    csLoadStats();
  } catch(e) {
    alert("삭제 실패: " + (e.message || e));
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

async function csQuickResolve(ticketId) {
  if (!confirm("단순변심으로 즉시 처리종결 하시겠습니까?\n(기술인계/테스트 단계를 건너뜁니다)")) return;
  const memo = document.getElementById("cs-memo-input")?.value.trim() || "단순변심";
  try {
    const res = await api.put(`/api/cs/tickets/${ticketId}/quick-resolve`, { action: "단순변심 반송", memo });
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
  if (file.size > 50 * 1024 * 1024) { alert("파일 크기가 50MB를 초과합니다."); return; }

  // 진행률 오버레이
  const overlay = document.createElement("div");
  overlay.id = "cs-upload-overlay";
  overlay.innerHTML = `<div style="position:fixed;inset:0;background:rgba(0,0,0,0.4);display:flex;align-items:center;justify-content:center;z-index:10000">
    <div style="background:#fff;border-radius:12px;padding:24px 32px;min-width:280px;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.2)">
      <div id="cs-upload-title" style="font-size:14px;font-weight:600;margin-bottom:12px">📎 파일 업로드 중...</div>
      <div style="background:#e5e7eb;border-radius:8px;height:8px;overflow:hidden;margin-bottom:8px">
        <div id="cs-upload-bar" style="background:#2563eb;height:100%;width:0%;transition:width 0.2s;border-radius:8px"></div>
      </div>
      <div id="cs-upload-pct" style="font-size:13px;color:#6b7280">0%</div>
    </div>
  </div>`;
  document.body.appendChild(overlay);

  const formData = new FormData();
  formData.append("file", file);

  try {
    await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", `/api/cs/tickets/${ticketId}/upload`);
      const token = localStorage.getItem("jwt_token");
      if (token) xhr.setRequestHeader("Authorization", `Bearer ${token}`);

      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) {
          const pct = Math.round(e.loaded / e.total * 100);
          const bar = document.getElementById("cs-upload-bar");
          const txt = document.getElementById("cs-upload-pct");
          if (bar) bar.style.width = pct + "%";
          if (txt) txt.textContent = pct + "% (" + (e.loaded / 1024 / 1024).toFixed(1) + "MB / " + (e.total / 1024 / 1024).toFixed(1) + "MB)";
          if (pct >= 100) {
            const title = document.getElementById("cs-upload-title");
            if (title) title.textContent = "⏳ 서버 처리 중...";
          }
        }
      };
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          try { resolve(JSON.parse(xhr.responseText)); } catch(_) { resolve({}); }
        } else {
          reject(new Error(xhr.responseText || `업로드 실패 (${xhr.status})`));
        }
      };
      xhr.onerror = () => reject(new Error("네트워크 오류"));
      xhr.timeout = 300000;
      xhr.ontimeout = () => reject(new Error("업로드 시간 초과 (5분)"));
      xhr.send(formData);
    });
    // 백그라운드 DB 저장 대기 후 새로고침
    await new Promise(r => setTimeout(r, 1000));
    csShowDetail(ticketId);
  } catch(e) { alert("업로드 오류: " + (e.message || e)); }
  finally { overlay.remove(); }
}

// ── 파일 삭제 ──
async function csDeleteFile(fileId, ticketId) {
  if (!confirm("이 파일을 삭제하시겠습니까?")) return;
  try {
    await api.delete(`/api/cs/files/${fileId}`);
    csShowDetail(ticketId);
  } catch(e) { alert("삭제 오류: " + (e.message || e)); }
}

// ── Drive 업로드 진단 ──
async function csDriveCheck() {
  try {
    const res = await api.get("/api/cs/drive-check");
    const lines = [
      `Service Account JSON: ${res.service_account_json_set ? "설정됨 (" + res.service_account_json_length + "자)" : "미설정"}`,
      `JSON 파싱: ${res.json_parse || "-"}`,
      `서비스 계정: ${res.service_account_email || "-"}`,
      `프로젝트: ${res.project_id || "-"}`,
      `CS 폴더 ID: ${res.cs_folder_id || "-"}`,
      `토큰 발급: ${res.token_status || "-"}`,
      `폴더 접근: ${res.folder_access || "-"}`,
      res.folder_files ? `폴더 내 파일: ${res.folder_files.join(", ") || "(없음)"}` : "",
      `업로드 테스트: ${res.upload_test || "-"}`,
    ].filter(Boolean);
    alert("Google Drive 업로드 진단\n\n" + lines.join("\n"));
  } catch(e) { alert("진단 오류: " + (e.message || e)); }
}

// ── 파일 다운로드 ──
async function csDownloadFile(fileId, fileName) {
  try {
    const token = localStorage.getItem("jwt_token");
    const res = await fetch(`/api/cs/download/${fileId}`, {
      headers: { "Authorization": `Bearer ${token}` },
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `다운로드 실패 (${res.status})`);
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName || "download";
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch(e) { alert("다운로드 오류: " + (e.message || e)); }
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

// ═══════════════════════════════════════════
//  (판매에이전트 삭제됨)
// ═══════════════════════════════════════════

// ══════════════════════════════════════════════════════════════
//  AICC 관리자 탭
// ══════════════════════════════════════════════════════════════

let _aiccCurrentId = null;
let _aiccAdminWs = null;
let _aiccListWs = null;
let _aiccPolling = null;
let _aiccMenuFilter = '';  // '', '제품문의', '기술문의'
let _aiccChannelFilter = '';  // '', 'shop', 'external'

// 탭 진입 시 호출
async function initAiccTab() {
  await loadAiccSessions();
  loadUnansweredCount();
  connectAiccListWS();
  if (_aiccPolling) clearInterval(_aiccPolling);
  _aiccPolling = setInterval(loadAiccSessions, 8000);
  if (Notification && Notification.requestPermission) Notification.requestPermission();
}

// 세션 목록 실시간 WebSocket
var _aiccListWsClosedIntentionally = false;

function connectAiccListWS() {
  // 기존 WS 정리 (onclose 재접속 방지)
  if (_aiccListWs) {
    _aiccListWsClosedIntentionally = true;
    _aiccListWs.onclose = null;
    _aiccListWs.onmessage = null;
    _aiccListWs.close();
    _aiccListWs = null;
  }
  _aiccListWsClosedIntentionally = false;

  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  _aiccListWs = new WebSocket(proto + '//' + location.host + '/ws/aicc/admin-list');
  _aiccListWs.onmessage = function(e) {
    const data = JSON.parse(e.data);
    if (data.type === 'sessions_list') renderAiccSessions(data.sessions);
    else if (data.type === 'new_session') {
      loadAiccSessions();
      showNewSessionNotification(data.session);
    } else if (data.type === 'session_update') {
      loadAiccSessions();
    } else if (data.type === 'unanswered_alert') {
      loadUnansweredCount();
      if (typeof toast === 'function') toast('미답변 발생: ' + (data.model||'') + ' - ' + (data.question||'').substring(0,40), 'error');
    }
  };
  _aiccListWs.onclose = function() {
    // 의도적 종료가 아닌 경우에만 재접속 (무한루프 방지)
    if (!_aiccListWsClosedIntentionally) {
      setTimeout(connectAiccListWS, 5000);
    }
  };
}

async function loadAiccSessions() {
  try {
    var url = '/api/aicc/sessions';
    var params = [];
    if (_aiccMenuFilter) params.push('menu=' + encodeURIComponent(_aiccMenuFilter));
    if (_aiccChannelFilter) params.push('channel=' + encodeURIComponent(_aiccChannelFilter));
    if (params.length) url += '?' + params.join('&');
    const data = await api.get(url);
    renderAiccSessions(data.sessions);
  } catch (e) { console.warn('AICC 세션 로드 실패', e); }
}

function setAiccMenuFilter(btn, menu) {
  _aiccMenuFilter = menu;
  document.querySelectorAll('#aicc-menu-tabs .aicc-tab').forEach(function(t) {
    if (t === btn) {
      t.style.background = '#1a1a2e'; t.style.color = '#fff'; t.style.borderColor = '#1a1a2e';
    } else {
      t.style.background = '#fff'; t.style.color = '#333'; t.style.borderColor = '#d1d5db';
    }
  });
  loadAiccSessions();
}

function setAiccChannelFilter(channel) {
  _aiccChannelFilter = channel;
  loadAiccSessions();
}

function renderAiccSessions(sessions) {
  // 클라이언트 사이드 필터 (WebSocket 이벤트 등 필터 없이 들어온 경우 대비)
  var filtered = sessions;
  if (_aiccMenuFilter) {
    filtered = filtered.filter(function(s) { return s.selected_menu === _aiccMenuFilter; });
  }
  if (_aiccChannelFilter) {
    filtered = filtered.filter(function(s) {
      var ch = s.channel || 'shop';  // NULL/undefined → 'shop' (기존 세션 호환)
      return ch === _aiccChannelFilter;
    });
  }

  const groups = {
    '🔴 신규': filtered.filter(function(s) { return s.status === 'active'; }),
    '🟡 진행중': filtered.filter(function(s) { return s.status === 'intervened' || s.status === 'waiting_admin'; }),
    '⚫ 종료': filtered.filter(function(s) { return s.status === 'closed'; }).slice(0, 30),
  };

  const newCount = groups['🔴 신규'].length;
  const badge = document.getElementById('aicc-nav-badge');
  if (badge) {
    badge.textContent = newCount;
    badge.style.display = newCount > 0 ? '' : 'none';
  }

  const c = document.getElementById('aicc-session-list');
  if (!c) return;
  var html = '';
  for (const [label, list] of Object.entries(groups)) {
    if (!list.length) continue;
    html += '<div style="color:#999;font-size:11px;margin-top:8px;margin-bottom:4px;font-weight:700">' + label + ' (' + list.length + ')</div>';
    list.forEach(function(s) {
      const active = s.session_id === _aiccCurrentId;
      const modelInfo = s.selected_model ? s.selected_model + ' · ' : '';
      var timeStr = '';
      if (s.created_at) {
        try {
          // DB에 KST 문자열로 저장됨 → 그대로 파싱 (UTC 변환 방지)
          var raw = String(s.created_at).replace('T', ' ');
          var parts = raw.match(/(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})/);
          if (parts) {
            timeStr = parseInt(parts[2]) + '/' + parseInt(parts[3]) + ' ' + parts[4] + ':' + parts[5];
          } else {
            timeStr = raw.substring(5, 16);
          }
        } catch(e) { timeStr = String(s.created_at).substring(5, 16); }
      }
      var chBadge = '';
      if (s.channel === 'external') {
        var srcLabel = s.source || 'external';
        chBadge = '<span style="display:inline-block;background:#e63946;color:#fff;font-size:9px;padding:1px 5px;border-radius:8px;margin-left:4px;font-weight:700">🌐 ' + srcLabel + '</span>';
      }
      html +=
        '<div class="aicc-session-item" data-sid="' + s.session_id + '" style="padding:8px;margin-bottom:4px;border-radius:6px;cursor:pointer;background:' + (active ? '#1a1a2e' : '#f8f9fa') + ';color:' + (active ? '#fff' : '#333') + '">' +
          '<div style="display:flex;justify-content:space-between;align-items:center"><span style="font-weight:600;font-size:13px">' + (s.customer_name || '비회원') + chBadge + '</span><span style="font-size:10px;opacity:.5">' + timeStr + '</span></div>' +
          '<div style="font-size:11px;opacity:.75">' + modelInfo + s.selected_menu + '</div>' +
        '</div>';
    });
  }
  c.innerHTML = html;
  // 이벤트 위임: innerHTML 할당 후 클릭 핸들러 등록
  c.onclick = function(e) {
    var item = e.target.closest('.aicc-session-item');
    if (item) selectAiccSession(item.dataset.sid);
  };
}

var _aiccSelectRequestId = 0;  // 레이스 컨디션 방지용

async function selectAiccSession(id) {
  // 레이스 컨디션 방지: 이전 요청 무효화
  var requestId = ++_aiccSelectRequestId;

  if (_aiccAdminWs) {
    _aiccAdminWs.onmessage = null;
    _aiccAdminWs.close();
    _aiccAdminWs = null;
  }
  _aiccCurrentId = id;

  var s;
  try {
    s = await api.get('/api/aicc/sessions/' + id);
  } catch (err) {
    console.error('[AICC] 세션 로드 실패:', err);
    alert('세션을 불러올 수 없습니다: ' + (err.message || err));
    return;
  }

  // 레이스 컨디션: 다른 세션이 선택되었으면 이 응답 무시
  if (requestId !== _aiccSelectRequestId) return;

  document.getElementById('aicc-empty').style.display = 'none';
  document.getElementById('aicc-detail').style.display = 'flex';
  document.getElementById('aicc-d-name').textContent = s.customer_name || '비회원';
  document.getElementById('aicc-d-model').textContent = s.selected_model || '-';
  document.getElementById('aicc-d-menu').textContent = s.selected_menu;

  // 채널/출처 배지
  var chEl = document.getElementById('aicc-d-channel');
  if (chEl) {
    if (s.channel === 'external') {
      chEl.style.display = '';
      chEl.style.background = '#e63946';
      chEl.style.color = '#fff';
      chEl.textContent = '🌐 ' + (s.source || 'external');
    } else {
      chEl.style.display = '';
      chEl.style.background = '#10b981';
      chEl.style.color = '#fff';
      chEl.textContent = '🛒 쇼핑몰';
    }
  }

  const intervened = s.is_admin_intervened;
  document.getElementById('aicc-btn-intervene').style.display = intervened ? 'none' : '';
  document.getElementById('aicc-admin-input').style.display = intervened ? 'block' : 'none';
  document.getElementById('aicc-intervene-bar').style.display = intervened ? 'block' : 'none';

  // 메시지 렌더
  const mc = document.getElementById('aicc-msgs');
  mc.innerHTML = '';
  var msgs = s.messages || [];
  if (msgs.length === 0) {
    mc.innerHTML = '<div style="text-align:center;color:#999;padding:40px 0;font-size:13px">아직 대화 내역이 없습니다.</div>';
  } else {
    msgs.forEach(function(m) { appendAiccMsg(m.role, m.content, false); });
    mc.scrollTop = mc.scrollHeight;
  }

  // 관리자 WebSocket
  var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  _aiccAdminWs = new WebSocket(proto + '//' + location.host + '/ws/aicc/admin/' + id);
  _aiccAdminWs.onmessage = function(e) {
    // 다른 세션이 선택되었으면 무시
    if (_aiccCurrentId !== id) return;
    var msg = JSON.parse(e.data);
    if (['customer_message', 'ai_message', 'admin_message'].includes(msg.type)) {
      // 빈 메시지 안내 제거
      var emptyNote = mc.querySelector('div[style*="text-align:center"]');
      if (emptyNote) emptyNote.remove();
      appendAiccMsg(msg.role || 'user', msg.content);
    }
  };

  // 세션 목록에서 선택 상태만 업데이트 (전체 리로드 대신 DOM 직접 조작)
  document.querySelectorAll('.aicc-session-item').forEach(function(el) {
    var isActive = el.dataset.sid === id;
    el.style.background = isActive ? '#1a1a2e' : '#f8f9fa';
    el.style.color = isActive ? '#fff' : '#333';
  });
}

function appendAiccMsg(role, content, scroll) {
  if (scroll === undefined) scroll = true;
  const mc = document.getElementById('aicc-msgs');
  const colors = { user: '#e3f2fd', assistant: '#f1f8e9', admin: '#fff3e0' };
  const labels = { user: '고객', assistant: 'AI', admin: '관리자' };
  const d = document.createElement('div');
  d.style.cssText = 'margin-bottom:10px;padding:8px 12px;border-radius:10px;background:' + (colors[role]||'#f5f5f5') + ';max-width:85%;' + (role==='user'?'margin-left:auto':'');
  d.innerHTML = '<div style="font-size:10px;color:#888;margin-bottom:3px">' + (labels[role]||role) + '</div><div style="font-size:13px;white-space:pre-wrap">' + content + '</div>';
  mc.appendChild(d);
  if (scroll) mc.scrollTop = mc.scrollHeight;
}

async function aiccIntervene() {
  await api.post('/api/aicc/sessions/' + _aiccCurrentId + '/intervene');
  document.getElementById('aicc-btn-intervene').style.display = 'none';
  document.getElementById('aicc-admin-input').style.display = 'block';
  document.getElementById('aicc-intervene-bar').style.display = 'block';
  document.getElementById('aicc-admin-txt').focus();
}

async function aiccSendAdmin() {
  const txt = document.getElementById('aicc-admin-txt').value.trim();
  if (!txt) return;
  if (_aiccAdminWs && _aiccAdminWs.readyState === WebSocket.OPEN) {
    _aiccAdminWs.send(JSON.stringify({ type: 'admin_message', content: txt }));
  } else {
    await api.post('/api/aicc/sessions/' + _aiccCurrentId + '/admin-message', { content: txt });
  }
  appendAiccMsg('admin', txt);
  document.getElementById('aicc-admin-txt').value = '';
}

async function aiccCloseSession() {
  if (!confirm('상담을 종료하시겠습니까?')) return;
  await api.post('/api/aicc/sessions/' + _aiccCurrentId + '/close');
  await loadAiccSessions();
}

document.addEventListener('keydown', function(e) {
  if (e.target.id === 'aicc-admin-txt' && e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    aiccSendAdmin();
  }
});

function showNewSessionNotification(session) {
  if (Notification && Notification.permission === 'granted') {
    new Notification('랜스타 AI 상담 — 신규 문의', {
      body: (session.customer_name || '비회원') + ' | ' + (session.selected_model || '') + ' | ' + session.selected_menu,
    });
  }
}

// ── 미답변 알림 시스템 ──────────────────────────────

var _unansCurrentId = null;

async function loadUnansweredCount() {
  try {
    var data = await api.get('/api/aicc/unanswered/count');
    var badge = document.getElementById('aicc-bell-badge');
    if (badge) {
      badge.textContent = data.count;
      badge.style.display = data.count > 0 ? '' : 'none';
    }
  } catch(e) { /* 무시 */ }
}

async function toggleUnansweredPanel() {
  var panel = document.getElementById('aicc-unanswered-panel');
  if (!panel) return;
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    await loadUnansweredList();
  } else {
    panel.style.display = 'none';
  }
}

async function loadUnansweredList() {
  try {
    var data = await api.get('/api/aicc/unanswered?resolved=false');
    var c = document.getElementById('aicc-unanswered-list');
    if (!c) return;
    if (!data.items || data.items.length === 0) {
      c.innerHTML = '<div style="color:#999;text-align:center;padding:12px">미답변 없음 ✅</div>';
      return;
    }
    var html = '';
    data.items.forEach(function(item) {
      html +=
        '<div onclick="openUnansModal(' + item.id + ')" style="padding:8px;margin-bottom:4px;border-radius:6px;background:#fef2f2;border:1px solid #fecaca;cursor:pointer">' +
          '<div style="font-weight:600;font-size:12px;color:#e63946">' + (item.model_name || '미지정') + '</div>' +
          '<div style="font-size:11px;color:#333;margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + _escHtml(item.user_question || '').substring(0, 60) + '</div>' +
          '<div style="font-size:10px;color:#999;margin-top:2px">' + (item.created_at || '') + '</div>' +
        '</div>';
    });
    c.innerHTML = html;
  } catch(e) { console.warn('미답변 로드 실패', e); }
}

async function openUnansModal(id) {
  _unansCurrentId = id;
  try {
    var data = await api.get('/api/aicc/unanswered?resolved=false');
    var item = data.items.find(function(x) { return x.id === id; });
    if (!item) return;

    var body = document.getElementById('unans-modal-body');
    body.innerHTML =
      '<div style="margin-bottom:8px"><span style="font-size:11px;color:#666">모델:</span> <strong>' + (item.model_name||'-') + '</strong></div>' +
      '<div style="background:#e3f2fd;padding:10px;border-radius:8px;margin-bottom:8px"><div style="font-size:10px;color:#666;margin-bottom:4px">고객 질문</div><div style="font-size:13px">' + _escHtml(item.user_question) + '</div></div>' +
      '<div style="background:#fff3e0;padding:10px;border-radius:8px"><div style="font-size:10px;color:#666;margin-bottom:4px">AI 응답 (미답변)</div><div style="font-size:12px;white-space:pre-wrap">' + _escHtml(item.ai_response).substring(0, 500) + '</div></div>';

    document.getElementById('unans-model').value = item.model_name || '';
    document.getElementById('unans-key').value = '';
    document.getElementById('unans-value').value = '';

    var modal = document.getElementById('aicc-unans-modal');
    modal.style.display = 'flex';
  } catch(e) { console.warn(e); }
}

function closeUnansModal() {
  document.getElementById('aicc-unans-modal').style.display = 'none';
  _unansCurrentId = null;
}

async function resolveUnanswered() {
  if (!_unansCurrentId) return;
  try {
    await api.post('/api/aicc/unanswered/' + _unansCurrentId + '/resolve');
    toast('해결 처리 완료', 'success');
    closeUnansModal();
    loadUnansweredList();
    loadUnansweredCount();
  } catch(e) { toast('오류 발생', 'error'); }
}

async function addKnowledgeFromUnanswered() {
  if (!_unansCurrentId) return;
  var model = document.getElementById('unans-model').value.trim();
  var key = document.getElementById('unans-key').value.trim();
  var value = document.getElementById('unans-value').value.trim();
  if (!model || !key || !value) {
    toast('모델명, 항목명, 내용을 모두 입력하세요', 'error');
    return;
  }
  try {
    await api.post('/api/aicc/unanswered/' + _unansCurrentId + '/add-knowledge', {
      model_name: model, key: key, value: value
    });
    toast(model + ' DB에 [' + key + '] 추가 완료', 'success');
    closeUnansModal();
    loadUnansweredList();
    loadUnansweredCount();
  } catch(e) { toast('DB 추가 실패', 'error'); }
}

function _escHtml(s) {
  var d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

// 미답변 카운트 주기적 체크 (30초마다)
setInterval(loadUnansweredCount, 30000);

// ── 지식 DB 직접 추가 시스템 ──────────────────────────────

var _kbSelectedModel = null;
var _kbAllModels = [];
var _kbAddHistory = [];
var _kbSearchTimer = null;

async function openKnowledgeAddModal() {
  _kbSelectedModel = null;
  _kbAddHistory = [];
  document.getElementById('kb-model-input').value = '';
  document.getElementById('kb-selected-model').style.display = 'none';
  document.getElementById('kb-model-dropdown').style.display = 'none';
  document.getElementById('kb-existing-area').style.display = 'none';
  document.getElementById('kb-existing-data').style.display = 'none';
  document.getElementById('kb-key').value = '';
  document.getElementById('kb-value').value = '';
  document.getElementById('kb-add-history').style.display = 'none';
  document.getElementById('kb-add-history-list').innerHTML = '';
  document.getElementById('aicc-kb-modal').style.display = 'flex';

  // 모델 목록 초기 로드 (캐시)
  if (_kbAllModels.length === 0) {
    try {
      _kbAllModels = await api.get('/api/aicc/models');
    } catch(e) { console.warn('모델 목록 로드 실패', e); }
  }
}

function closeKbModal() {
  document.getElementById('aicc-kb-modal').style.display = 'none';
  _kbSelectedModel = null;
}

function kbModelSearch(q) {
  // 디바운스
  if (_kbSearchTimer) clearTimeout(_kbSearchTimer);
  _kbSearchTimer = setTimeout(function() { _kbDoModelSearch(q); }, 200);
}

function _kbDoModelSearch(q) {
  var dropdown = document.getElementById('kb-model-dropdown');
  q = (q || '').trim().toUpperCase();
  if (q.length < 2) {
    dropdown.style.display = 'none';
    return;
  }

  // 로컬 필터링 (이미 전체 모델 로드됨)
  var matches = _kbAllModels.filter(function(m) {
    return m.model_name.toUpperCase().indexOf(q) >= 0 ||
           (m.product_name || '').toUpperCase().indexOf(q) >= 0;
  }).slice(0, 15);

  if (matches.length === 0) {
    dropdown.innerHTML = '<div style="padding:8px 12px;color:#999;font-size:12px">검색 결과 없음</div>';
    dropdown.style.display = 'block';
    return;
  }

  var html = '';
  matches.forEach(function(m) {
    html += '<div onclick="kbSelectModel(\'' + _escHtml(m.model_name) + '\',\'' + _escHtml(m.product_name || '') + '\')" ' +
      'style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0;font-size:12px" ' +
      'onmouseover="this.style.background=\'#f3f4f6\'" onmouseout="this.style.background=\'#fff\'">' +
      '<span style="font-weight:600;color:#4f46e5">' + _escHtml(m.model_name) + '</span> ' +
      '<span style="color:#888">' + _escHtml(m.product_name || '') + '</span>' +
      '</div>';
  });
  dropdown.innerHTML = html;
  dropdown.style.display = 'block';
}

async function kbSelectModel(modelName, productName) {
  _kbSelectedModel = modelName;
  document.getElementById('kb-model-input').value = modelName;
  document.getElementById('kb-model-dropdown').style.display = 'none';
  document.getElementById('kb-selected-name').textContent = modelName + (productName ? ' (' + productName + ')' : '');
  document.getElementById('kb-selected-model').style.display = 'block';

  // 기존 데이터 로드
  document.getElementById('kb-existing-area').style.display = 'block';
  document.getElementById('kb-existing-data').style.display = 'none';
  document.getElementById('kb-existing-toggle').textContent = '펼치기';
  try {
    var data = await api.get('/api/aicc/knowledge/' + encodeURIComponent(modelName));
    if (data && data.data) {
      var formatted = JSON.stringify(data.data, null, 2);
      document.getElementById('kb-existing-data').textContent = formatted;
    } else {
      document.getElementById('kb-existing-data').textContent = '(등록된 지식 데이터 없음)';
    }
  } catch(e) {
    document.getElementById('kb-existing-data').textContent = '(등록된 지식 데이터 없음 — 새로 추가됩니다)';
  }
}

function toggleKbExisting() {
  var area = document.getElementById('kb-existing-data');
  var btn = document.getElementById('kb-existing-toggle');
  if (area.style.display === 'none') {
    area.style.display = 'block';
    btn.textContent = '접기';
  } else {
    area.style.display = 'none';
    btn.textContent = '펼치기';
  }
}

async function addKnowledgeDirect() {
  if (!_kbSelectedModel) {
    toast('모델명을 선택하세요 (검색 후 목록에서 클릭)', 'error');
    return;
  }
  var key = document.getElementById('kb-key').value.trim();
  var value = document.getElementById('kb-value').value.trim();
  if (!key || !value) {
    toast('항목명과 내용을 모두 입력하세요', 'error');
    return;
  }
  try {
    await api.post('/api/aicc/knowledge/add-direct', {
      model_name: _kbSelectedModel,
      key: key,
      value: value
    });
    toast(_kbSelectedModel + ' DB에 [' + key + '] 추가 완료', 'success');

    // 이력 표시
    _kbAddHistory.push({ model: _kbSelectedModel, key: key });
    var historyDiv = document.getElementById('kb-add-history');
    var historyList = document.getElementById('kb-add-history-list');
    historyDiv.style.display = 'block';
    var hHtml = '';
    _kbAddHistory.forEach(function(h) {
      hHtml += '<div style="padding:4px 0;border-bottom:1px solid #f0f0f0">✅ <strong>' + _escHtml(h.model) + '</strong> → [' + _escHtml(h.key) + ']</div>';
    });
    historyList.innerHTML = hHtml;

    // 입력 초기화 (모델은 유지)
    document.getElementById('kb-key').value = '';
    document.getElementById('kb-value').value = '';

    // 기존 데이터 새로고침
    kbSelectModel(_kbSelectedModel, '');
  } catch(e) { toast('DB 추가 실패: ' + (e.message || ''), 'error'); }
}

// ═══════════════════════════════════════════════════════
//  재고 변동 모니터링
// ═══════════════════════════════════════════════════════

async function initInventoryMonitor() {
  await Promise.all([
    loadAlertSettings(),
    loadAlertHistory(),
    loadExcludeKeywords(),
  ]);
}

async function runMonitorNow() {
  const btn = document.getElementById('btn-monitor-run');
  const resultDiv = document.getElementById('monitor-run-result');

  btn.disabled = true;
  btn.textContent = '⏳ 실행 중...';
  resultDiv.style.display = 'block';
  resultDiv.innerHTML = '<span style="color:#888;">ERP 재고 조회 및 비교 중입니다. 잠시 기다려주세요...</span>';

  try {
    const data = await api.post('/api/inventory-monitor/run', {});

    if (data.status === 'ok') {
      resultDiv.innerHTML = `<div style="padding:12px; background:#E8F5E9; border-radius:8px;"><b>✅ 완료!</b><br>알림 대상: <b>${data.alerts_count}건</b><br>${data.message}</div>`;
      loadAlertHistory();
    } else if (data.status === 'no_prev') {
      resultDiv.innerHTML = `<div style="padding:12px; background:#FFF3E0; border-radius:8px;"><b>ℹ️ 첫 실행</b><br>${data.message}<br>내일부터 비교가 시작됩니다.</div>`;
    } else {
      resultDiv.innerHTML = `<div style="padding:12px; background:#FFEBEE; border-radius:8px;"><b>❌ 오류</b><br>${data.message}</div>`;
    }
  } catch (err) {
    resultDiv.innerHTML = `<div style="padding:12px; background:#FFEBEE; border-radius:8px;"><b>❌ 실행 실패</b><br>${err.message || err}</div>`;
  }

  btn.disabled = false;
  btn.textContent = '▶ 지금 실행';
}

async function loadAlertHistory() {
  const days = document.getElementById('history-days')?.value || 30;
  const tbody = document.getElementById('alert-history-body');
  if (!tbody) return;

  try {
    const data = await api.get(`/api/inventory-monitor/history?days=${days}`);
    const items = data.items || [];

    if (items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="10" style="padding:20px; text-align:center; color:#888;">알림 이력이 없습니다.</td></tr>';
      return;
    }

    tbody.innerHTML = items.map(item => {
      const triggerBadge = {
        'amount': '<span style="background:#E3F2FD; color:#1565C0; padding:2px 8px; border-radius:12px; font-size:12px;">💰 금액</span>',
        'qty': '<span style="background:#FFF3E0; color:#E65100; padding:2px 8px; border-radius:12px; font-size:12px;">📉 수량</span>',
        'both': '<span style="background:#FCE4EC; color:#C62828; padding:2px 8px; border-radius:12px; font-size:12px;">🔥 복합</span>',
      }[item.trigger_type] || item.trigger_type;

      const dateStr = `${item.check_date.substring(0,4)}-${item.check_date.substring(4,6)}-${item.check_date.substring(6)}`;

      return `<tr style="border-bottom:1px solid #eee;">
        <td style="padding:8px;">${dateStr}</td>
        <td style="padding:8px; font-family:monospace; font-size:12px;">${item.prod_cd}</td>
        <td style="padding:8px; max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${item.prod_name}">${item.prod_name}</td>
        <td style="padding:8px;">${item.model_name || '-'}</td>
        <td style="padding:8px; text-align:right;">${Number(item.unit_price).toLocaleString()}원</td>
        <td style="padding:8px; text-align:right;">${Number(item.prev_qty).toLocaleString()}</td>
        <td style="padding:8px; text-align:right;">${Number(item.curr_qty).toLocaleString()}</td>
        <td style="padding:8px; text-align:right; color:#D32F2F; font-weight:bold;">-${Number(item.diff_qty).toLocaleString()}</td>
        <td style="padding:8px; text-align:right; font-weight:bold;">${Number(item.diff_amount).toLocaleString()}원</td>
        <td style="padding:8px;">${triggerBadge}</td>
      </tr>`;
    }).join('');
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="10" style="padding:20px; text-align:center; color:#D32F2F;">이력 로딩 실패: ${err.message || err}</td></tr>`;
  }
}

async function loadAlertSettings() {
  try {
    const settings = await api.get('/api/inventory-monitor/settings');
    const el = (id) => document.getElementById(id);
    if (el('setting-threshold-amount')) el('setting-threshold-amount').value = settings.threshold_amount || 500000;
    if (el('setting-threshold-qty')) el('setting-threshold-qty').value = settings.threshold_qty || 100;
    if (el('setting-enabled')) el('setting-enabled').checked = settings.enabled !== false;
    if (settings.telegram_chat_id && el('setting-telegram-chatid')) el('setting-telegram-chatid').value = settings.telegram_chat_id;
    if (settings.telegram_bot_token_masked && el('setting-telegram-token')) el('setting-telegram-token').placeholder = settings.telegram_bot_token_masked;
  } catch (err) { console.error('알림 설정 로딩 실패:', err); }
}

async function saveAlertSettings() {
  const resultDiv = document.getElementById('settings-save-result');
  const body = {
    threshold_amount: parseInt(document.getElementById('setting-threshold-amount').value),
    threshold_qty: parseInt(document.getElementById('setting-threshold-qty').value),
    enabled: document.getElementById('setting-enabled').checked,
  };
  const token = document.getElementById('setting-telegram-token').value;
  if (token && !token.includes('...')) body.telegram_bot_token = token;
  const chatId = document.getElementById('setting-telegram-chatid').value;
  if (chatId) body.telegram_chat_id = chatId;

  try {
    await api.put('/api/inventory-monitor/settings', body);
    resultDiv.innerHTML = '<span style="color:#4CAF50;">✅ 설정이 저장되었습니다.</span>';
    setTimeout(() => { resultDiv.innerHTML = ''; }, 3000);
  } catch (err) {
    resultDiv.innerHTML = `<span style="color:#D32F2F;">❌ 저장 실패: ${err.message || err}</span>`;
  }
}

async function testTelegram() {
  const resultDiv = document.getElementById('telegram-test-result');
  resultDiv.innerHTML = '<span style="color:#888;">테스트 중...</span>';
  try {
    const data = await api.post('/api/inventory-monitor/telegram/test', {});
    if (data.message_sent) {
      resultDiv.innerHTML = `<span style="color:#4CAF50;">✅ 발송 성공! 텔레그램을 확인하세요. (@${data.bot_info?.bot_username || ''})</span>`;
    } else {
      resultDiv.innerHTML = '<span style="color:#D32F2F;">❌ 발송 실패. 토큰과 Chat ID를 확인하세요.</span>';
    }
  } catch (err) {
    resultDiv.innerHTML = `<span style="color:#D32F2F;">❌ ${err.message || err}</span>`;
  }
}

async function loadExcludeKeywords() {
  try {
    const data = await api.get('/api/inventory-monitor/keywords');
    renderKeywords(data.keywords || []);
  } catch (err) { console.error('키워드 로딩 실패:', err); }
}

function renderKeywords(keywords) {
  const container = document.getElementById('keywords-list');
  if (!container) return;
  container.innerHTML = keywords.map(kw => `
    <span style="display:inline-flex; align-items:center; gap:4px; padding:6px 12px; background:#FFF3E0; border-radius:20px; font-size:14px;">
      ${kw}
      <button onclick="removeKeyword('${kw}')" style="background:none; border:none; cursor:pointer; color:#E65100; font-size:16px; padding:0 2px;">✕</button>
    </span>
  `).join('');
  if (keywords.length === 0) container.innerHTML = '<span style="color:#888; font-size:14px;">등록된 키워드가 없습니다.</span>';
}

async function addKeyword() {
  const input = document.getElementById('new-keyword-input');
  const keyword = input.value.trim();
  if (!keyword) return;
  try {
    await api.post('/api/inventory-monitor/keywords', { keyword });
    input.value = '';
    loadExcludeKeywords();
  } catch (err) { toast('키워드 추가 실패: ' + (err.message || err), 'error'); }
}

async function removeKeyword(keyword) {
  if (!confirm(`"${keyword}" 키워드를 삭제하시겠습니까?`)) return;
  try {
    await api.delete(`/api/inventory-monitor/keywords/${encodeURIComponent(keyword)}`);
    loadExcludeKeywords();
  } catch (err) { toast('키워드 삭제 실패: ' + (err.message || err), 'error'); }
}


// ═══════════════════════════════════════════════════════════
//  매입정산 (Purchase Reconciliation) — 일괄 처리
// ═══════════════════════════════════════════════════════════

const _rc = {
  step: 1,
  batchResult: null,
  purchaseQueue: [],
  vendorFiles: [],  // {file, name, code, status} 누적 관리
};

let _reconcileDropzoneInitialized = false;

function initReconcilePage() {
  reconcileSetStep(1);
  reconcileCheckErpCache();
  _rc.vendorFiles = [];
  _rc.batchResult = null;

  // DOM 초기화 (이전 결과 잔여물 제거)
  const vfList = document.getElementById("reconcile-vendor-file-list");
  if (vfList) vfList.innerHTML = "";
  const vendorConfirm = document.getElementById("reconcile-vendor-confirm");
  if (vendorConfirm) vendorConfirm.style.display = "none";
  const vendorNameList = document.getElementById("reconcile-vendor-name-list");
  if (vendorNameList) vendorNameList.innerHTML = "";
  const summary = document.getElementById("reconcile-batch-summary");
  if (summary) summary.innerHTML = "";
  const accordion = document.getElementById("reconcile-vendor-accordion");
  if (accordion) accordion.innerHTML = "";
  const progress = document.getElementById("reconcile-batch-progress");
  if (progress) progress.style.display = "none";

  // 드롭존 이벤트 리스너 (최초 1회만 등록)
  if (_reconcileDropzoneInitialized) return;
  _reconcileDropzoneInitialized = true;

  const dropzone = document.getElementById("rc-vendor-dropzone");
  const vf = document.getElementById("reconcile-vendor-files");
  if (dropzone && vf) {
    let _vendorFileProcessing = false;
    let _processingTimeout = null;

    function _setProcessing(val) {
      _vendorFileProcessing = val;
      // 타임아웃 안전장치: 30초 후 자동 해제
      if (_processingTimeout) clearTimeout(_processingTimeout);
      if (val) {
        dropzone.style.opacity = "0.6";
        dropzone.style.pointerEvents = "none";
        _processingTimeout = setTimeout(() => {
          _vendorFileProcessing = false;
          dropzone.style.opacity = "";
          dropzone.style.pointerEvents = "";
        }, 30000);
      } else {
        dropzone.style.opacity = "";
        dropzone.style.pointerEvents = "";
      }
    }

    dropzone.addEventListener("click", (e) => {
      if (_vendorFileProcessing) return;
      if (e.target === vf || e.target.closest("button")) return;
      vf.click();
    });
    dropzone.addEventListener("dragover", e => { e.preventDefault(); dropzone.classList.add("dragover"); });
    dropzone.addEventListener("dragleave", () => dropzone.classList.remove("dragover"));
    dropzone.addEventListener("drop", async (e) => {
      e.preventDefault(); dropzone.classList.remove("dragover");
      if (_vendorFileProcessing || !e.dataTransfer.files.length) return;
      _setProcessing(true);
      try { await _addVendorFiles(Array.from(e.dataTransfer.files)); }
      finally { _setProcessing(false); }
    });
    vf.addEventListener("change", async () => {
      if (!vf.files.length) return;
      const files = Array.from(vf.files);
      vf.value = ""; // 먼저 리셋하여 change 재발방지
      if (_vendorFileProcessing) return;
      _setProcessing(true);
      try { await _addVendorFiles(files); }
      finally { _setProcessing(false); }
    });
  }
}

async function _addVendorFiles(newFiles) {
  const added = [];
  const ALLOWED_EXTS = [".xlsx", ".xls"];
  for (const f of newFiles) {
    // 파일 타입 검증
    const ext = (f.name.match(/\.[^.]+$/) || [""])[0].toLowerCase();
    if (!ALLOWED_EXTS.includes(ext)) {
      toast(`${f.name}: 엑셀 파일(.xlsx, .xls)만 업로드 가능합니다`, "error");
      continue;
    }
    const exists = _rc.vendorFiles.some(v => v.file.name === f.name && v.file.size === f.size);
    if (exists) {
      toast(`${f.name}: 이미 추가된 파일입니다`, "warn");
      continue;
    }
    const entry = { file: f, name: "", code: "", status: "pending", date_from: "", date_to: "", item_count: 0, saved_path: "" };
    _rc.vendorFiles.push(entry);
    added.push(entry);
  }
  if (!added.length) return;
  _renderVendorFileList();  // 즉시 파일 태그 + pending 상태 표시

  // Upload to preview-vendors
  const formData = new FormData();
  for (const a of added) formData.append("vendor_files", a.file);
  try {
    const res = await api.postForm("/api/reconcile/preview-vendors", formData);
    const previews = res.previews || [];
    for (let j = 0; j < previews.length; j++) {
      const p = previews[j];
      const entry = added[j];
      if (!entry) continue;
      entry.name = p.vendor_name || _extractVendorName(entry.file.name);
      entry.date_from = p.date_from || "";
      entry.date_to = p.date_to || "";
      entry.item_count = p.item_count || 0;
      entry.saved_path = p.saved_path || "";

      if (p.error) {
        entry.status = "error";
        toast(`${entry.file.name}: 파싱 실패 — ${p.error}`, "error");
        continue;
      }

      // Fuzzy match against vendors.json for code
      try {
        const vRes = await api.get(`/api/reconcile/vendor-list?q=${encodeURIComponent(entry.name)}`);
        const vendors = vRes.vendors || [];
        const best = _fuzzyMatchVendor(entry.name, vendors);
        if (best) {
          entry.name = best.name;
          entry.code = best.code || "";
        }
        entry.status = "matched";
      } catch (e2) {
        console.error(`[VendorMatch] #${_rc.vendorFiles.indexOf(entry)} 오류:`, e2);
        entry.status = "matched"; // 코드 못 찾아도 파일 자체는 유효
      }
    }
    if (added.length > 0 && added.every(a => a.status === "matched")) {
      toast(`${added.length}개 파일 업로드 완료`, "success");
    }
  } catch (e) {
    console.error("[PreviewVendors] API 오류:", e);
    for (const a of added) { a.status = "error"; }
    toast("파일 업로드 실패: " + (e.message || e), "error");
  }

  _renderVendorFileList();
}

function _removeVendorFile(idx) {
  _rc.vendorFiles.splice(idx, 1);
  _renderVendorFileList();
}

function _extractVendorName(filename) {
  let base = filename.replace(/\.(xlsx|xls)$/i, "");
  base = base.replace(/-[0-9a-f]{6,12}$/, "");
  const parts = base.replace(/-/g, "_").split("_");
  const skipPrefixes = new Set(["거래장부내역","거래내역","거래확인서","거래원장","원장"]);
  const nameParts = parts.filter(p => p && !/^\d+$/.test(p) && !skipPrefixes.has(p));
  return nameParts.length ? nameParts[nameParts.length - 1] : base;
}

function _fmtDate(d) {
  // "20260301" → "2026.03.01", "2026-03-01" → "2026.03.01", "03/01" → "03/01"
  if (!d) return "";
  const s = d.replace(/[-/.]/g, "");
  if (s.length === 8) return `${s.slice(0,4)}.${s.slice(4,6)}.${s.slice(6,8)}`;
  return d;
}

function _fuzzyMatchVendor(extracted, vendors) {
  // (주), 주식회사 등 제거 후 비교
  const clean = s => s.replace(/\(주\)|주식회사|\s/g, "").trim();
  const ec = clean(extracted);
  if (!ec) return null;

  // 1순위: 정확 매칭
  const exact = vendors.find(v => v.name === extracted);
  if (exact) return exact;

  // 2순위: clean 후 정확 매칭
  const cleanExact = vendors.find(v => clean(v.name) === ec);
  if (cleanExact) return cleanExact;

  // 3순위: 부분 포함 (한쪽이 다른쪽을 포함)
  const partial = vendors.find(v => {
    const vc = clean(v.name);
    return vc.includes(ec) || ec.includes(vc);
  });
  if (partial) return partial;

  // 4순위: 첫 번째 결과 (API가 이미 q로 필터링했으므로 관련성 높음)
  return vendors[0] || null;
}

function _renderVendorFileList() {
  const el = document.getElementById("reconcile-vendor-file-list");
  const confirmArea = document.getElementById("reconcile-vendor-confirm");
  if (!el) return;

  if (!_rc.vendorFiles.length) {
    el.innerHTML = "";
    if (confirmArea) confirmArea.style.display = "none";
    return;
  }

  el.innerHTML = _rc.vendorFiles.map((v, i) =>
    `<span class="rc-file-tag">📄 ${v.file.name} <button onclick="_removeVendorFile(${i})" style="border:none;background:none;cursor:pointer;color:#999;font-size:14px;padding:0 2px">&times;</button></span>`
  ).join("");

  if (confirmArea) confirmArea.style.display = "block";
  _renderVendorNameRows();
}

function _renderVendorNameRows() {
  const nameList = document.getElementById("reconcile-vendor-name-list");
  if (!nameList) return;
  nameList.innerHTML = _rc.vendorFiles.map((v, i) => {
    const displayName = v.name || _extractVendorName(v.file.name);
    const displayCode = v.code || "";
    const icon = v.status === "error" ? "❌" : v.status === "pending" ? "🔍" : "✅";

    // Format date range
    let dateStr = "";
    if (v.date_from && v.date_to) {
      dateStr = `${_fmtDate(v.date_from)} ~ ${_fmtDate(v.date_to)}`;
    } else if (v.date_from) {
      dateStr = _fmtDate(v.date_from);
    }

    return `<div class="rc-vendor-name-row">
      <span class="rc-vnr-file" title="${v.file.name}">📄 ${v.file.name}</span>
      <input type="text" class="rc-vendor-name-input" data-idx="${i}" value="${displayName}"
        onchange="_onVendorNameChange(${i}, this.value)">
      <span class="rc-vnr-code" id="rc-vnr-code-${i}" title="거래처코드">${displayCode}</span>
      <span class="rc-vnr-date" id="rc-vnr-date-${i}" title="거래기간">${dateStr}</span>
      <span class="rc-vnr-status" id="rc-vnr-status-${i}">${icon}</span>
      <button onclick="_removeVendorFile(${i})" class="rc-vnr-delete" title="삭제">&times;</button>
    </div>`;
  }).join("");
}

async function _onVendorNameChange(idx, newName) {
  // 사용자가 거래처명을 수정하면 다시 API로 매칭 시도
  const v = _rc.vendorFiles[idx];
  if (!v || !newName.trim()) return;
  const statusEl = document.getElementById(`rc-vnr-status-${idx}`);
  const codeEl = document.getElementById(`rc-vnr-code-${idx}`);
  if (statusEl) statusEl.textContent = "🔍";
  try {
    const res = await api.get(`/api/reconcile/vendor-list?q=${encodeURIComponent(newName.trim())}`);
    const vendors = res.vendors || [];
    const best = _fuzzyMatchVendor(newName.trim(), vendors);
    if (best) {
      v.name = best.name;
      v.code = best.code || "";
      if (codeEl) codeEl.textContent = best.code || "";
      const inputEl = document.querySelector(`.rc-vendor-name-input[data-idx="${idx}"]`);
      if (inputEl && inputEl.value !== best.name) inputEl.value = best.name;
    } else {
      v.name = newName.trim();
      v.code = "";
      if (codeEl) codeEl.textContent = "";
    }
    v.status = "matched";
    if (statusEl) statusEl.textContent = "✅";
  } catch (e) {
    v.status = "matched";
    if (statusEl) statusEl.textContent = "✅";
  }
}

function reconcileSetStep(n) {
  _rc.step = n;
  for (let i = 1; i <= 4; i++) {
    const el = document.getElementById(`reconcile-step${i}`);
    if (el) el.style.display = i === n ? "" : "none";
  }
  document.querySelectorAll("#reconcile-steps .rc-step").forEach(b => {
    const s = parseInt(b.dataset.step);
    b.classList.remove("active", "done");
    if (s === n) b.classList.add("active");
    else if (s < n) b.classList.add("done");
  });
}

function reconcileGoBack(toStep) { reconcileSetStep(toStep); }

// ── ERP 캐시 관리 ──

async function reconcileCheckErpCache() {
  try {
    const res = await api.get("/api/reconcile/erp-cache-status");
    _updateErpCacheUI("purchase", res.purchase);
    _updateErpCacheUI("sales", res.sales);
  } catch (e) { /* ignore */ }
}

function _updateErpCacheUI(type, info) {
  if (type === "sales") {
    const fileWrap = document.getElementById("reconcile-sales-file-wrap");
    const cachedDiv = document.getElementById("reconcile-sales-cached");
    const cachedInfo = document.getElementById("reconcile-sales-cached-info");
    if (!fileWrap || !cachedDiv || !cachedInfo) return;
    if (info.cached && info.total > 0) {
      fileWrap.style.display = "none";
      cachedDiv.style.display = "flex";
      cachedInfo.textContent = `${info.filename || "판매현황"} — ${info.total.toLocaleString()}건 캐시됨`;
    } else {
      fileWrap.style.display = "";
      cachedDiv.style.display = "none";
    }
  }
  if (type === "purchase") {
    const pFileEl = document.getElementById("reconcile-erp-purchase-file");
    const cachedP = document.getElementById("reconcile-purchase-cached");
    if (!pFileEl || !cachedP) return;
    const cachedInfo = document.getElementById("reconcile-purchase-cached-info");
    if (info.cached && info.total > 0) {
      pFileEl.style.display = "none";
      cachedP.style.display = "flex";
      if (cachedInfo) cachedInfo.textContent = `${info.filename || "구매현황"} — ${info.total.toLocaleString()}건 캐시됨`;
    } else {
      pFileEl.style.display = "";
      cachedP.style.display = "none";
    }
  }
}

async function reconcileClearPurchaseCache() {
  if (!confirm("캐시된 구매현황 데이터를 삭제하시겠습니까?")) return;
  try {
    await api.delete("/api/reconcile/clear-purchase-cache");
    _updateErpCacheUI("purchase", {cached:false, total:0, filename:""});
    toast("구매현황 캐시가 삭제되었습니다", "info");
  } catch (e) { toast("삭제 실패: " + (e.message||e), "error"); }
}

async function reconcileClearSalesCache() {
  if (!confirm("캐시된 판매현황 데이터를 삭제하시겠습니까?")) return;
  try {
    await api.delete("/api/reconcile/clear-sales-cache");
    _updateErpCacheUI("sales", {cached:false, total:0, filename:""});
    toast("판매현황 캐시가 삭제되었습니다", "info");
  } catch (e) { toast("삭제 실패: " + (e.message||e), "error"); }
}

// ── STEP 1: 일괄 정산 시작 ──

async function reconcileBatchStart() {
  const purchaseFile = document.getElementById("reconcile-erp-purchase-file");
  const salesFileEl = document.getElementById("reconcile-erp-sales-file");

  if (!_rc.vendorFiles.length) {
    toast("거래처 원장 파일을 하나 이상 선택하세요", "error"); return;
  }

  const pFile = purchaseFile?.files?.[0];
  const hasPurchaseCache = document.getElementById("reconcile-purchase-cached")?.style?.display === "flex";
  const sFile = salesFileEl?.files?.[0];

  if (!pFile && !hasPurchaseCache) {
    toast("구매현황 파일을 업로드하거나 캐시가 필요합니다", "error"); return;
  }

  // 확인된 거래처명 + 거래처코드 수집
  const nameInputs = document.querySelectorAll(".rc-vendor-name-input");
  const confirmedNames = Array.from(nameInputs).map(inp => inp.value.trim());
  const confirmedCodes = _rc.vendorFiles.map(v => v.code || "");

  const btn = document.getElementById("btn-reconcile-batch");
  btn.disabled = true;
  btn.innerHTML = "<span class='rc-progress-spinner' style='width:14px;height:14px;display:inline-block;vertical-align:middle;margin-right:6px'></span> 처리 중...";

  const progressArea = document.getElementById("reconcile-batch-progress");
  const progressText = document.getElementById("reconcile-batch-progress-text");
  const progressBar = document.getElementById("reconcile-batch-progress-bar");
  const logContent = document.getElementById("reconcile-log-content");
  progressArea.style.display = "block";
  progressText.textContent = "서버 연결 중...";
  progressBar.style.width = "5%";
  progressBar.style.background = "";
  if (logContent) logContent.innerHTML = "";

  function appendLog(msg, level) {
    if (!logContent) return;
    const line = document.createElement("div");
    line.className = "log-line" + (level ? ` ${level}` : "");
    line.textContent = msg;
    logContent.appendChild(line);
    logContent.scrollTop = logContent.scrollHeight;
  }

  try {
    const formData = new FormData();
    // Don't re-upload files if saved_paths exist (they're already on server from preview)
    const filesToUpload = _rc.vendorFiles.filter(v => !v.saved_path);
    for (const v of filesToUpload) formData.append("vendor_files", v.file);
    if (pFile) formData.append("purchase_file", pFile);
    if (sFile) formData.append("sales_file", sFile);
    formData.append("vendor_names_json", JSON.stringify(confirmedNames));
    formData.append("vendor_codes_json", JSON.stringify(confirmedCodes));
    const savedPaths = _rc.vendorFiles.map(v => v.saved_path).filter(Boolean);
    formData.append("saved_paths_json", JSON.stringify(savedPaths));

    appendLog("📡 서버에 요청 전송 중...");

    const resp = await fetch(API_BASE + "/api/reconcile/batch-reconcile-stream", {
      method: "POST",
      headers: { "Authorization": `Bearer ${api.getToken()}` },
      body: formData,
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `서버 오류 (${resp.status})`);
    }

    // SSE 스트림 파싱
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let finalResult = null;
    let currentEvent = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split("\n");
      buffer = lines.pop(); // 마지막 불완전한 줄 유지
      for (const line of lines) {
        if (line.startsWith("event: ")) {
          currentEvent = line.slice(7).trim();
        } else if (line.startsWith("data: ")) {
          const dataStr = line.slice(6);
          try {
            const data = JSON.parse(dataStr);
            if (currentEvent === "log") {
              appendLog(data.msg, data.level || "");
              progressText.textContent = data.msg.replace(/^[^\s]+\s/, "");
            } else if (currentEvent === "progress") {
              const pct = data.total > 0 ? Math.round((data.current / data.total) * 80) + 15 : 15;
              progressBar.style.width = pct + "%";
              progressText.textContent = `${data.current}/${data.total} 거래처 처리 중...`;
            } else if (currentEvent === "result") {
              finalResult = data;
            }
          } catch (parseErr) { /* skip malformed JSON */ }
          currentEvent = "";
        }
      }
    }

    if (finalResult) {
      _rc.batchResult = finalResult;
      progressBar.style.width = "100%";
      progressText.textContent = "완료!";
      appendLog("✅ 결과 렌더링 중...");

      reconcileCheckErpCache();
      _renderBatchResults(finalResult);
      reconcileSetStep(2);
      toast(`${finalResult.summary.vendor_count}개 거래처 일괄 정산 완료`, "success");
    } else {
      throw new Error("서버에서 결과를 받지 못했습니다");
    }

  } catch (e) {
    progressText.textContent = "오류: " + (e.message || e);
    progressBar.style.width = "100%";
    progressBar.style.background = "var(--danger)";
    appendLog("❌ " + (e.message || e), "error");
    toast("일괄 정산 실패: " + (e.message || e), "error");
  } finally {
    btn.disabled = false;
    btn.innerHTML = "<span>▶</span> 일괄 정산 시작";
  }
}

// ── STEP 2: 결과 렌더링 ──

function _renderBatchResults(result) {
  const s = result.summary;
  const vrs = result.vendor_results || [];
  const totalMemo = vrs.reduce((sum, vr) => sum + (vr.summary?.memo_filtered || 0), 0);
  const totalPayment = vrs.reduce((sum, vr) => sum + (vr.summary?.payment_filtered || 0), 0);
  const totalDiscount = vrs.reduce((sum, vr) => sum + (vr.summary?.discount_absorbed_count || 0), 0);
  const totalReturns = vrs.reduce((sum, vr) => sum + (vr.summary?.return_matched_count || 0) + (vr.summary?.return_unmatched_count || 0), 0);
  const totalRegular = s.total_matched + s.total_unmatched;
  const overallPct = totalRegular > 0 ? Math.round(s.total_matched / totalRegular * 100) : 0;
  const pctColor = overallPct === 100 ? "#16a34a" : overallPct >= 90 ? "#ca8a04" : "#dc2626";

  // 요약 카드
  document.getElementById("reconcile-batch-summary").innerHTML = `
    <div class="rc-overall-rate" style="text-align:center;padding:12px 0 8px;font-size:14px">
      <span style="font-weight:700;font-size:22px;color:${pctColor}">${overallPct}%</span>
      <span style="color:var(--gray-300);margin-left:6px">전체 매칭률</span>
      <span style="color:var(--gray-400);font-size:12px;margin-left:4px">(${s.total_matched}/${totalRegular}건)</span>
    </div>
    <div class="rc-summary-grid">
      <div class="rc-summary-card matched">
        <div class="rc-summary-num">${s.total_matched}</div>
        <div class="rc-summary-label">매칭 완료</div>
      </div>
      <div class="rc-summary-card unmatched">
        <div class="rc-summary-num">${s.total_unmatched}</div>
        <div class="rc-summary-label">미매칭</div>
      </div>
      <div class="rc-summary-card mismatch">
        <div class="rc-summary-num">${s.total_amount_mismatch}</div>
        <div class="rc-summary-label">금액차이</div>
      </div>
      <div class="rc-summary-card vendor">
        <div class="rc-summary-num">${s.vendor_count}</div>
        <div class="rc-summary-label">거래처</div>
      </div>
      ${totalDiscount > 0 ? `<div class="rc-summary-card" style="border-left:3px solid #16a34a">
        <div class="rc-summary-num">${totalDiscount}</div>
        <div class="rc-summary-label">할인반영</div>
      </div>` : ""}
      ${totalReturns > 0 ? `<div class="rc-summary-card" style="border-left:3px solid #8b5cf6">
        <div class="rc-summary-num">${totalReturns}</div>
        <div class="rc-summary-label">반품</div>
      </div>` : ""}
      ${s.total_shipping > 0 ? `<div class="rc-summary-card shipping">
        <div class="rc-summary-num">${s.total_shipping}</div>
        <div class="rc-summary-label">배송료</div>
      </div>` : ""}
      ${totalPayment > 0 ? `<div class="rc-summary-card" style="border-left:3px solid #6b7280">
        <div class="rc-summary-num">${totalPayment}</div>
        <div class="rc-summary-label">결제제외</div>
      </div>` : ""}
      ${totalMemo > 0 ? `<div class="rc-summary-card memo">
        <div class="rc-summary-num">${totalMemo}</div>
        <div class="rc-summary-label">메모제외</div>
      </div>` : ""}
    </div>
    <div class="rc-data-info">
      <span>💾 구매현황 ${s.purchase_total.toLocaleString()}건${s.purchase_from_cache ? " (캐시)" : ""}</span>
      <span>📊 판매현황 ${s.sales_total.toLocaleString()}건${s.sales_from_cache ? " (캐시)" : ""}</span>
      ${s.errors.length ? `<span style="color:var(--danger)">⚠ 오류 ${s.errors.length}건</span>` : ""}
    </div>`;

  // 거래처별 아코디언
  const accordion = document.getElementById("reconcile-vendor-accordion");
  accordion.innerHTML = (result.vendor_results || []).map((vr, vi) => {
    const vs = vr.summary || {};
    const hasError = !!vr.error;
    const vendorTotalOk = vs.vendor_total_match === true;
    const vendorMatchPct = (vs.matched_count + vs.unmatched_count) > 0
      ? Math.round(vs.matched_count / (vs.matched_count + vs.unmatched_count) * 100) : 0;
    // 상태 판단: 100%매칭 or 총액일치 → OK, 80%+ → 주의, 그 외 → 경고
    const isOk = !hasError && (vendorMatchPct === 100 || vendorTotalOk) && vs.amount_mismatch_count === 0;
    const statusCls = hasError ? "status-error" : isOk ? "status-ok" : vendorMatchPct >= 80 ? "status-warn" : "status-error";
    const statusIcon = hasError ? "❌" : isOk ? "✅" : vendorMatchPct >= 80 ? "⚠️" : "❌";

    let detailHTML = "";
    if (!hasError) {
      // 매칭됨
      if (vs.matched_count > 0) {
        const matchedTotal = (vr.matched||[]).reduce((s,r) => s + (r.vendor_item?.amount||0), 0);
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title matched" style="cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>✅ 매칭됨 (${vs.matched_count}건) <span style="font-size:11px;color:var(--gray-300);margin-left:8px">합계 ${matchedTotal.toLocaleString()}원</span><span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += (vr.matched || []).map(r => {
          const v = r.vendor_item || {};
          const e = r.erp_match || {};
          const discAmt = r.discount_absorbed_amount || 0;
          const vAmt = v.amount || 0;
          const eAmt = parseFloat(String(e.total || e["합계"] || e["합 계"] || 0).replace(/,/g, ""));
          const hasDiff = Math.abs(vAmt - eAmt) > 1;
          const conf = Math.round((r.confidence || 0) * 100);
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#16a34a">✓</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${v.date||""}</span>
            <span class="rc-item-meta">${vAmt.toLocaleString()}원</span>
            ${discAmt && hasDiff ? `<span class="rc-item-meta" style="color:#f59e0b;font-size:10px">(할인 ${Math.abs(discAmt).toLocaleString()}원 → ${eAmt.toLocaleString()}원)</span>` : ""}
            <span class="rc-arrow">→</span>
            <span class="rc-erp-name">${e.prod_cd||""} ${e.prod_name||""}</span>
            ${discAmt && hasDiff ? `<span class="rc-item-meta" style="color:#16a34a;font-size:10px;font-weight:600">${conf}%</span>` : ""}
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 할인반영 (매출할인이 매입단가에 반영됨)
      if ((vr.discount_absorbed||[]).length > 0) {
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title" style="color:#16a34a;cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>💰 할인반영 (${vr.discount_absorbed.length}건) <span style="font-size:10px;color:var(--gray-400)">— 할인이 매입단가에 이미 반영됨</span><span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += vr.discount_absorbed.map(r => {
          const v = r.vendor_item || {};
          const pname = v.product_name || v.product_category || "";
          const absorbedBy = r.absorbed_by || "";
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#16a34a">✓</span>
            <span class="rc-item-name">${pname}</span>
            <span class="rc-item-meta">${v.date||""}</span>
            <span class="rc-item-meta">${(v.amount||0).toLocaleString()}원</span>
            ${absorbedBy ? `<span class="rc-arrow">→</span><span class="rc-item-meta" style="color:var(--gray-400);font-size:10px">${absorbedBy} 단가에 반영</span>` : ""}
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 반품 매칭
      if ((vr.return_matched||[]).length > 0 || (vr.return_unmatched||[]).length > 0) {
        const rmCount = (vr.return_matched||[]).length;
        const ruCount = (vr.return_unmatched||[]).length;
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title" style="color:#8b5cf6;cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>🔄 반품/교환 (매칭 ${rmCount}건${ruCount > 0 ? `, 미매칭 ${ruCount}건` : ""}) <span style="font-size:10px;color:var(--gray-400)">— 거래처 매입 항목 ↔ ERP 음수 매입전표</span><span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += (vr.return_matched||[]).map(r => {
          const v = r.vendor_item || {};
          const e = r.erp_match || {};
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#8b5cf6">↩</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${v.date||""}</span>
            <span class="rc-item-meta">${(v.amount||0).toLocaleString()}원</span>
            <span class="rc-arrow">→</span>
            <span class="rc-erp-name">${e.prod_cd||""} ${e.prod_name||""}</span>
          </div>`;
        }).join("");
        detailHTML += (vr.return_unmatched||[]).map(r => {
          const v = r.vendor_item || {};
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#dc2626">✗</span>
            <span class="rc-item-name">${v.product_name||""} (반품-미매칭)</span>
            <span class="rc-item-meta">${v.date||""}</span>
            <span class="rc-item-meta">${(v.amount||0).toLocaleString()}원</span>
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 결제성 항목 (필터링됨)
      if ((vr.payment_items||[]).length > 0) {
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title" style="color:#6b7280;cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>💳 결제성 제외 (${vr.payment_items.length}건) <span style="font-size:10px;color:var(--gray-400)">— 입금/출금/기타 결제 항목</span><span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += vr.payment_items.map(p => {
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#6b7280">─</span>
            <span class="rc-item-name">${p.product_name||p.tx_type||""}</span>
            <span class="rc-item-meta">${p.date||""}</span>
            <span class="rc-item-meta">${(p.amount||0).toLocaleString()}원</span>
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 누락
      if (vs.unmatched_count > 0) {
        const unmatchedTotal = (vr.unmatched||[]).reduce((s,r) => s + (r.vendor_item?.amount||0), 0);
        const totalMatchNote = vr.vendor_total_match
          ? `<span style="color:#16a34a;font-size:11px;margin-left:8px">✅ 거래처 총액 일치 — 개별 항목 차이는 무시 가능</span>`
          : "";
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title unmatched" style="cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>❌ 매입전표 누락 (${vs.unmatched_count}건) <span style="font-size:11px;color:#dc2626;margin-left:4px">합계 ${unmatchedTotal.toLocaleString()}원</span>${totalMatchNote}<span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += (vr.unmatched || []).map(r => {
          const v = r.vendor_item || {};
          const txType = v.tx_type || "";
          const pname = v.product_name || v.product_category || "";
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#dc2626">✗</span>
            <span class="rc-item-name">${pname}${txType ? ` (${txType})` : ""}</span>
            <span class="rc-item-meta">${v.date||""}</span>
            <span class="rc-item-meta">수량 ${v.qty||0}</span>
            <span class="rc-item-meta">${(v.amount||0).toLocaleString()}원</span>
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 판매이력
      if ((vr.sales_check||[]).length > 0) {
        const withSales = vr.sales_check.filter(sc => sc.has_sales_history);
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title sales" style="cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>🔍 판매이력 확인 (${withSales.length}/${vr.sales_check.length}건)<span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += vr.sales_check.map((sc, sci) => {
          const v = sc.vendor_item || {};
          const best = sc.best_candidate;
          return `<div class="rc-detail-row">
            <span class="rc-icon">${sc.has_sales_history ? "📦" : "❓"}</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${v.date||""} | ${(v.amount||0).toLocaleString()}원</span>
            ${best ? `<span class="rc-arrow">→</span><span class="rc-erp-name">${best.product_code||""} ${best.product_name||""} (${Math.round((best.confidence||0)*100)}%)</span>` : ""}
            ${sc.has_sales_history ? `<label style="margin-left:auto;font-size:11px;cursor:pointer;white-space:nowrap"><input type="checkbox" class="batch-include-check" data-vi="${vi}" data-sci="${sci}" checked> 입력</label>` : ""}
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 배송료
      if (vs.shipping_count > 0) {
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title shipping" style="cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>🚚 배송료 (${vs.shipping_count}건)<span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += (vr.shipping_items||[]).map(si => {
          const v = si.vendor_item||{};
          return `<div class="rc-detail-row">
            <span class="rc-icon">🚚</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${(v.amount||0).toLocaleString()}원</span>
            <span class="rc-item-meta">${si.erp_match ? "✓ ERP매칭" : "미매칭"}</span>
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 금액차이
      if (vs.amount_mismatch_count > 0) {
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title mismatch" style="cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>⚠️ 금액 불일치 (${vs.amount_mismatch_count}건)<span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += (vr.amount_mismatches||[]).map(r => {
          const v = r.vendor_item||{};
          const vAmt = r.vendor_amount || v.amount || 0;
          const eAmt = r.erp_amount || 0;
          const diff = r.amount_diff || 0;
          return `<div class="rc-detail-row" style="flex-wrap:wrap;gap:4px">
            <span class="rc-icon">⚠️</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${v.date||""}</span>
            <span class="rc-item-meta" style="color:#6366f1">원장 ${vAmt.toLocaleString()}원</span>
            <span class="rc-arrow">→</span>
            <span class="rc-item-meta" style="color:#0891b2">매입 ${eAmt.toLocaleString()}원</span>
            <span class="rc-item-meta" style="color:${diff > 0 ? '#dc2626' : '#16a34a'};font-weight:600">차액 ${diff > 0 ? "+" : ""}${diff.toLocaleString()}원</span>
          </div>`;
        }).join("");
        detailHTML += "</div></div>";
      }

      // 초과 ERP 항목 (거래처원장에 없지만 매입전표에 있는 항목)
      if ((vr.excess_erp||[]).length > 0) {
        detailHTML += `<div class="rc-detail-section"><div class="rc-detail-title" style="color:#7c3aed;cursor:pointer" onclick="rcToggleSection(this)"><span class="rc-sa" style="font-size:10px;margin-right:6px;display:inline-block;width:10px;transition:transform 0.2s">▸</span>📋 매입전표 초과 (${vr.excess_erp.length}건) <span style="font-size:10px;color:var(--gray-400)">— 거래처원장에 없는 매입전표</span><span class="rc-sh" style="margin-left:auto;font-size:10px;color:var(--gray-400);white-space:nowrap">상세보기 ▸</span></div><div style="display:none">`;
        detailHTML += vr.excess_erp.slice(0, 10).map(e => {
          const eName = e.prod_name || e["품명 및 모델"] || "";
          const eCode = e.prod_cd || e["품목코드"] || "";
          const eDate = e.date || e["월/일"] || "";
          const eAmt = parseFloat(String(e.total || e["합계"] || e["합 계"] || 0).replace(/,/g,""));
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#7c3aed">+</span>
            <span class="rc-item-name">${eCode} ${eName}</span>
            <span class="rc-item-meta">${eDate}</span>
            <span class="rc-item-meta">${eAmt.toLocaleString()}원</span>
          </div>`;
        }).join("");
        if (vr.excess_erp.length > 10) detailHTML += `<div style="color:var(--gray-400);font-size:11px;padding:4px 24px">... 외 ${vr.excess_erp.length - 10}건</div>`;
        detailHTML += "</div></div>";
      }

      // 거래처 총액 비교 (항상 표시)
      if (typeof vr.vendor_ledger_total === "number" && typeof vr.erp_purchase_total === "number") {
        const vTotal = vr.vendor_ledger_total;
        const eTotal = vr.erp_purchase_total;
        const tDiff = vTotal - eTotal;
        const tMatch = Math.abs(tDiff) <= 1;
        detailHTML += `<div class="rc-detail-section" style="border-top:1px solid var(--gray-700);padding-top:8px;margin-top:4px">
          <div style="font-size:12px;display:flex;gap:12px;align-items:center;flex-wrap:wrap">
            <span style="font-weight:600">${tMatch ? "✅" : "⚠️"} 거래처 총액:</span>
            <span style="color:#6366f1">원장 ${vTotal.toLocaleString()}원</span>
            <span style="color:#0891b2">매입전표 ${eTotal.toLocaleString()}원</span>
            ${!tMatch ? `<span style="color:#dc2626;font-weight:600">차액 ${tDiff > 0 ? "+" : ""}${tDiff.toLocaleString()}원</span>` : `<span style="color:#16a34a">일치</span>`}
          </div>
        </div>`;
      }
    }

    return `
    <div class="rc-vendor-card ${statusCls}" id="rc-vendor-${vi}">
      <div class="rc-vendor-header" onclick="document.getElementById('rc-vendor-${vi}').classList.toggle('open')">
        <div>
          <div class="rc-vendor-title">${statusIcon} ${vr.vendor_name || vr.filename}</div>
          <div class="rc-vendor-stats">
            ${hasError ? `<span style="color:var(--danger)">${vr.error}</span>` : (() => {
              const regularTotal = vs.matched_count + vs.unmatched_count;
              const matchPct = regularTotal > 0 ? Math.round(vs.matched_count / regularTotal * 100) : 0;
              const pctColor = matchPct === 100 ? "#16a34a" : matchPct >= 80 ? "#ca8a04" : "#dc2626";
              return `
              <span class="rc-vendor-stat good">매칭 ${vs.matched_count}/${regularTotal} <b style="color:${pctColor}">(${matchPct}%)</b></span>
              ${vs.unmatched_count > 0 ? `<span class="rc-vendor-stat bad">누락 ${vs.unmatched_count}</span>` : ""}
              ${(vs.return_matched_count||0) > 0 ? `<span class="rc-vendor-stat" style="color:#8b5cf6">반품 ${vs.return_matched_count}</span>` : ""}
              ${(vs.payment_filtered||0) > 0 ? `<span class="rc-vendor-stat" style="color:#6b7280">결제제외 ${vs.payment_filtered}</span>` : ""}
              ${vs.shipping_count > 0 ? `<span class="rc-vendor-stat">배송 ${vs.shipping_count}</span>` : ""}
              <span class="rc-vendor-stat">ERP ${vs.purchase_filtered}건</span>
            `;
            })()}
          </div>
          ${!hasError && typeof vr.vendor_ledger_total === "number" && typeof vr.erp_purchase_total === "number" ? (() => {
            const vT = vr.vendor_ledger_total;
            const eT = vr.erp_purchase_total;
            const tMatch = Math.abs(vT - eT) <= 1;
            const tDiff = vT - eT;
            return `<div style="font-size:11px;margin-top:4px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;color:var(--gray-400)">
              <span style="font-weight:600">${tMatch ? "✅" : "⚠️"} 거래처 총액:</span>
              <span style="color:#6366f1">원장 ${vT.toLocaleString()}원</span>
              <span style="color:#0891b2">매입전표 ${eT.toLocaleString()}원</span>
              ${!tMatch ? `<span style="color:#dc2626;font-weight:600">차액 ${tDiff > 0 ? "+" : ""}${tDiff.toLocaleString()}원</span>` : `<span style="color:#16a34a">일치</span>`}
            </div>`;
          })() : ""}
        </div>
        <span class="rc-vendor-toggle">▼</span>
      </div>
      <div class="rc-vendor-detail">${hasError ? "" : detailHTML}</div>
    </div>`;
  }).join("");
}

// ── 세부 섹션 토글 ──
function rcToggleSection(el) {
  const body = el.nextElementSibling;
  if (!body) return;
  const arrow = el.querySelector('.rc-sa');
  const hint = el.querySelector('.rc-sh');
  if (body.style.display === 'none') {
    body.style.display = '';
    if (arrow) arrow.textContent = '▾';
    if (hint) hint.textContent = '접기 ▾';
  } else {
    body.style.display = 'none';
    if (arrow) arrow.textContent = '▸';
    if (hint) hint.textContent = '상세보기 ▸';
  }
}

// ── 엑셀 다운로드 ──
async function reconcileBatchDownloadExcel() {
  if (!_rc.batchResult?.session_id) {
    toast("비교 결과가 없습니다", "error"); return;
  }
  try {
    const resp = await fetch(API_BASE + `/api/reconcile/download-result/${_rc.batchResult.session_id}`, {
      headers: { "Authorization": `Bearer ${api.getToken()}` }
    });
    if (!resp.ok) throw new Error(`다운로드 실패 (${resp.status})`);
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `매입정산_일괄결과_${_rc.batchResult.session_id}.xlsx`;
    a.click();
    URL.revokeObjectURL(url);
    toast("엑셀 다운로드 완료", "success");
  } catch (e) {
    toast("다운로드 실패: " + (e.message||e), "error");
  }
}

// ── STEP 2 → STEP 3: 매입전표 입력 준비 ──
function reconcileBatchPrepareInput() {
  if (!_rc.batchResult) { toast("비교 결과가 없습니다", "error"); return; }

  _rc.purchaseQueue = [];
  const year = new Date().getFullYear().toString();

  for (const vr of _rc.batchResult.vendor_results || []) {
    if (vr.error) continue;
    const vendorCode = vr.vendor_code || "";
    const vendorName = vr.vendor_name || "";

    (vr.sales_check || []).forEach((sc, sci) => {
      if (!sc.has_sales_history || !sc.best_candidate) return;
      const vi = _rc.batchResult.vendor_results.indexOf(vr);
      const cb = document.querySelector(`.batch-include-check[data-vi="${vi}"][data-sci="${sci}"]`);
      if (cb && !cb.checked) return;

      const v = sc.vendor_item || {};
      const c = sc.best_candidate;

      let ioDate = "";
      if (v.date) {
        const parts = v.date.replace(/[\/\-]/g, ".").split(".");
        if (parts.length >= 2) {
          ioDate = year + parts[0].padStart(2, "0") + parts[1].padStart(2, "0");
        }
      }

      _rc.purchaseQueue.push({
        io_date: ioDate, cust_code: vendorCode, cust_name: vendorName,
        wh_cd: "", prod_cd: c.product_code || "",
        prod_name: c.product_name || v.product_name || "", size_des: "",
        qty: v.qty || 1, price: v.unit_price || 0,
        supply_amt: v.amount || 0, vat_amt: 0,
        remarks: "매입정산 자동입력",
      });
    });
  }

  if (!_rc.purchaseQueue.length) {
    // 모든 거래처 총액이 일치하면 입력할 게 없는 것이 정상
    const allTotalMatch = (_rc.batchResult.vendor_results || []).every(vr =>
      vr.error || vr.vendor_total_match === true || (vr.summary?.unmatched_count || 0) === 0
    );
    if (allTotalMatch) {
      toast("모든 거래처 총액이 일치하여 추가 입력이 필요 없습니다.", "success");
    } else {
      toast("입력할 항목이 없습니다. 판매이력이 있는 미매칭 항목이 있으면 체크 후 시도하세요.", "info");
    }
    return;
  }

  _renderPurchaseEditCards();
  reconcileSetStep(3);
  toast(`${_rc.purchaseQueue.length}건 매입전표 입력 준비`, "success");
}

function _renderPurchaseEditCards() {
  const listEl = document.getElementById("reconcile-purchase-list");
  if (!_rc.purchaseQueue.length) {
    listEl.innerHTML = '<p style="color:var(--gray-400);font-size:13px">입력할 항목이 없습니다.</p>';
    return;
  }
  listEl.innerHTML = _rc.purchaseQueue.map((item, i) => `
    <div class="rc-purchase-card" id="purchase-edit-${i}">
      <div class="rc-purchase-card-header">
        <span class="rc-purchase-card-title">#${i+1} [${item.cust_name}] ${item.prod_name}</span>
        <button onclick="reconcileRemoveItem(${i})" class="rc-purchase-card-delete">&times;</button>
      </div>
      <div class="rc-edit-grid">
        <div class="rc-edit-field"><label>전표일자</label><input value="${item.io_date}" onchange="_rc.purchaseQueue[${i}].io_date=this.value" placeholder="YYYYMMDD"></div>
        <div class="rc-edit-field"><label>거래처코드</label><input value="${item.cust_code}" onchange="_rc.purchaseQueue[${i}].cust_code=this.value"></div>
        <div class="rc-edit-field"><label>거래처명</label><input value="${item.cust_name}" onchange="_rc.purchaseQueue[${i}].cust_name=this.value"></div>
        <div class="rc-edit-field"><label>품목코드</label><input value="${item.prod_cd}" onchange="_rc.purchaseQueue[${i}].prod_cd=this.value"></div>
        <div class="rc-edit-field"><label>품목명</label><input value="${item.prod_name}" onchange="_rc.purchaseQueue[${i}].prod_name=this.value"></div>
        <div class="rc-edit-field"><label>규격</label><input value="${item.size_des}" onchange="_rc.purchaseQueue[${i}].size_des=this.value"></div>
        <div class="rc-edit-field"><label>수량</label><input type="number" value="${item.qty}" onchange="_rc.purchaseQueue[${i}].qty=+this.value"></div>
        <div class="rc-edit-field"><label>단가</label><input type="number" value="${item.price}" onchange="_rc.purchaseQueue[${i}].price=+this.value"></div>
        <div class="rc-edit-field"><label>공급가</label><input type="number" value="${item.supply_amt}" onchange="_rc.purchaseQueue[${i}].supply_amt=+this.value"></div>
        <div class="rc-edit-field"><label>부가세</label><input type="number" value="${item.vat_amt}" onchange="_rc.purchaseQueue[${i}].vat_amt=+this.value"></div>
      </div>
    </div>
  `).join("");
}

function reconcileRemoveItem(idx) {
  _rc.purchaseQueue.splice(idx, 1);
  _renderPurchaseEditCards();
}

// ── STEP 3 → STEP 4: ERP 전송 ──
async function reconcileSubmitERP() {
  if (!_rc.purchaseQueue.length) {
    toast("입력할 항목이 없습니다", "error"); return;
  }

  const invalid = _rc.purchaseQueue.filter(p => !p.io_date || !p.cust_code || !p.prod_cd);
  if (invalid.length) {
    toast(`${invalid.length}건의 필수 항목(전표일자, 거래처코드, 품목코드)이 누락되었습니다`, "error"); return;
  }

  if (!confirm(`${_rc.purchaseQueue.length}건의 매입전표를 ERP에 입력하시겠습니까?`)) return;

  const btn = document.getElementById("btn-reconcile-submit");
  btn.disabled = true;
  btn.textContent = "전송 중...";

  try {
    const result = await api.post("/api/reconcile/save-purchase", {
      items: _rc.purchaseQueue, upload_ser_no: "1",
    });

    const resultEl = document.getElementById("reconcile-result");
    const cls = result.status === "success" ? "success" : result.status === "partial" ? "partial" : "error";

    resultEl.innerHTML = `
      <div class="reconcile-result-box ${cls}">
        <div style="font-size:36px;margin-bottom:8px">${cls === "success" ? "✓" : cls === "partial" ? "⚠" : "✕"}</div>
        <div style="font-size:18px;font-weight:700;margin-bottom:4px">${cls === "success" ? "입력 완료" : cls === "partial" ? "일부 성공" : "입력 실패"}</div>
        <div style="font-size:14px">전체 ${result.total}건 | 성공 ${result.success}건 | 실패 ${result.failed}건</div>
      </div>
      ${(result.results||[]).map(r => `
        <div style="margin-top:8px;padding:10px 14px;background:var(--gray-50);border-radius:8px;font-size:12px">
          거래처: ${r.cust_code} — ${r.status === "success"
            ? `<span style="color:#166534">성공 (${r.success}건) ${r.slip_nos ? '전표: ' + r.slip_nos : ''}</span>`
            : `<span style="color:#dc2626">실패: ${r.error_message || ""}</span>`}
        </div>
      `).join("")}`;

    reconcileSetStep(4);
    toast(cls === "success" ? "ERP 입력 완료" : "ERP 입력 결과를 확인하세요", cls === "success" ? "success" : "error");
  } catch (e) {
    toast("ERP 전송 실패: " + (e.message||e), "error");
  } finally {
    btn.disabled = false;
    btn.textContent = "ERP 구매입력 전송";
  }
}

function reconcileReset() {
  _rc.step = 1;
  _rc.batchResult = null;
  _rc.purchaseQueue = [];
  _rc.vendorFiles = [];  // 거래처 파일 목록 초기화
  reconcileSetStep(1);

  // 파일 입력 초기화
  const vendorFiles = document.getElementById("reconcile-vendor-files");
  if (vendorFiles) vendorFiles.value = "";
  const vfList = document.getElementById("reconcile-vendor-file-list");
  if (vfList) vfList.innerHTML = "";
  const pFile = document.getElementById("reconcile-erp-purchase-file");
  if (pFile) pFile.value = "";
  const sFile = document.getElementById("reconcile-erp-sales-file");
  if (sFile) sFile.value = "";

  // 거래처 확인 영역 초기화
  const vendorConfirm = document.getElementById("reconcile-vendor-confirm");
  if (vendorConfirm) vendorConfirm.style.display = "none";
  const vendorNameList = document.getElementById("reconcile-vendor-name-list");
  if (vendorNameList) vendorNameList.innerHTML = "";

  // 진행바 초기화
  const progress = document.getElementById("reconcile-batch-progress");
  if (progress) progress.style.display = "none";

  // 결과 영역 초기화
  const summary = document.getElementById("reconcile-batch-summary");
  if (summary) summary.innerHTML = "";
  const accordion = document.getElementById("reconcile-vendor-accordion");
  if (accordion) accordion.innerHTML = "";

  reconcileCheckErpCache();
}

function reconcileNewMatch() {
  // 거래처 파일만 초기화 (구매/판매 캐시는 유지)
  _rc.vendorFiles = [];
  _rc.batchResult = null;
  _rc.purchaseQueue = [];
  reconcileSetStep(1);

  const vendorFiles = document.getElementById("reconcile-vendor-files");
  if (vendorFiles) vendorFiles.value = "";
  const vfList = document.getElementById("reconcile-vendor-file-list");
  if (vfList) vfList.innerHTML = "";
  const vendorConfirm = document.getElementById("reconcile-vendor-confirm");
  if (vendorConfirm) vendorConfirm.style.display = "none";
  const vendorNameList = document.getElementById("reconcile-vendor-name-list");
  if (vendorNameList) vendorNameList.innerHTML = "";
  const progress = document.getElementById("reconcile-batch-progress");
  if (progress) progress.style.display = "none";
  const summary = document.getElementById("reconcile-batch-summary");
  if (summary) summary.innerHTML = "";
  const accordion = document.getElementById("reconcile-vendor-accordion");
  if (accordion) accordion.innerHTML = "";

  toast("새 매칭을 시작합니다. 거래처 원장을 업로드하세요.", "info");
}


