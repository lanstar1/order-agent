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
    rebate:       "리베이트",
    map_monitor:  "지도가 감시",
    mail_agent:   "📧 메일 자동화",
    datalab:      "📊 데이터랩",
  }[pageId] || "";
  // AI 상담 페이지 진입 시 초기화
  if (pageId === "aicc") initAiccTab();
  // CS/RMA 페이지 진입 시 초기화
  if (pageId === "cs_rma") csInit();
  // MAP 감시 페이지 진입 시 초기화
  if (pageId === "map_monitor") initMapMonitor();
  // 메일 자동화 페이지 진입 시 초기화
  if (pageId === "mail_agent") initMailAutoPage();
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
  // 리베이트 페이지 진입 시 초기화
  if (pageId === "rebate" && typeof initRebatePage === "function") initRebatePage();
  // 데이터랩 페이지 진입 시 초기화
  if (pageId === "datalab") initDatalabPage();
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
      <thead style="position:sticky;top:0;z-index:10;background:#1e293b"><tr style="background:#1e293b;color:#fff">
        <th style="width:60px">탭</th>
        <th style="width:100px">주문번호</th>
        <th>카테고리</th>
        <th style="min-width:180px">모델명</th>
        <th>설명</th>
        <th style="width:60px;text-align:right">수량</th>
        <th style="width:50px">단위</th>
        <th style="width:120px">주문일</th>
        <th style="width:100px">선적일</th>
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
        <td style="font-size:11px;${item.shipping_date?'color:#7c3aed;font-weight:600':''}">${item.shipping_date || '-'}</td>
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
//  CS / RMA 관리 v2.0
// ═══════════════════════════════════════════════════
let _csStatus = "";
let _csPage = 1;
let _csSearch = "";
let _csChannel = "";
let _csType = "";
let _csReason = "";
let _csView = "kanban";
let _csOptions = null;

const CS_CHANNEL_COLORS = {
  "스마트스토어": "#16a34a", "G마켓": "#2563eb", "옥션": "#dc2626",
  "쿠팡": "#ea580c", "오늘의집": "#7c3aed", "나비엠알오": "#6b7280",
  "자사몰": "#0891b2", "기타": "#9ca3af"
};

async function csInit() {
  _csStatus = "";
  _csPage = 1;
  _csSearch = "";
  _csChannel = "";
  _csType = "";
  _csReason = "";
  const searchInput = document.getElementById("cs-search-input");
  if (searchInput) searchInput.value = "";

  // What's New 팝업 체크
  if (!localStorage.getItem("cs_whatsnew_v22_dismissed")) {
    document.getElementById("cs-whatsnew-overlay").style.display = "block";
  }

  // 옵션 로드 (드롭다운용)
  try {
    _csOptions = await api.get("/api/cs/options");
    csPopulateFilters();
  } catch(e) { console.error("CS options load error:", e); }

  await csLoadDashboard();
  csRenderView();
}

function csPopulateFilters() {
  if (!_csOptions) return;
  const chSel = document.getElementById("cs-filter-channel");
  const tySel = document.getElementById("cs-filter-type");
  const reSel = document.getElementById("cs-filter-reason");
  if (chSel) {
    chSel.innerHTML = '<option value="">전체 채널</option>' +
      (_csOptions.sales_channels||[]).map(c => `<option value="${c}">${c}</option>`).join("");
  }
  if (tySel) {
    tySel.innerHTML = '<option value="">전체 유형</option>' +
      (_csOptions.cs_types||[]).map(c => `<option value="${c}">${c}</option>`).join("");
  }
  if (reSel) {
    reSel.innerHTML = '<option value="">전체 사유</option>' +
      (_csOptions.reason_categories||[]).map(c => `<option value="${c}">${c}</option>`).join("");
  }
}

async function csLoadDashboard() {
  try {
    const stats = await api.get("/api/cs/stats");
    csRenderStatsBar(stats);
    csRenderChannelPanel(stats);
    csRenderReasonPanel(stats);
  } catch(e) {
    console.error("CS stats error:", e);
    const bar = document.getElementById("cs-stats-bar");
    if (bar) bar.innerHTML = '<div style="font-size:12px;color:#9ca3af">통계 로딩 실패</div>';
  }
}

function csRenderStatsBar(stats) {
  const bar = document.getElementById("cs-stats-bar");
  if (!bar) return;
  const sc = stats.status_counts || {};
  const tc = stats.type_counts || {};
  const processing = (sc["접수완료"]||0) + (sc["물류수령"]||0) + (sc["기술인계"]||0) + (sc["테스트완료"]||0);
  const items = [
    { label: "전체", num: stats.total||0, color: "#111827", filter: "" },
    { label: "처리중", num: processing, color: "#2563eb", filter: "active" },
    { label: "미출고", num: tc["미출고"]||0, color: "#ea580c", filter: "backorder" },
    { label: "지연 (7일+)", num: stats.overdue_count||0, color: "#ef4444", filter: "overdue" },
    { label: "오늘 접수", num: stats.today_count||0, color: "#7c3aed", filter: "today" },
    { label: "평균 처리일", num: (stats.avg_resolution_days||0)+"일", color: "#0891b2", filter: "" },
  ];
  bar.innerHTML = items.map(i => `
    <div class="cs-stat-card" onclick="csStatCardClick('${i.filter}')">
      <div class="num" style="color:${i.color}">${i.num}</div>
      <div class="label">${i.label}</div>
    </div>`).join("");
}

function csRenderChannelPanel(stats) {
  const panel = document.getElementById("cs-channel-panel");
  if (!panel) return;
  const ch = stats.channel_counts || {};
  const active = stats.channel_active || {};
  if (Object.keys(ch).length === 0) {
    panel.innerHTML = '<div style="font-size:13px;color:#9ca3af;text-align:center;padding:20px">채널 정보가 등록된 티켓이 없습니다</div>';
    return;
  }
  panel.innerHTML = Object.entries(ch).map(([name, cnt]) => {
    const color = CS_CHANNEL_COLORS[name] || "#9ca3af";
    const act = active[name] || 0;
    return `<div class="cs-ch-row" onclick="csFilterByChannel('${name}')" style="cursor:pointer">
      <span class="cs-ch-dot" style="background:${color}"></span>
      <span class="cs-ch-name">${name}</span>
      ${act > 0 ? `<span class="cs-ch-active">${act}건 처리중</span>` : ''}
      <span class="cs-ch-cnt">${cnt}</span>
    </div>`;
  }).join("");
}

function csRenderReasonPanel(stats) {
  const panel = document.getElementById("cs-reason-panel");
  if (!panel) return;
  const rc = stats.reason_counts || {};
  const total = Object.values(rc).reduce((a,b)=>a+b, 0);
  if (total === 0) {
    panel.innerHTML = '<div style="font-size:13px;color:#9ca3af;text-align:center;padding:20px">사유 정보가 등록된 티켓이 없습니다</div>';
    return;
  }
  const reasonColors = { "파손 및 불량":"#ef4444", "단순 변심":"#f59e0b", "주문 실수":"#3b82f6", "오배송 및 지연":"#6b7280", "재고 부족":"#8b5cf6", "기타":"#9ca3af" };
  panel.innerHTML = Object.entries(rc).map(([name, cnt]) => {
    const pct = total > 0 ? Math.round(cnt/total*100) : 0;
    return `<div class="cs-ch-row" onclick="csFilterByReason('${name}')" style="cursor:pointer">
      <span class="cs-ch-dot" style="background:${reasonColors[name]||'#9ca3af'}"></span>
      <span class="cs-ch-name">${name}</span>
      <span style="font-size:11px;color:#9ca3af">${pct}%</span>
      <span class="cs-ch-cnt">${cnt}</span>
    </div>`;
  }).join("") +
  `<div style="margin-top:8px;padding-top:8px;border-top:1px solid #f3f4f6;font-size:12px;color:#9ca3af;display:flex;justify-content:space-between">
    <span>평균 처리: ${stats.avg_resolution_days||0}일</span><span>지연: ${stats.overdue_count||0}건</span>
  </div>`;
}

function csFilterByChannel(ch) {
  document.getElementById("cs-filter-channel").value = ch;
  csApplyFilters();
}
function csFilterByReason(r) {
  document.getElementById("cs-filter-reason").value = r;
  csApplyFilters();
}

function csApplyFilters() {
  _csChannel = document.getElementById("cs-filter-channel")?.value || "";
  _csType = document.getElementById("cs-filter-type")?.value || "";
  _csReason = document.getElementById("cs-filter-reason")?.value || "";
  _csSearch = document.getElementById("cs-search-input")?.value?.trim() || "";
  _csPage = 1;
  csRenderView();
}

function csSwitchView(el, view) {
  _csView = view;
  el.parentElement.querySelectorAll(".cs-pipe-tab").forEach(t => t.classList.remove("active"));
  el.classList.add("active");
  document.getElementById("cs-kanban-board").style.display = view === "kanban" ? "flex" : "none";
  document.getElementById("cs-list-view").style.display = view === "list" ? "block" : "none";
  document.getElementById("cs-backorder-view").style.display = view === "backorder" ? "block" : "none";
  csRenderView();
}

async function csRenderView() {
  if (_csView === "kanban") {
    await csLoadKanban();
  } else if (_csView === "backorder") {
    await csLoadBackorder();
  } else {
    await csLoadTickets();
  }
}

// ── 미출고/지연 뷰 ──
let _csBackorderStatus = "";
function csBackorderTab(el, status) {
  _csBackorderStatus = status;
  el.parentElement.querySelectorAll(".cs-pipe-tab").forEach(t => t.classList.remove("active"));
  el.classList.add("active");
  csLoadBackorder();
}

async function csLoadBackorder() {
  const list = document.getElementById("cs-backorder-list");
  if (!list) return;
  const params = new URLSearchParams({ size: "200" });
  if (_csBackorderStatus) params.set("status", _csBackorderStatus);
  if (_csChannel) params.set("channel", _csChannel);
  if (_csSearch) params.set("search", _csSearch);
  try {
    const res = await api.get(`/api/cs/backorders?${params}`);
    const items = res.backorders || [];
    const sc = res.status_counts || {};
    const cc = res.channel_counts || {};

    // 통계 바
    let statsHtml = `<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">
      <div class="cs-stat-card" onclick="csBackorderTab(document.querySelector('[data-bostatus=\\'\\']'),'')"><div class="num">${res.total||0}</div><div class="label">전체</div></div>
      <div class="cs-stat-card"><div class="num" style="color:#ea580c">${sc["미출고"]||0}</div><div class="label">미출고</div></div>
      <div class="cs-stat-card"><div class="num" style="color:#16a34a">${sc["출고완료"]||0}</div><div class="label">출고완료</div></div>
      <div class="cs-stat-card"><div class="num" style="color:#6b7280">${sc["취소"]||0}</div><div class="label">취소</div></div>
    </div>`;

    if (!items.length) {
      list.innerHTML = statsHtml + '<div style="text-align:center;padding:40px;color:#9ca3af">미출고 내역이 없습니다.<br><br><button class="btn btn-primary" onclick="csShowBackorderForm()" style="font-size:13px">+ 미출고 접수</button></div>';
      return;
    }

    // 테이블
    let tbl = `<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="background:#f9fafb;border-bottom:2px solid #e5e7eb">
        <th style="padding:8px 10px;text-align:left;font-weight:600;color:#374151;white-space:nowrap">사이트</th>
        <th style="padding:8px 10px;text-align:left;font-weight:600;color:#374151;white-space:nowrap">주문확인일</th>
        <th style="padding:8px 10px;text-align:left;font-weight:600;color:#374151;white-space:nowrap">주문번호</th>
        <th style="padding:8px 10px;text-align:left;font-weight:600;color:#374151;white-space:nowrap">수취인명</th>
        <th style="padding:8px 10px;text-align:left;font-weight:600;color:#374151;white-space:nowrap">상품명</th>
        <th style="padding:8px 10px;text-align:left;font-weight:600;color:#374151;white-space:nowrap">옵션</th>
        <th style="padding:8px 6px;text-align:center;font-weight:600;color:#374151">수량</th>
        <th style="padding:8px 10px;text-align:center;font-weight:600;color:#374151">처리</th>
        <th style="padding:8px 6px;text-align:center;font-weight:600;color:#374151">관리</th>
      </tr></thead><tbody>`;

    const statusStyles = {"미출고":"background:#fef3c7;color:#92400e","출고완료":"background:#d1fae5;color:#065f46","취소":"background:#f3f4f6;color:#6b7280"};
    items.forEach(b => {
      const chColor = CS_CHANNEL_COLORS[b.sales_channel] || "#9ca3af";
      tbl += `<tr style="border-bottom:1px solid #f3f4f6;transition:background .1s" onmouseover="this.style.background='#f9fafb'" onmouseout="this.style.background=''">
        <td style="padding:8px 10px"><span class="cs-ch-badge" style="background:${chColor}15;color:${chColor}">${b.sales_channel||'-'}</span></td>
        <td style="padding:8px 10px;white-space:nowrap;color:#6b7280">${(b.order_date||'').slice(0,10)}</td>
        <td style="padding:8px 10px;font-size:12px;color:#374151;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${b.order_number||''}">${b.order_number||'-'}</td>
        <td style="padding:8px 10px;font-weight:500;color:#111827;white-space:nowrap">${b.recipient_name}</td>
        <td style="padding:8px 10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${b.product_name}">${b.product_name}</td>
        <td style="padding:8px 10px;font-size:12px;color:#6b7280;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${b.option_info||''}">${b.option_info||'-'}</td>
        <td style="padding:8px 6px;text-align:center">${b.quantity}</td>
        <td style="padding:8px 10px;text-align:center">
          <select onchange="csUpdateBackorderStatus(${b.id},this.value)" style="padding:2px 6px;border:1px solid #d1d5db;border-radius:6px;font-size:12px;font-weight:600;${statusStyles[b.status]||''}">
            <option value="미출고" ${b.status==='미출고'?'selected':''}>미출고</option>
            <option value="출고완료" ${b.status==='출고완료'?'selected':''}>출고완료</option>
            <option value="취소" ${b.status==='취소'?'selected':''}>취소</option>
          </select>
        </td>
        <td style="padding:8px 6px;text-align:center">
          <button onclick="csEditBackorder(${b.id})" style="background:none;border:none;cursor:pointer;font-size:14px" title="수정">✏️</button>
          <button onclick="csDeleteBackorder(${b.id})" style="background:none;border:none;cursor:pointer;font-size:14px" title="삭제">🗑</button>
        </td>
      </tr>`;
    });
    tbl += `</tbody></table></div>`;

    list.innerHTML = statsHtml +
      `<div style="display:flex;justify-content:flex-end;margin-bottom:8px"><button class="btn btn-primary" onclick="csShowBackorderForm()" style="font-size:13px">+ 미출고 접수</button></div>` + tbl;
  } catch(e) { list.innerHTML = `<div style="color:red;padding:20px">오류: ${e.message||e}</div>`; }
}

async function csUpdateBackorderStatus(id, status) {
  try {
    await api.put(`/api/cs/backorders/${id}/status?status=${encodeURIComponent(status)}`);
    csLoadBackorder();
    csLoadDashboard();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csDeleteBackorder(id) {
  if (!confirm("삭제하시겠습니까?")) return;
  try { await api.delete(`/api/cs/backorders/${id}`); csLoadBackorder(); csLoadDashboard(); }
  catch(e) { alert("오류: " + (e.message||e)); }
}

function csShowBackorderForm(editData) {
  const d = editData || {};
  const channels = (_csOptions?.sales_channels) || ["스마트스토어","G마켓","옥션","쿠팡","오늘의집","나비엠알오","자사몰","기타"];
  document.getElementById("cs-modal-content").innerHTML = `
    <div class="cs-modal-header"><h3 style="margin:0">${d.id ? '✏️ 미출고 수정' : '📦 미출고 접수'}</h3></div>
    <div class="cs-modal-body">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        <div>
          <div class="cs-field-label">사이트(채널)</div>
          <select id="bo-f-channel" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
            <option value="">선택</option>${channels.map(c=>`<option value="${c}" ${d.sales_channel===c?'selected':''}>${c}</option>`).join("")}
          </select>
        </div>
        <div>
          <div class="cs-field-label">주문확인일</div>
          <input id="bo-f-date" type="date" value="${(d.order_date||'').slice(0,10)}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">주문번호</div>
          <input id="bo-f-orderno" value="${d.order_number||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">수취인명 *</div>
          <input id="bo-f-name" value="${d.recipient_name||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">수취인 연락처</div>
          <input id="bo-f-phone" value="${d.recipient_phone||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">수량</div>
          <input id="bo-f-qty" type="number" min="1" value="${d.quantity||1}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div style="grid-column:1/-1">
          <div class="cs-field-label">상품명 *</div>
          <input id="bo-f-product" value="${d.product_name||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div style="grid-column:1/-1">
          <div class="cs-field-label">옵션정보</div>
          <input id="bo-f-option" value="${d.option_info||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">처리상태</div>
          <select id="bo-f-status" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
            <option value="미출고" ${d.status==='미출고'||!d.status?'selected':''}>미출고</option>
            <option value="출고완료" ${d.status==='출고완료'?'selected':''}>출고완료</option>
            <option value="취소" ${d.status==='취소'?'selected':''}>취소</option>
          </select>
        </div>
        <div>
          <div class="cs-field-label">메모</div>
          <input id="bo-f-memo" value="${d.memo||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
      </div>
      <div style="margin-top:16px;text-align:right;display:flex;gap:8px;justify-content:flex-end">
        <button onclick="csCloseModal()" style="padding:8px 20px;background:#f3f4f6;color:#374151;border:1px solid #d1d5db;border-radius:6px;font-size:13px;cursor:pointer">취소</button>
        <button onclick="csSubmitBackorder(${d.id||0})" style="padding:8px 20px;background:#2563eb;color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600">${d.id?'수정':'등록'}</button>
      </div>
    </div>`;
  document.getElementById("cs-modal-overlay").style.display = "block";
}

async function csSubmitBackorder(editId) {
  const g = id => document.getElementById(id)?.value?.trim() || "";
  const body = {
    sales_channel: g("bo-f-channel"), order_date: g("bo-f-date"), order_number: g("bo-f-orderno"),
    recipient_name: g("bo-f-name"), recipient_phone: g("bo-f-phone"), product_name: g("bo-f-product"),
    option_info: g("bo-f-option"), quantity: parseInt(g("bo-f-qty")) || 1,
    status: g("bo-f-status") || "미출고", memo: g("bo-f-memo"),
  };
  if (!body.recipient_name || !body.product_name) { alert("수취인명과 상품명은 필수입니다."); return; }
  try {
    if (editId) { await api.put(`/api/cs/backorders/${editId}`, body); }
    else { await api.post("/api/cs/backorders", body); }
    csCloseModal(); csLoadBackorder(); csLoadDashboard();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csEditBackorder(id) {
  try {
    const res = await api.get(`/api/cs/backorders?size=500`);
    const item = (res.backorders||[]).find(b => b.id === id);
    if (item) csShowBackorderForm(item);
  } catch(e) { alert("오류: " + (e.message||e)); }
}

// ── 티켓 수정 ──
async function csEditTicket(ticketId) {
  try {
    const res = await api.get(`/api/cs/tickets/${ticketId}`);
    const t = res.ticket || res;
    const opts = _csOptions || {};
    const channels = opts.sales_channels || [];
    const types = opts.cs_types || [];
    const reasons = opts.reason_categories || [];
    const shipCosts = opts.shipping_cost_statuses || [];

    document.getElementById("cs-modal-content").innerHTML = `
      <div class="cs-modal-header"><h3 style="margin:0">✏️ 티켓 수정 — ${ticketId}</h3></div>
      <div class="cs-modal-body">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
          <div>
            <div class="cs-field-label">판매채널</div>
            <select id="cs-e-channel" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
              <option value="">선택</option>${channels.map(c=>`<option value="${c}" ${t.sales_channel===c?'selected':''}>${c}</option>`).join("")}
            </select>
          </div>
          <div>
            <div class="cs-field-label">CS 유형</div>
            <select id="cs-e-type" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
              ${types.map(c=>`<option value="${c}" ${t.cs_type===c?'selected':''}>${c}</option>`).join("")}
            </select>
          </div>
          <div>
            <div class="cs-field-label">고객명</div>
            <input id="cs-e-name" value="${t.customer_name||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
          <div>
            <div class="cs-field-label">연락처</div>
            <input id="cs-e-contact" value="${t.contact_info||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
          <div>
            <div class="cs-field-label">주문번호</div>
            <input id="cs-e-orderno" value="${t.order_number||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
          <div>
            <div class="cs-field-label">사유 분류</div>
            <select id="cs-e-reason" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
              <option value="">선택</option>${reasons.map(c=>`<option value="${c}" ${t.reason_category===c?'selected':''}>${c}</option>`).join("")}
            </select>
          </div>
          <div style="grid-column:1/-1">
            <div class="cs-field-label">상품명</div>
            <input id="cs-e-product" value="${t.product_name||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
          <div>
            <div class="cs-field-label">수량</div>
            <input id="cs-e-qty" type="number" value="${t.quantity||1}" min="1" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
          <div>
            <div class="cs-field-label">배송비 처리</div>
            <select id="cs-e-shipcost" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
              <option value="">선택</option>${shipCosts.map(c=>`<option value="${c}" ${t.shipping_cost_status===c?'selected':''}>${c}</option>`).join("")}
            </select>
          </div>
          <div style="grid-column:1/-1">
            <div class="cs-field-label">증상 / 사유</div>
            <textarea id="cs-e-symptom" rows="3" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;resize:vertical">${t.defect_symptom||''}</textarea>
          </div>
          <div>
            <div class="cs-field-label">반품 택배사</div>
            <input id="cs-e-retcourier" value="${t.return_courier||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
          <div>
            <div class="cs-field-label">반품 송장번호</div>
            <input id="cs-e-rettrack" value="${t.return_tracking_no||''}" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
          </div>
        </div>
        <div style="margin-top:16px;text-align:right;display:flex;gap:8px;justify-content:flex-end">
          <button onclick="csShowDetail('${ticketId}')" style="padding:8px 20px;background:#f3f4f6;color:#374151;border:1px solid #d1d5db;border-radius:6px;font-size:13px;cursor:pointer">취소</button>
          <button onclick="csSubmitEdit('${ticketId}')" style="padding:8px 20px;background:#2563eb;color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600">저장</button>
        </div>
      </div>`;
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csSubmitEdit(ticketId) {
  const g = id => document.getElementById(id)?.value?.trim();
  const body = {
    customer_name: g("cs-e-name"), contact_info: g("cs-e-contact"), product_name: g("cs-e-product"),
    defect_symptom: g("cs-e-symptom"), sales_channel: g("cs-e-channel"), order_number: g("cs-e-orderno"),
    cs_type: g("cs-e-type"), reason_category: g("cs-e-reason"), quantity: parseInt(g("cs-e-qty")) || 1,
    shipping_cost_status: g("cs-e-shipcost"), return_courier: g("cs-e-retcourier"), return_tracking_no: g("cs-e-rettrack"),
  };
  try {
    await api.put(`/api/cs/tickets/${ticketId}/edit`, body);
    alert("수정 완료");
    csShowDetail(ticketId);
    csLoadDashboard();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

// ── 대시보드 (칸반) ──
async function csLoadKanban() {
  const board = document.getElementById("cs-kanban-board");
  if (!board) return;
  const statuses = ["접수완료", "물류수령", "기술인계", "테스트완료", "처리종결"];
  const statusIcons = {"접수완료":"📋","물류수령":"📦","기술인계":"🔬","테스트완료":"✅","처리종결":"🏁"};

  // 단일 API 호출로 전체 로드
  try {
    const params = new URLSearchParams({ size: "500" });
    if (_csChannel) params.set("channel", _csChannel);
    if (_csType) params.set("cs_type", _csType);
    if (_csReason) params.set("reason", _csReason);
    if (_csSearch) params.set("search", _csSearch);
    const res = await api.get(`/api/cs/tickets?${params}`);
    const allTickets = res.tickets || [];

    // JS에서 상태별 그룹핑
    const columns = {};
    statuses.forEach(s => columns[s] = []);
    allTickets.forEach(t => {
      if (columns[t.current_status]) columns[t.current_status].push(t);
    });

  board.innerHTML = statuses.map(st => {
    const tickets = columns[st] || [];
    const cnt = tickets.length;
    const statusColors = {"접수완료":"#fef3c7","물류수령":"#dbeafe","기술인계":"#e0e7ff","테스트완료":"#d1fae5","처리종결":"#f3f4f6"};
    return `<div class="cs-kanban-col">
      <div class="cs-kanban-head">
        <span>${statusIcons[st]||''} ${st}</span>
        <span class="cs-kanban-cnt" style="background:${statusColors[st]||'#e5e7eb'}">${cnt}</span>
      </div>
      ${cnt === 0 ? '<div style="font-size:12px;color:#9ca3af;text-align:center;padding:20px">없음</div>' :
        tickets.map(t => {
          const isOverdue = _isOverdue(t);
          const chColor = CS_CHANNEL_COLORS[t.sales_channel] || "#9ca3af";
          return `<div class="cs-kanban-item ${isOverdue?'overdue':''}" onclick="csShowDetail('${t.ticket_id}')">
            <div class="cust">${t.customer_name}${t.cs_type && t.cs_type !== '반품' ? ` <span style="font-size:10px;background:#e0e7ff;color:#3730a3;padding:0 4px;border-radius:4px;font-weight:500">${t.cs_type}</span>` : ''}</div>
            <div class="prod">${t.product_name}${t.quantity>1?` x${t.quantity}`:''}</div>
            <div class="meta">
              ${t.sales_channel ? `<span class="cs-ch-badge" style="background:${chColor}15;color:${chColor}">${t.sales_channel}</span>` : '<span></span>'}
              <span>${_csDateShort(t.created_at)}${isOverdue ? ' ⚠️' : ''}</span>
            </div>
          </div>`;
        }).join("")
      }
    </div>`;
  }).join("");
  } catch(e) { board.innerHTML = `<div style="color:red;padding:20px">로딩 오류: ${e?.message||e?.detail||String(e)}</div>`; }
}

function _isOverdue(t) {
  if (t.current_status === "처리종결") return false;
  if (!t.created_at) return false;
  const created = new Date(t.created_at);
  const now = new Date();
  return (now - created) > 7 * 24 * 60 * 60 * 1000;
}
function _csDateShort(dt) {
  if (!dt) return "";
  try { const d = new Date(dt); return `${d.getMonth()+1}/${d.getDate()}`; } catch { return dt.slice(5,10); }
}

// ── 리스트 뷰 (기존 기능 유지) ──
async function csLoadStats() {
  try {
    const data = await api.get("/api/cs/stats");
    csRenderStatsBar(data);
  } catch(e) {}
}

async function csLoadTickets() {
  const list = document.getElementById("cs-ticket-list");
  if (!list) return;
  const params = new URLSearchParams({ page: _csPage, size: 50 });
  if (_csStatus) params.set("status", _csStatus);
  if (_csChannel) params.set("channel", _csChannel);
  if (_csType) params.set("cs_type", _csType);
  if (_csReason) params.set("reason", _csReason);
  if (_csSearch) params.set("search", _csSearch);
  try {
    const res = await api.get(`/api/cs/tickets?${params}`);
    const tickets = res.tickets || [];
    if (!tickets.length) {
      list.innerHTML = '<div style="text-align:center;padding:40px;color:#9ca3af">' +
        (_csSearch || _csChannel || _csType || _csReason ? '필터 조건에 맞는 결과가 없습니다.' : '등록된 CS 티켓이 없습니다.<br><br><button class="btn btn-primary" onclick="csShowCreateForm()">+ 새 접수</button>') +
        '</div>';
      document.getElementById("cs-pagination").innerHTML = "";
      return;
    }
    list.innerHTML = tickets.map(t => {
      const isOverdue = _isOverdue(t);
      const chColor = CS_CHANNEL_COLORS[t.sales_channel] || "#9ca3af";
      return `<div class="cs-ticket-card ${isOverdue?'overdue':''}" onclick="csShowDetail('${t.ticket_id}')">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;flex-wrap:wrap">
          <div>
            <span style="font-weight:600;color:#111827">${t.customer_name}</span>
            <span style="font-size:12px;color:#9ca3af;margin-left:8px">${t.ticket_id}</span>
            ${t.sales_channel ? `<span class="cs-ch-badge" style="background:${chColor}15;color:${chColor};margin-left:6px">${t.sales_channel}</span>` : ''}
            ${t.cs_type && t.cs_type !== '반품' ? `<span style="font-size:11px;background:#e0e7ff;color:#3730a3;padding:1px 6px;border-radius:6px;margin-left:4px">${t.cs_type}</span>` : ''}
          </div>
          <span class="cs-status-badge cs-status-${t.current_status}">${t.current_status}</span>
        </div>
        <div style="margin-top:6px;font-size:13px;color:#4b5563">${t.product_name}${t.quantity > 1 ? ` x${t.quantity}` : ''}</div>
        <div style="margin-top:4px;font-size:12px;color:#9ca3af;display:flex;gap:12px;flex-wrap:wrap">
          <span>${t.created_at?.slice(0,10) || ''}</span>
          ${t.reason_category ? `<span>${t.reason_category}</span>` : ''}
          ${t.order_number ? `<span>주문: ${t.order_number.slice(-8)}</span>` : ''}
          ${isOverdue ? '<span style="color:#ef4444;font-weight:600">⚠ 7일+ 지연</span>' : ''}
        </div>
      </div>`;
    }).join("");

    // 페이지네이션
    const total = res.total || 0;
    const pages = Math.ceil(total / 50);
    const pgDiv = document.getElementById("cs-pagination");
    if (pages <= 1) { pgDiv.innerHTML = ""; return; }
    let pgHtml = "";
    for (let i = 1; i <= pages; i++) {
      pgHtml += `<button onclick="_csPage=${i};csLoadTickets()" style="padding:4px 12px;border:1px solid ${i===_csPage?'#2563eb':'#d1d5db'};border-radius:6px;background:${i===_csPage?'#2563eb':'#fff'};color:${i===_csPage?'#fff':'#374151'};cursor:pointer;font-size:13px">${i}</button>`;
    }
    pgDiv.innerHTML = pgHtml;
  } catch(e) { list.innerHTML = `<div style="color:red;padding:20px">오류: ${e.message||e}</div>`; }
}

function csSwitchTab(el, status) {
  _csStatus = status;
  _csPage = 1;
  document.querySelectorAll("#cs-list-view .cs-pipe-tab").forEach(t => t.classList.remove("active"));
  el.classList.add("active");
  csLoadTickets();
}

function csStatCardClick(filter) {
  if (filter === "처리종결") {
    _csStatus = "처리종결";
    _csView = "list";
    const tabs = document.querySelectorAll("#page-cs_rma .cs-pipe-tab");
    tabs.forEach(t => t.classList.remove("active"));
    tabs.forEach(t => { if(t.textContent.includes('리스트')) t.classList.add("active"); });
    document.getElementById("cs-kanban-board").style.display = "none";
    document.getElementById("cs-list-view").style.display = "block";
    document.getElementById("cs-backorder-view").style.display = "none";
    csLoadTickets();
  } else if (filter === "backorder") {
    _csView = "backorder";
    const tabs = document.querySelectorAll("#page-cs_rma .cs-pipe-tab");
    tabs.forEach(t => t.classList.remove("active"));
    tabs.forEach(t => { if(t.textContent.includes('미출고')) t.classList.add("active"); });
    document.getElementById("cs-kanban-board").style.display = "none";
    document.getElementById("cs-list-view").style.display = "none";
    document.getElementById("cs-backorder-view").style.display = "block";
    csLoadBackorder();
  }
}

function csCloseModal() {
  document.getElementById("cs-modal-overlay").style.display = "none";
}

// ── 새 접수 폼 (확장) ──
function csShowCreateForm() {
  const opts = _csOptions || {};
  const channels = opts.sales_channels || ["스마트스토어","G마켓","옥션","쿠팡","오늘의집","나비엠알오","자사몰","기타"];
  const types = opts.cs_types || ["반품","교환","A/S수리","미출고"];
  const reasons = opts.reason_categories || ["파손 및 불량","단순 변심","주문 실수","오배송 및 지연","재고 부족","기타"];
  const shipCosts = opts.shipping_cost_statuses || ["환불금에서 차감","판매자에게 직접 송금","추가결제","무료반품","해당없음"];

  document.getElementById("cs-modal-content").innerHTML = `
    <div class="cs-modal-header"><h3 style="margin:0">📝 새 CS/RMA 접수</h3></div>
    <div class="cs-modal-body">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        <div>
          <div class="cs-field-label">판매채널 *</div>
          <select id="cs-f-channel" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
            <option value="">선택</option>
            ${channels.map(c=>`<option value="${c}">${c}</option>`).join("")}
          </select>
        </div>
        <div>
          <div class="cs-field-label">CS 유형</div>
          <select id="cs-f-type" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
            ${types.map(c=>`<option value="${c}">${c}</option>`).join("")}
          </select>
        </div>
        <div>
          <div class="cs-field-label">고객명 *</div>
          <input id="cs-f-name" placeholder="고객명" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">연락처 *</div>
          <input id="cs-f-contact" placeholder="전화번호" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">주문번호</div>
          <input id="cs-f-orderno" placeholder="채널 주문번호" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">사유 분류</div>
          <select id="cs-f-reason" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
            <option value="">선택</option>
            ${reasons.map(c=>`<option value="${c}">${c}</option>`).join("")}
          </select>
        </div>
        <div style="grid-column:1/-1">
          <div class="cs-field-label">상품명 *</div>
          <input id="cs-f-product" placeholder="상품명" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">수량</div>
          <input id="cs-f-qty" type="number" value="1" min="1" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">시리얼번호</div>
          <input id="cs-f-serial" placeholder="선택사항" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div style="grid-column:1/-1">
          <div class="cs-field-label">불량 증상 / 사유 상세 *</div>
          <textarea id="cs-f-symptom" rows="3" placeholder="증상 또는 사유를 상세히 기록" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;resize:vertical"></textarea>
        </div>
        <div>
          <div class="cs-field-label">배송비 처리</div>
          <select id="cs-f-shipcost" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
            <option value="">선택</option>
            ${shipCosts.map(c=>`<option value="${c}">${c}</option>`).join("")}
          </select>
        </div>
        <div>
          <div class="cs-field-label">반품 택배사</div>
          <input id="cs-f-retcourier" placeholder="로젠, CJ 등" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">반품 송장번호</div>
          <input id="cs-f-rettrack" placeholder="반품 송장" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
        <div>
          <div class="cs-field-label">메모</div>
          <input id="cs-f-memo" placeholder="추가 메모" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        </div>
      </div>
      <div style="margin-top:16px;text-align:right;display:flex;gap:8px;justify-content:flex-end">
        <button onclick="csCloseModal()" style="padding:8px 20px;background:#f3f4f6;color:#374151;border:1px solid #d1d5db;border-radius:6px;font-size:13px;cursor:pointer">취소</button>
        <button onclick="csSubmitCreate()" style="padding:8px 20px;background:#2563eb;color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600">접수 등록</button>
      </div>
    </div>`;
  document.getElementById("cs-modal-overlay").style.display = "block";
}

async function csSubmitCreate() {
  const g = id => document.getElementById(id)?.value?.trim() || "";
  const body = {
    customer_name: g("cs-f-name"),
    contact_info: g("cs-f-contact"),
    product_name: g("cs-f-product"),
    serial_number: g("cs-f-serial"),
    defect_symptom: g("cs-f-symptom"),
    memo: g("cs-f-memo"),
    sales_channel: g("cs-f-channel"),
    order_number: g("cs-f-orderno"),
    cs_type: g("cs-f-type") || "반품",
    reason_category: g("cs-f-reason"),
    quantity: parseInt(g("cs-f-qty")) || 1,
    shipping_cost_status: g("cs-f-shipcost"),
    return_courier: g("cs-f-retcourier"),
    return_tracking_no: g("cs-f-rettrack"),
  };
  if (!body.customer_name || !body.contact_info || !body.product_name || !body.defect_symptom) {
    alert("필수 항목(고객명, 연락처, 상품명, 증상)을 입력해주세요."); return;
  }
  try {
    const res = await api.post("/api/cs/tickets", body);
    alert(res.message || "접수 완료");
    csCloseModal();
    csLoadDashboard();
    csRenderView();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

// ── 상세 보기 (기존 기능 유지 + 신규 필드 표시) ──
async function csShowDetail(ticketId) {
  // ── Phase 1: 즉시 모달 표시 (로딩 스켈레톤) ──
  document.getElementById("cs-modal-content").innerHTML = `
    <div class="cs-modal-header" style="display:flex;justify-content:space-between;align-items:center">
      <div>
        <div style="width:180px;height:16px;background:#e5e7eb;border-radius:4px;margin-bottom:8px" class="cs-skel"></div>
        <div style="width:120px;height:14px;background:#e5e7eb;border-radius:4px" class="cs-skel"></div>
      </div>
      <button onclick="csCloseModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:#6b7280">&times;</button>
    </div>
    <div class="cs-modal-body">
      <div style="display:flex;gap:12px;margin-bottom:20px">${[1,2,3,4,5].map(()=>'<div style="flex:1;text-align:center"><div style="width:24px;height:24px;border-radius:50%;background:#e5e7eb;margin:0 auto 4px" class="cs-skel"></div><div style="width:40px;height:10px;background:#e5e7eb;border-radius:3px;margin:0 auto" class="cs-skel"></div></div>').join("")}</div>
      <div style="width:100%;height:60px;background:#fef2f2;border-radius:8px;margin-bottom:16px" class="cs-skel"></div>
      <div style="width:70%;height:14px;background:#e5e7eb;border-radius:4px;margin-bottom:10px" class="cs-skel"></div>
      <div style="width:50%;height:14px;background:#e5e7eb;border-radius:4px;margin-bottom:10px" class="cs-skel"></div>
      <div style="width:60%;height:14px;background:#e5e7eb;border-radius:4px" class="cs-skel"></div>
    </div>
    <style>.cs-skel{animation:csPulse 1.2s ease-in-out infinite}@keyframes csPulse{0%,100%{opacity:1}50%{opacity:.4}}</style>`;
  document.getElementById("cs-modal-overlay").style.display = "block";

  // ── Phase 2: API 데이터 로드 후 실제 내용 교체 ──
  try {
    const res = await api.get(`/api/cs/tickets/${ticketId}`);
    const t = res.ticket || res;
    const testResult = res.test_result;
    const files = res.files || [];
    const logs = res.logs || [];
    const _e = s => _escHtml(s || '');
    const chColor = CS_CHANNEL_COLORS[t.sales_channel] || "#9ca3af";

    // 상태 진행 바
    const steps = ["접수완료","물류수령","기술인계","테스트완료","처리종결"];
    const curIdx = steps.indexOf(t.current_status);
    const stepsHtml = steps.map((s,i) => {
      const done = i <= curIdx;
      const color = done ? "#2563eb" : "#d1d5db";
      return `<div style="flex:1;text-align:center">
        <div style="width:24px;height:24px;border-radius:50%;background:${done?'#2563eb':'#fff'};border:2px solid ${color};margin:0 auto 4px;display:flex;align-items:center;justify-content:center">
          ${done?'<span style="color:#fff;font-size:11px">✓</span>':''}
        </div>
        <div style="font-size:10px;color:${done?'#2563eb':'#9ca3af'};font-weight:${i===curIdx?'600':'400'}">${s}</div>
        ${i<4?`<div style="position:relative;top:-20px;left:50%;width:100%;height:2px;background:${i<curIdx?'#2563eb':'#e5e7eb'}"></div>`:''}
      </div>`;
    }).join("");

    // 첨부파일 (미리보기)
    let filesHtml = "";
    if (files.length > 0) {
      filesHtml = `<div style="margin-top:16px">
        <div style="font-weight:600;font-size:13px;margin-bottom:8px">📎 첨부파일 (${files.length})</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${files.map(f => {
            const imgSrc = f.file_url && f.file_url.startsWith("/api/cs/files/db/") ? f.file_url : (f.file_url || '/api/cs/files/db/'+f.id);
            const delBtn = t.current_status !== "처리종결"
              ? '<button onclick="event.stopPropagation();csDeleteFile('+f.id+',\''+t.ticket_id+'\')" title="삭제" style="position:absolute;top:-6px;right:-6px;width:20px;height:20px;border-radius:50%;background:#ef4444;color:#fff;border:none;font-size:11px;cursor:pointer;line-height:20px;text-align:center">✕</button>'
              : '';
            const dlBtn = '<button onclick="event.stopPropagation();csDownloadFile('+f.id+',\''+_e(f.file_name).replace(/'/g,"\\'")+'\')" title="다운로드" style="position:absolute;top:-6px;right:'+( t.current_status!=="처리종결"?'18':'- 6')+'px;width:20px;height:20px;border-radius:50%;background:#2563eb;color:#fff;border:none;font-size:10px;cursor:pointer;line-height:20px;text-align:center">↓</button>';
            if (f.file_type === "image") {
              return '<div style="position:relative;display:inline-block">'+delBtn+dlBtn+'<a href="'+imgSrc+'" target="_blank"><img src="'+imgSrc+'" style="width:100px;height:100px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb" onerror="this.style.display=\'none\'"></a></div>';
            } else if (f.file_type === "video") {
              return '<div style="position:relative;display:inline-block">'+delBtn+dlBtn+'<video src="'+imgSrc+'" style="width:120px;height:80px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb" controls preload="metadata"></video></div>';
            }
            return '<div style="position:relative;display:inline-block">'+delBtn+dlBtn+'<a href="'+imgSrc+'" target="_blank" style="display:inline-flex;align-items:center;justify-content:center;width:80px;height:80px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;font-size:11px;color:#6b7280;text-align:center;text-decoration:none;padding:4px;word-break:break-all">'+_e(f.file_name)+'</a></div>';
          }).join("")}
        </div>
      </div>`;
    }

    // 타임라인
    let timelineHtml = logs.length > 0 ? `<div style="margin-top:16px">
      <div class="cs-field-label">처리 이력</div><div class="cs-timeline">
      ${logs.map(l => `<div class="cs-timeline-item">
        <div class="cs-timeline-dot done"></div>
        <div><strong style="font-size:13px">${_e(l.action_type)}</strong>
          <span style="font-size:12px;color:#6b7280;margin-left:8px">${_e(l.actor_name)}</span></div>
        ${l.detail ? `<div style="font-size:12px;color:#4b5563;margin-top:2px">${_e(l.detail)}</div>` : ''}
        <div class="cs-timeline-time">${l.created_at||''}</div>
      </div>`).join("")}
      </div></div>` : "";

    // 테스트 결과
    let testHtml = "";
    if (testResult) {
      testHtml = `<div style="margin-top:12px"><div class="cs-field-label">테스트 결과</div>
        <span class="cs-test-badge cs-test-${testResult.test_status}">${testResult.test_status}</span>
        ${testResult.test_comment ? `<div style="margin-top:6px;font-size:13px;color:#374151;white-space:pre-wrap">${_e(testResult.test_comment)}</div>` : ''}
      </div>`;
    }

    let html = `<div class="cs-modal-header" style="display:flex;justify-content:space-between;align-items:flex-start">
      <div>
        <div style="font-size:14px;font-weight:500;color:#111827;margin-bottom:4px">${_e(t.customer_name)} · ${_e(t.contact_info)}</div>
        <div style="font-size:13px;color:#374151">${_e(t.product_name)}${t.quantity>1?' x'+t.quantity:''}${t.serial_number ? ' (S/N: '+_e(t.serial_number)+')' : ''}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:4px;display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          <span>${t.ticket_id}</span>
          ${t.sales_channel ? `<span class="cs-ch-badge" style="background:${chColor}15;color:${chColor}">${t.sales_channel}</span>` : ''}
          ${t.cs_type && t.cs_type !== '반품' ? `<span style="background:#e0e7ff;color:#3730a3;padding:1px 6px;border-radius:6px;font-size:11px">${t.cs_type}</span>` : ''}
          ${t.reason_category ? `<span>${t.reason_category}</span>` : ''}
        </div>
      </div>
      <div style="text-align:right">
        <span class="cs-status-badge cs-status-${t.current_status}" style="font-size:13px">${t.current_status}</span>
        ${t.final_action ? `<div style="font-size:12px;color:#6b7280;margin-top:4px">${t.final_action}</div>` : ''}
        <button onclick="csCloseModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:#6b7280;margin-top:4px">&times;</button>
      </div>
    </div>
    <div class="cs-modal-body">
      <div style="display:flex;align-items:flex-start;margin-bottom:20px;gap:0">${stepsHtml}</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0 20px">
        ${t.order_number ? `<div><div class="cs-field-label">주문번호</div><div class="cs-field-value">${_e(t.order_number)}</div></div>` : ''}
        ${t.shipping_cost_status ? `<div><div class="cs-field-label">배송비 처리</div><div class="cs-field-value">${_e(t.shipping_cost_status)}</div></div>` : ''}
        ${t.courier ? `<div><div class="cs-field-label">택배사</div><div class="cs-field-value">${_e(t.courier)}</div></div>` : ''}
        ${t.tracking_no ? `<div><div class="cs-field-label">송장번호</div><div class="cs-field-value">${_e(t.tracking_no)}</div></div>` : ''}
        ${t.return_courier||t.return_tracking_no ? `<div><div class="cs-field-label">반품 배송</div><div class="cs-field-value">${_e(t.return_courier)} ${_e(t.return_tracking_no)}</div></div>` : ''}
      </div>
      <div style="margin-top:8px"><div class="cs-field-label">불량 증상 / 사유</div><div class="cs-symptom-box">${_e(t.defect_symptom)}</div></div>
      ${testHtml}
      ${filesHtml}
      ${t.current_status !== "처리종결" ? `<div style="margin-top:16px">
        <label style="display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border:1px dashed #d1d5db;border-radius:6px;cursor:pointer;font-size:13px;color:#6b7280;transition:all .15s" onmouseover="this.style.borderColor='#93c5fd'" onmouseout="this.style.borderColor='#d1d5db'">
          📎 파일 첨부
          <input type="file" multiple style="display:none" onchange="csUploadFile('${t.ticket_id}',this)">
        </label>
      </div>` : ''}
      ${timelineHtml}
      ${t.current_status !== "처리종결" ? `<div style="margin-top:12px;display:flex;gap:6px">
        <input type="text" id="cs-memo-input" placeholder="메모 추가..." style="flex:1;padding:6px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:13px">
        <button onclick="csAddMemo('${t.ticket_id}')" style="padding:6px 14px;background:#f3f4f6;border:1px solid #d1d5db;border-radius:6px;font-size:13px;cursor:pointer">추가</button>
      </div>` : ''}
      <div style="margin-top:16px;padding-top:16px;border-top:1px solid #e5e7eb;display:flex;gap:8px;flex-wrap:wrap">
        <button onclick="csEditTicket('${t.ticket_id}')" class="btn" style="font-size:13px">✏️ 수정</button>`;

    const st = t.current_status;
    if (st === "접수완료") html += `<button onclick="csAction('${t.ticket_id}','receive')" class="btn" style="font-size:13px">📦 물류 수령</button>`;
    if (st === "물류수령") html += `<button onclick="csAction('${t.ticket_id}','handover')" class="btn" style="font-size:13px">🔬 기술 인계</button>`;
    if (st === "기술인계") html += `<button onclick="csShowTestForm('${t.ticket_id}')" class="btn" style="font-size:13px">📝 테스트 결과</button>`;
    if (st === "테스트완료") html += `<button onclick="csResolveWithPrompt('${t.ticket_id}')" class="btn" style="font-size:13px;background:#2563eb;color:#fff;border-color:#2563eb">🏁 처리종결</button>`;
    if (st === "물류수령") html += `<button onclick="csQuickResolve('${t.ticket_id}')" class="btn" style="font-size:13px;background:#059669;color:#fff;border-color:#059669">⚡ 빠른 종결</button>`;
    html += `<button onclick="csDeleteTicket('${t.ticket_id}')" class="btn" style="font-size:13px;color:#ef4444;margin-left:auto">🗑 삭제</button>`;
    html += `</div></div>`;

    document.getElementById("cs-modal-content").innerHTML = html;
  } catch(e) {
    document.getElementById("cs-modal-content").innerHTML = `
      <div class="cs-modal-header" style="display:flex;justify-content:space-between;align-items:center">
        <span style="color:#ef4444;font-weight:600">⚠️ 로딩 실패</span>
        <button onclick="csCloseModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:#6b7280">&times;</button>
      </div>
      <div class="cs-modal-body" style="text-align:center;padding:40px;color:#6b7280">
        <p>${e.message||e}</p>
        <button onclick="csShowDetail('${ticketId}')" class="btn btn-primary" style="margin-top:12px;font-size:13px">다시 시도</button>
      </div>`;
  }
}

async function csDeleteTicket(ticketId) {
  if (!confirm(`정말 ${ticketId}을(를) 삭제하시겠습니까?`)) return;
  try {
    await api.delete(`/api/cs/tickets/${ticketId}`);
    csCloseModal();
    csLoadDashboard();
    csRenderView();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csAction(ticketId, action) {
  const memo = document.getElementById("cs-memo-input")?.value?.trim() || "";
  try {
    await api.put(`/api/cs/tickets/${ticketId}/${action}`, { memo });
    csShowDetail(ticketId);
    csLoadDashboard();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

function csShowTestForm(ticketId) {
  const results = (_csOptions?.test_results) || ["정상","의심","불량"];
  const div = document.createElement("div");
  div.id = "cs-test-form";
  div.style.cssText = "margin-top:12px;padding:12px;background:#f9fafb;border-radius:8px;border:1px solid #e5e7eb";
  div.innerHTML = `
    <div class="cs-field-label">테스트 결과</div>
    <div style="display:flex;gap:8px;margin:8px 0">
      ${results.map(r => `<label style="cursor:pointer"><input type="radio" name="cs-test-r" value="${r}"> ${r}</label>`).join("")}
    </div>
    <input id="cs-test-comment" placeholder="테스트 코멘트" style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;margin-bottom:8px">
    <button onclick="csSubmitTest('${ticketId}')" style="padding:6px 16px;background:#2563eb;color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer">등록</button>`;
  const existing = document.getElementById("cs-test-form");
  if (existing) existing.remove();
  document.querySelector(".cs-modal-body")?.appendChild(div);
}

async function csSubmitTest(ticketId) {
  const status = document.querySelector('input[name="cs-test-r"]:checked')?.value;
  if (!status) { alert("테스트 결과를 선택해주세요."); return; }
  const comment = document.getElementById("cs-test-comment")?.value?.trim() || "";
  try {
    await api.post(`/api/cs/tickets/${ticketId}/test-result`, { test_status: status, test_comment: comment });
    csShowDetail(ticketId);
    csLoadDashboard();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csResolve(ticketId, action) {
  try {
    await api.put(`/api/cs/tickets/${ticketId}/resolve`, { action, memo: "" });
    csShowDetail(ticketId);
    csLoadDashboard();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csResolveWithPrompt(ticketId) {
  const actions = (_csOptions?.final_actions) || ["교환발송","환불처리","정상반송","단순변심 반송"];
  const action = prompt(`최종 처리 선택:\n${actions.map((a,i)=>`${i+1}. ${a}`).join("\n")}\n\n번호 입력:`);
  if (!action) return;
  const idx = parseInt(action) - 1;
  if (idx < 0 || idx >= actions.length) { alert("올바른 번호를 입력해주세요."); return; }
  try {
    await api.put(`/api/cs/tickets/${ticketId}/resolve`, { action: actions[idx], memo: "" });
    csCloseModal();
    csLoadDashboard();
    csRenderView();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csQuickResolve(ticketId) {
  const actions = (_csOptions?.final_actions) || ["교환발송","환불처리","정상반송","단순변심 반송"];
  const action = prompt(`최종 처리 선택:\n${actions.map((a,i)=>`${i+1}. ${a}`).join("\n")}\n\n번호 입력:`);
  if (!action) return;
  const idx = parseInt(action) - 1;
  if (idx < 0 || idx >= actions.length) { alert("올바른 번호를 입력해주세요."); return; }
  try {
    await api.put(`/api/cs/tickets/${ticketId}/quick-resolve`, { action: actions[idx], memo: "" });
    csCloseModal();
    csLoadDashboard();
    csRenderView();
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csUploadFile(ticketId, input) {
  const files = input.files;
  if (!files.length) return;

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    if (file.size > 100 * 1024 * 1024) { alert(`파일 크기 초과 (100MB): ${file.name}`); continue; }

    // 진행률 오버레이
    const overlay = document.createElement("div");
    overlay.id = "cs-upload-overlay";
    overlay.innerHTML = `<div style="position:fixed;inset:0;background:rgba(0,0,0,0.4);display:flex;align-items:center;justify-content:center;z-index:10000">
      <div style="background:#fff;border-radius:12px;padding:24px 32px;min-width:300px;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.2)">
        <div id="cs-upload-title" style="font-size:14px;font-weight:600;margin-bottom:4px">📎 파일 업로드 중...</div>
        <div style="font-size:12px;color:#9ca3af;margin-bottom:12px">${file.name}${files.length>1?` (${i+1}/${files.length})`:''}</div>
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
            if (txt) txt.textContent = pct + "% (" + (e.loaded/1024/1024).toFixed(1) + "MB / " + (e.total/1024/1024).toFixed(1) + "MB)";
            if (pct >= 100) {
              const title = document.getElementById("cs-upload-title");
              if (title) title.textContent = "⏳ 서버 처리 중...";
            }
          }
        };
        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            try { resolve(JSON.parse(xhr.responseText)); } catch(_) { resolve({}); }
          } else { reject(new Error(xhr.responseText || `업로드 실패 (${xhr.status})`)); }
        };
        xhr.onerror = () => reject(new Error("네트워크 오류"));
        xhr.timeout = 300000;
        xhr.ontimeout = () => reject(new Error("업로드 시간 초과 (5분)"));
        xhr.send(formData);
      });
    } catch(e) { alert("업로드 오류: " + (e.message||e)); }
    finally { overlay.remove(); }
  }
  await new Promise(r => setTimeout(r, 500));
  csShowDetail(ticketId);
}

async function csDeleteFile(fileId, ticketId) {
  if (!confirm("파일을 삭제하시겠습니까?")) return;
  try {
    await api.delete(`/api/cs/files/${fileId}`);
    csShowDetail(ticketId);
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csDriveCheck() {
  try {
    const res = await api.get("/api/cs/drive-check");
    alert(JSON.stringify(res, null, 2));
  } catch(e) { alert("오류: " + (e.message||e)); }
}

async function csDownloadFile(fileId, fileName) {
  try {
    const res = await fetch(`/api/cs/download/${fileId}`, { headers: { "Authorization": `Bearer ${api.getToken()}` } });
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = fileName; a.click();
    URL.revokeObjectURL(url);
  } catch(e) { alert("다운로드 오류: " + (e.message||e)); }
}

async function csAddMemo(ticketId) {
  const memo = document.getElementById("cs-memo-input")?.value.trim();
  if (!memo) { alert("메모 내용을 입력해주세요."); return; }
  try {
    await api.post(`/api/cs/tickets/${ticketId}/memo`, { memo });
    csShowDetail(ticketId);
  } catch(e) { alert("오류: " + (e.message || e)); }
}

// ── What's New 팝업 ──
function csCloseWhatsNew() {
  document.getElementById("cs-whatsnew-overlay").style.display = "none";
  if (document.getElementById("cs-wn-dontshow")?.checked) {
    localStorage.setItem("cs_whatsnew_v22_dismissed", "1");
  }
}

// ── 하위 호환: 기존 csSearch 호출 리다이렉트 ──
function csSearch() { csApplyFilters(); }


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
  // 온라인관리품목 탭이 보이면 분석도 로딩
  const planTab = document.getElementById('inv-tab-planning');
  if (planTab && planTab.style.display !== 'none') ipRefreshAnalysis();
}

function switchInvTab(tab) {
  document.querySelectorAll('.inv-tab-btn').forEach(b => {
    b.style.borderBottomColor = 'transparent';
    b.style.color = '#64748b';
    b.style.fontWeight = '400';
  });
  const btn = document.querySelector(`.inv-tab-btn[data-inv-tab="${tab}"]`);
  if (btn) { btn.style.borderBottomColor = '#2563eb'; btn.style.color = '#2563eb'; btn.style.fontWeight = '600'; }
  document.querySelectorAll('.inv-tab-content').forEach(c => c.style.display = 'none');
  const content = document.getElementById(`inv-tab-${tab}`);
  if (content) content.style.display = 'block';
  if (tab === 'planning' && !_ipData) ipRefreshAnalysis();
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
//  적정재고 — 온라인관리품목 (Inventory Planning)
// ═══════════════════════════════════════════════════════════

let _ipData = null;
let _ipSelected = null; // 등록 모달에서 선택된 품목
let _ipSortKey = '';
let _ipSortAsc = true;

async function ipRefreshAnalysis() {
  const tbody = document.getElementById('ip-table-body');
  if (tbody) tbody.innerHTML = '<tr><td colspan="11" style="padding:30px;text-align:center;color:#94a3b8">⏳ 분석 중...</td></tr>';
  try {
    _ipData = await api.get('/api/inventory-planning/analysis');
    ipRenderSummary(_ipData.summary);
    ipRenderTable(_ipData.items);
    ipRenderScanInfo(_ipData.last_scan);
  } catch (err) {
    if (tbody) tbody.innerHTML = `<tr><td colspan="11" style="padding:20px;text-align:center;color:#dc2626">분석 실패: ${err.message||err}</td></tr>`;
  }
}

function ipRenderScanInfo(scanInfo) {
  const el = document.getElementById('ip-scan-info');
  if (!el || !scanInfo) return;
  const parts = [];
  if (scanInfo.shipping_scan) {
    parts.push(`📧 <b>선적스캔</b>: ${scanInfo.shipping_scan.executed_at || '-'} KST`);
    if (scanInfo.shipping_scan.email_dates) parts.push(`(메일: ${scanInfo.shipping_scan.email_dates})`);
  }
  if (scanInfo.orderlist_sync) {
    parts.push(`📋 <b>오더리스트</b>: ${scanInfo.orderlist_sync.executed_at || '-'} KST`);
    if (scanInfo.orderlist_sync.email_dates) parts.push(`(메일: ${scanInfo.orderlist_sync.email_dates})`);
  }
  if (parts.length > 0) {
    el.innerHTML = parts.join(' &nbsp;│&nbsp; ');
    el.style.display = 'block';
  } else {
    el.innerHTML = '📧 선적스캔: 아직 실행 안 됨 &nbsp;│&nbsp; 📋 오더리스트: 아직 실행 안 됨';
    el.style.display = 'block';
  }
}

function ipRenderSummary(s) {
  const el = id => document.getElementById(id);
  if (el('ip-cnt-urgent')) el('ip-cnt-urgent').textContent = s.urgent || 0;
  if (el('ip-cnt-warning')) el('ip-cnt-warning').textContent = s.warning || 0;
  if (el('ip-cnt-ordered')) el('ip-cnt-ordered').textContent = s.ordered || 0;
  if (el('ip-cnt-safe')) el('ip-cnt-safe').textContent = (s.safe || 0) + (s.no_sales || 0);
}

function ipCardFilter(status) {
  const sel = document.getElementById('ip-status-filter');
  if (sel) { sel.value = status; }
  ipFilterTable();
}

function ipSort(key) {
  if (_ipSortKey === key) { _ipSortAsc = !_ipSortAsc; } else { _ipSortKey = key; _ipSortAsc = true; }
  if (_ipData) ipRenderTable(_ipData.items);
}

function ipRenderTable(items) {
  const tbody = document.getElementById('ip-table-body');
  if (!tbody) return;
  if (!items || items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" style="padding:30px;text-align:center;color:#94a3b8">등록된 관리품목이 없습니다. [+ 품목 등록] 버튼으로 추가하세요.</td></tr>';
    return;
  }

  const statusBadge = (s, label) => {
    const colors = {
      urgent: 'background:#FEF2F2;color:#DC2626;border:1px solid #FECACA',
      warning: 'background:#FFFBEB;color:#D97706;border:1px solid #FDE68A',
      ordered: 'background:#ECFDF5;color:#059669;border:1px solid #A7F3D0',
      safe: 'background:#F8FAFC;color:#64748B;border:1px solid #E2E8F0',
      no_sales: 'background:#F8FAFC;color:#94A3B8;border:1px solid #E2E8F0',
    };
    return `<span style="padding:3px 8px;border-radius:12px;font-size:11px;font-weight:600;white-space:nowrap;${colors[s]||colors.safe}">${label}</span>`;
  };

  const filter = (document.getElementById('ip-status-filter')||{}).value || '';
  const search = ((document.getElementById('ip-search')||{}).value || '').toUpperCase();

  let filtered = items.slice();
  if (filter) filtered = filtered.filter(i => i.status === filter);
  if (search) filtered = filtered.filter(i =>
    (i.model_name||'').toUpperCase().includes(search) ||
    (i.prod_name||'').toUpperCase().includes(search) ||
    (i.prod_cd||'').toUpperCase().includes(search)
  );

  // 정렬
  if (_ipSortKey) {
    const statusOrder = {urgent:0, warning:1, ordered:2, no_sales:3, safe:4};
    filtered.sort((a, b) => {
      let va = a[_ipSortKey], vb = b[_ipSortKey];
      if (_ipSortKey === 'status') { va = statusOrder[va]||9; vb = statusOrder[vb]||9; }
      if (_ipSortKey === 'model_name' || _ipSortKey === 'order_deadline') {
        va = (va||'').toString(); vb = (vb||'').toString();
        return _ipSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      }
      va = parseFloat(va) || 0; vb = parseFloat(vb) || 0;
      return _ipSortAsc ? va - vb : vb - va;
    });
  }

  tbody.innerHTML = filtered.map(i => {
    const stockout = i.days_until_stockout >= 9999 ? '-' : `${Math.round(i.days_until_stockout)}일`;
    const stockoutStyle = i.days_until_stockout <= i.lead_time_days ? 'color:#DC2626;font-weight:700' : '';
    const orders = i.pending_orders || [];
    let orderInfo;
    if (i.has_pending_order && orders.length > 0) {
      const totalQty = orders.reduce((s, o) => s + (o.qty || 0), 0);
      const tags = orders.map(o => `<span style="display:inline-block;padding:2px 6px;background:#ECFDF5;border:1px solid #A7F3D0;border-radius:4px;white-space:nowrap;font-size:10px;color:#065F46">${o.order_date||'-'} ${(o.qty||0).toLocaleString()}</span>`).join(' ');
      orderInfo = `<div style="display:flex;align-items:center;gap:6px;flex-wrap:nowrap"><span style="font-weight:700;color:#059669;font-size:12px;white-space:nowrap">✅ ${totalQty.toLocaleString()}</span>${tags}</div>`;
    } else if (i.shipping_date) {
      orderInfo = '<span style="color:#7c3aed;font-size:11px">🚢 선적확인</span>';
    } else {
      orderInfo = i.need_order ? '<span style="color:#DC2626;font-size:11px">❌ 미발주</span>' : '<span style="color:#94a3b8;font-size:11px">여유</span>';
    }

    return `<tr style="border-bottom:1px solid #f1f5f9;cursor:pointer" onclick="ipShowDetail(${i.id})">
      <td style="padding:8px">${statusBadge(i.status, i.status_label)}</td>
      <td style="padding:8px;font-weight:600;font-size:12px">${i.model_name||i.prod_cd}</td>
      <td style="padding:8px;text-align:right;font-weight:600">${(i.current_stock||0).toLocaleString()}</td>
      <td style="padding:8px;text-align:right">${i.avg_daily_7d ? i.avg_daily_7d : '<span style="color:#cbd5e1">-</span>'}</td>
      <td style="padding:8px;text-align:right;${stockoutStyle}">${stockout}</td>
      <td style="padding:8px;text-align:right;font-weight:600;${i.recommended_qty>0?'color:#DC2626':''}">${i.recommended_qty>0?i.recommended_qty.toLocaleString():'-'}</td>
      <td style="padding:8px;font-size:12px;${i.need_order?'color:#DC2626;font-weight:600':''}">${i.order_deadline||'-'}</td>
      <td style="padding:8px">${orderInfo}</td>
      <td style="padding:8px;font-size:11px">${(()=>{const se=i.shipping_entries||[];if(!se.length)return'-';return se.map(e=>'<span style="display:inline-block;padding:1px 5px;background:#F5F3FF;border:1px solid #DDD6FE;border-radius:4px;color:#7c3aed;white-space:nowrap;margin:1px">'+e.shipping_date+'</span>').join(' ');})()}</td>
      <td style="padding:8px;font-size:11px">${(()=>{const se=i.shipping_entries||[];if(!se.length)return'-';return se.map(e=>'<span style="display:inline-block;padding:1px 5px;background:#ECFDF5;border:1px solid #A7F3D0;border-radius:4px;color:#059669;font-weight:600;white-space:nowrap;margin:1px">'+(e.arrival_date||'-')+'</span>').join(' ');})()}</td>
      <td style="padding:8px"><button onclick="event.stopPropagation();ipDeleteTarget(${i.id},'${i.model_name}')" style="background:none;border:none;cursor:pointer;color:#dc2626;font-size:12px">삭제</button></td>
    </tr>`;
  }).join('');

  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" style="padding:20px;text-align:center;color:#94a3b8">해당하는 품목이 없습니다.</td></tr>';
  }
}

function ipFilterTable() {
  if (_ipData) ipRenderTable(_ipData.items);
}

// ── 관리품목 추가 모달 ──
let _ipAddMode = 'search'; // 'search' or 'manual'

function ipShowAddModal() {
  _ipSelected = null;
  _ipAddMode = 'search';
  document.getElementById('ip-add-search').value = '';
  document.getElementById('ip-search-results').style.display = 'none';
  document.getElementById('ip-add-selected').style.display = 'none';
  document.getElementById('ip-add-btn').disabled = true;
  document.getElementById('ip-add-btn').style.opacity = '0.5';
  document.getElementById('ip-add-leadtime').value = '40';
  document.getElementById('ip-add-safety').value = '10';
  if (document.getElementById('ip-add-moq')) document.getElementById('ip-add-moq').value = '0';
  if (document.getElementById('ip-add-supplier')) document.getElementById('ip-add-supplier').value = '';
  if (document.getElementById('ip-manual-model')) document.getElementById('ip-manual-model').value = '';
  if (document.getElementById('ip-manual-prodcd')) document.getElementById('ip-manual-prodcd').value = '';
  if (document.getElementById('ip-manual-prodname')) document.getElementById('ip-manual-prodname').value = '';
  ipSwitchAddTab('search');
  document.getElementById('ip-add-modal').style.display = 'flex';
}

function ipSwitchAddTab(mode) {
  _ipAddMode = mode;
  document.getElementById('ip-add-search-tab').style.display = mode === 'search' ? 'block' : 'none';
  document.getElementById('ip-add-manual-tab').style.display = mode === 'manual' ? 'block' : 'none';
  document.getElementById('ip-tab-search').style.borderBottomColor = mode === 'search' ? '#2563eb' : 'transparent';
  document.getElementById('ip-tab-search').style.color = mode === 'search' ? '#2563eb' : '#64748b';
  document.getElementById('ip-tab-manual').style.borderBottomColor = mode === 'manual' ? '#2563eb' : 'transparent';
  document.getElementById('ip-tab-manual').style.color = mode === 'manual' ? '#2563eb' : '#64748b';
  // 신규 입력 탭은 필수 필드 입력 시 버튼 활성화
  if (mode === 'manual') {
    document.getElementById('ip-add-btn').disabled = false;
    document.getElementById('ip-add-btn').style.opacity = '1';
  } else {
    document.getElementById('ip-add-btn').disabled = !_ipSelected;
    document.getElementById('ip-add-btn').style.opacity = _ipSelected ? '1' : '0.5';
  }
}

function ipCloseAddModal() {
  document.getElementById('ip-add-modal').style.display = 'none';
}

let _ipSearchTimer = null;
function ipSearchProducts(q) {
  clearTimeout(_ipSearchTimer);
  const resultsDiv = document.getElementById('ip-search-results');
  if (!q || q.length < 2) { resultsDiv.style.display = 'none'; return; }
  _ipSearchTimer = setTimeout(async () => {
    try {
      const data = await api.get(`/api/inventory-planning/search?q=${encodeURIComponent(q)}`);
      if (!data.items || data.items.length === 0) {
        resultsDiv.innerHTML = '<div style="padding:12px;color:#94a3b8;font-size:13px">검색 결과 없음</div>';
      } else {
        resultsDiv.innerHTML = data.items.map(p => `
          <div onclick="ipSelectProduct(${JSON.stringify(p).replace(/"/g,'&quot;')})"
               style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f1f5f9;font-size:13px;hover:background:#f8fafc"
               onmouseover="this.style.background='#f0f9ff'" onmouseout="this.style.background=''">
            <b>${p.model_name||p.prod_cd}</b>
            <span style="color:#64748b;margin-left:8px">${p.prod_name||''}</span>
            <span style="color:#94a3b8;margin-left:8px;font-size:11px">${p.prod_cd}</span>
          </div>
        `).join('');
      }
      resultsDiv.style.display = 'block';
    } catch (e) { resultsDiv.style.display = 'none'; }
  }, 300);
}

function ipSelectProduct(p) {
  _ipSelected = p;
  document.getElementById('ip-search-results').style.display = 'none';
  document.getElementById('ip-add-selected').style.display = 'block';
  document.getElementById('ip-add-selected').innerHTML = `
    <div style="font-weight:600">${p.model_name||p.prod_cd}</div>
    <div style="font-size:12px;color:#64748b">${p.prod_name||''} | ${p.prod_cd}</div>
  `;
  document.getElementById('ip-add-btn').disabled = false;
  document.getElementById('ip-add-btn').style.opacity = '1';
}

async function ipAddTarget() {
  let prod_cd, model_name, prod_name;

  if (_ipAddMode === 'manual') {
    model_name = (document.getElementById('ip-manual-model').value || '').trim();
    prod_cd = (document.getElementById('ip-manual-prodcd').value || '').trim();
    prod_name = (document.getElementById('ip-manual-prodname').value || '').trim();
    if (!model_name || !prod_cd) {
      toast('모델명과 품목코드는 필수입니다', 'error');
      return;
    }
  } else {
    if (!_ipSelected) return;
    prod_cd = _ipSelected.prod_cd;
    model_name = _ipSelected.model_name || '';
    prod_name = _ipSelected.prod_name || '';
  }

  try {
    await api.post('/api/inventory-planning/targets', {
      prod_cd,
      model_name,
      prod_name,
      lead_time_days: parseInt(document.getElementById('ip-add-leadtime').value) || 40,
      safety_stock_days: parseInt(document.getElementById('ip-add-safety').value) || 10,
      moq: parseInt(document.getElementById('ip-add-moq').value) || 0,
      supplier_group: (document.getElementById('ip-add-supplier').value || '').trim(),
    });
    ipCloseAddModal();
    toast('품목이 등록되었습니다.', 'success');
    ipRefreshAnalysis();
  } catch (err) { toast('등록 실패: ' + (err.message||err), 'error'); }
}

async function ipDeleteTarget(id, name) {
  if (!confirm(`"${name}" 품목을 관리 목록에서 제거하시겠습니까?`)) return;
  try {
    await api.delete(`/api/inventory-planning/targets/${id}`);
    toast('삭제 완료', 'success');
    ipRefreshAnalysis();
  } catch (err) { toast('삭제 실패: ' + (err.message||err), 'error'); }
}

// ── 상세 모달 ──
async function ipShowDetail(targetId) {
  document.getElementById('ip-detail-modal').style.display = 'flex';
  document.getElementById('ip-detail-content').innerHTML = '<div style="text-align:center;padding:30px;color:#94a3b8">⏳ 로딩 중...</div>';
  try {
    const d = await api.get(`/api/inventory-planning/analysis/${targetId}`);
    document.getElementById('ip-detail-title').textContent = `${d.model_name||d.prod_cd} 상세 분석`;

    const statusColors = {urgent:'#DC2626',warning:'#D97706',ordered:'#059669',safe:'#64748B',no_sales:'#94A3B8'};

    // 일별 판매 차트 (간이 바 차트)
    const sales = d.daily_sales || [];
    const maxSale = Math.max(...sales.map(s => s.sales), 1);
    const chartHtml = sales.length > 0 ? `
      <div style="margin:16px 0">
        <h4 style="margin:0 0 8px;font-size:14px">📊 최근 ${sales.length}일 판매 추이</h4>
        <div style="display:flex;align-items:flex-end;gap:2px;height:100px;border-bottom:1px solid #e2e8f0;padding-bottom:4px">
          ${sales.map(s => {
            const h = Math.max(2, (s.sales / maxSale) * 90);
            const col = s.sales > d.avg_daily_30d * 2 ? '#DC2626' : (s.sales > 0 ? '#3B82F6' : '#E2E8F0');
            const dt = s.date; const label = dt.substring(4,6)+'/'+dt.substring(6);
            return `<div style="flex:1;display:flex;flex-direction:column;align-items:center;min-width:0" title="${label}: ${s.sales}개 판매 (재고:${Math.round(s.stock)})">
              <div style="width:100%;max-width:14px;height:${h}px;background:${col};border-radius:2px 2px 0 0"></div>
            </div>`;
          }).join('')}
        </div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:#94a3b8;margin-top:2px">
          <span>${sales[0]?.date?.substring(4,6)+'/'+sales[0]?.date?.substring(6)||''}</span>
          <span>${sales[sales.length-1]?.date?.substring(4,6)+'/'+sales[sales.length-1]?.date?.substring(6)||''}</span>
        </div>
      </div>` : '';

    // 오더 정보
    const ordersHtml = d.pending_orders?.length > 0 ? `
      <div style="margin-top:16px">
        <h4 style="margin:0 0 8px;font-size:14px">📋 오더리스트</h4>
        ${d.pending_orders.map(o => `
          <div style="padding:8px;background:#ECFDF5;border-radius:6px;margin-bottom:4px;font-size:13px">
            <b>${o.order_no||'N/A'}</b> | ${o.order_date||'-'} | ${o.qty||0} ${o.unit||'PCS'} | 탭: ${o.sheet_tab||''}
          </div>`).join('')}
      </div>` : '';

    document.getElementById('ip-detail-content').innerHTML = `
      <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px">
        <div style="flex:1;min-width:200px;padding:12px;background:#f8fafc;border-radius:8px">
          <div style="font-size:12px;color:#64748b">상태</div>
          <div style="font-size:18px;font-weight:700;color:${statusColors[d.status]||'#64748b'}">${d.status_label}</div>
        </div>
        <div style="flex:1;min-width:200px;padding:12px;background:#f8fafc;border-radius:8px">
          <div style="font-size:12px;color:#64748b">현재고</div>
          <div style="font-size:18px;font-weight:700">${(d.current_stock||0).toLocaleString()}개</div>
        </div>
        <div style="flex:1;min-width:200px;padding:12px;background:#f8fafc;border-radius:8px">
          <div style="font-size:12px;color:#64748b">소진 예상</div>
          <div style="font-size:18px;font-weight:700;color:${d.days_until_stockout<=d.lead_time_days?'#DC2626':'inherit'}">${d.days_until_stockout>=9999?'판매없음':Math.round(d.days_until_stockout)+'일'}</div>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;margin-bottom:16px">
        <div>일평균 판매 (7일): <b>${d.avg_daily_7d}</b>개</div>
        <div>일평균 판매 (30일): <b>${d.avg_daily_30d}</b>개</div>
        <div>일평균 판매 (3개월): <b>${d.avg_daily_90d||0}</b>개</div>
        <div>일평균 판매 (6개월): <b>${d.avg_daily_180d||0}</b>개</div>
        <div>30일 총 판매: <b>${(d.total_sold_30d||0).toLocaleString()}</b>개</div>
        <div>3개월 총 판매: <b>${(d.total_sold_90d||0).toLocaleString()}</b>개</div>
        <div>판매일수: <b>${d.selling_days||0}</b>일 / ${d.daily_sales?.length||0}일</div>
        <div>리드타임: <b>${d.lead_time_days}일</b> | 안전재고: <b>${d.safety_stock_days}일</b></div>
        <div>권장 발주수량: <b style="color:${d.recommended_qty>0?'#DC2626':'inherit'}">${d.recommended_qty>0?d.recommended_qty.toLocaleString()+'개':'발주 불필요'}</b> <span style="font-size:11px;color:#94a3b8">(3개월 평균 기준)</span></div>
        <div>발주 기한: <b style="color:${d.need_order?'#DC2626':'inherit'}">${d.order_deadline||'-'}</b></div>
      </div>
      ${chartHtml}
      ${ordersHtml}
    `;
  } catch (err) {
    document.getElementById('ip-detail-content').innerHTML = `<div style="color:#dc2626">로딩 실패: ${err.message||err}</div>`;
  }
}

function ipCloseDetailModal() {
  document.getElementById('ip-detail-modal').style.display = 'none';
}

async function ipScanShippingMails() {
  if (!confirm('선적메일 스캔 + 오더리스트 최신화를 통합 실행합니다.')) return;

  // 로그 패널 표시
  let logPanel = document.getElementById('ip-log-panel');
  if (!logPanel) {
    logPanel = document.createElement('div');
    logPanel.id = 'ip-log-panel';
    logPanel.style.cssText = 'position:fixed;bottom:20px;right:20px;width:420px;max-height:350px;background:#1e293b;color:#e2e8f0;border-radius:12px;padding:16px;font-family:monospace;font-size:12px;z-index:1000;box-shadow:0 8px 30px rgba(0,0,0,0.3);overflow-y:auto';
    document.body.appendChild(logPanel);
  }
  logPanel.style.display = 'block';
  logPanel.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px"><b style="color:#60a5fa">📧 통합 스캔 로그</b><button onclick="this.parentElement.parentElement.style.display=\'none\'" style="background:none;border:none;color:#94a3b8;cursor:pointer;font-size:16px">✕</button></div><div id="ip-log-progress" style="background:#334155;border-radius:6px;height:6px;margin-bottom:10px;overflow:hidden"><div id="ip-log-bar" style="height:100%;background:#3b82f6;width:0%;transition:width 0.3s"></div></div><div id="ip-log-lines"></div>';

  const logLines = document.getElementById('ip-log-lines');
  const logBar = document.getElementById('ip-log-bar');

  function addLog(msg, isError) {
    const line = document.createElement('div');
    line.style.cssText = `padding:3px 0;border-bottom:1px solid #334155;color:${isError ? '#f87171' : '#e2e8f0'}`;
    line.textContent = `${new Date().toLocaleTimeString()} ${msg}`;
    logLines.appendChild(line);
    logPanel.scrollTop = logPanel.scrollHeight;
  }

  try {
    const resp = await fetch('/api/inventory-planning/shipping/scan-all', {
      method: 'POST',
      headers: api._headers ? api._headers() : {'Content-Type': 'application/json'},
    });

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const {done, value} = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, {stream: true});
      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const data = JSON.parse(line.substring(6));
            addLog(data.msg, data.step?.includes('error'));
            if (logBar && data.pct !== undefined) logBar.style.width = data.pct + '%';
            if (data.step === 'done') {
              logBar.style.background = '#22c55e';
              setTimeout(() => ipRefreshAnalysis(), 500);
            }
          } catch (e) {}
        }
      }
    }
  } catch (err) {
    addLog('❌ 스캔 실패: ' + (err.message || err), true);
  }
}

async function ipSyncBorOrderlist() {
  // 통합 스캔에 포함되어 별도 사용 안함
}

function ipExportExcel() {
  window.open('/api/inventory-planning/export/excel', '_blank');
}

async function ipShowSchedulerModal() {
  try {
    const status = await api.get('/api/inventory-planning/scheduler/status');
    const enabled = status.enabled;
    const hour = status.hour || 8;
    const minute = status.minute || 0;

    const modal = document.createElement('div');
    modal.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:1000;display:flex;align-items:center;justify-content:center';
    modal.innerHTML = `
      <div style="background:#fff;border-radius:12px;padding:24px;width:360px;box-shadow:0 8px 30px rgba(0,0,0,0.2)">
        <h3 style="margin:0 0 16px">⏰ 자동 스캔 설정</h3>
        <div style="margin-bottom:16px">
          <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
            <input type="checkbox" id="sched-enabled" ${enabled?'checked':''} style="width:18px;height:18px">
            <span>매일 자동 스캔 활성화</span>
          </label>
        </div>
        <div style="margin-bottom:16px;display:flex;align-items:center;gap:8px">
          <span>시간 (KST):</span>
          <input type="number" id="sched-hour" value="${hour}" min="0" max="23" style="width:60px;padding:6px;border:1px solid #e2e8f0;border-radius:6px">
          <span>:</span>
          <input type="number" id="sched-minute" value="${minute}" min="0" max="59" style="width:60px;padding:6px;border:1px solid #e2e8f0;border-radius:6px">
        </div>
        <p style="font-size:12px;color:#64748b;margin-bottom:16px">BOR 오더리스트 + 선적 + NAM 오더를 매일 자동으로 스캔합니다.</p>
        <div style="display:flex;gap:8px;justify-content:flex-end">
          <button onclick="this.closest('div[style]').parentElement.remove()" style="padding:8px 16px;border:1px solid #e2e8f0;border-radius:6px;background:#fff;cursor:pointer">취소</button>
          <button onclick="ipSaveScheduler()" style="padding:8px 16px;border:none;border-radius:6px;background:#2563eb;color:#fff;cursor:pointer">저장</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.onclick = (e) => { if (e.target === modal) modal.remove(); };
  } catch (err) {
    toast('스케줄러 설정 로드 실패', 'error');
  }
}

async function ipSaveScheduler() {
  const enabled = document.getElementById('sched-enabled').checked;
  const hour = parseInt(document.getElementById('sched-hour').value) || 8;
  const minute = parseInt(document.getElementById('sched-minute').value) || 0;
  try {
    const data = await api.post('/api/inventory-planning/scheduler/set', {enabled, hour, minute});
    toast(data.message || '설정 완료', 'success');
    document.querySelector('div[style*="position:fixed"][style*="z-index:1000"]')?.remove();
  } catch (err) {
    toast('설정 실패: ' + (err.message || err), 'error');
  }
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
  const totalRegular = s.total_matched + s.total_unmatched + (s.total_amount_mismatch || 0);
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
    const vendorRegularTotal = vs.matched_count + vs.unmatched_count + (vs.amount_mismatch_count || 0);
    const vendorMatchPct = vendorRegularTotal > 0
      ? Math.round(vs.matched_count / vendorRegularTotal * 100) : 0;
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
            <span class="rc-item-meta">${rcFmtDate(v.date, e.date)}</span>
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
          const erpDate = r.absorbed_erp_date || "";
          return `<div class="rc-detail-row">
            <span class="rc-icon" style="color:#16a34a">✓</span>
            <span class="rc-item-name">${pname}</span>
            <span class="rc-item-meta">${rcFmtDate(v.date, erpDate)}</span>
            <span class="rc-item-meta">${(v.amount||0).toLocaleString()}원</span>
            ${absorbedBy ? `<span class="rc-arrow">→</span><span class="rc-item-meta" style="color:var(--gray-400);font-size:10px">${r.absorbed_erp_name||absorbedBy} 단가에 반영</span>` : ""}
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
            <span class="rc-item-meta">${rcFmtDate(v.date, e.date)}</span>
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
            <span class="rc-item-meta">${rcFmtDate(v.date, "")}</span>
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
            <span class="rc-item-meta">${rcFmtDate(p.date, "")}</span>
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
            <span class="rc-item-meta">${rcFmtDate(v.date, "")}</span>
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
          const bestDate = best ? (best.date || "") : "";
          return `<div class="rc-detail-row">
            <span class="rc-icon">${sc.has_sales_history ? "📦" : "❓"}</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${rcFmtDate(v.date, bestDate)} | ${(v.amount||0).toLocaleString()}원</span>
            ${best ? `<span class="rc-arrow">→</span><span class="rc-erp-name">${best.product_code||""} ${best.product_name||""} (${Math.round((best.confidence||0)*100)}%)</span>${best.reason ? `<span style="font-size:10px;color:#6b7280;display:block;margin-left:28px;margin-top:2px">💡 ${best.reason}</span>` : ""}` : ""}
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
          const se = si.erp_match||{};
          return `<div class="rc-detail-row">
            <span class="rc-icon">🚚</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${rcFmtDate(v.date, se.date)}</span>
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
          const e = r.erp_match||{};
          const vAmt = r.vendor_amount || v.amount || 0;
          const eAmt = r.erp_amount || 0;
          const diff = r.amount_diff || 0;
          const vQty = r.vendor_qty || v.qty || 0;
          const eQty = r.erp_qty || 0;
          const reason = r.mismatch_reason || "";
          return `<div class="rc-detail-row" style="flex-wrap:wrap;gap:4px">
            <span class="rc-icon">⚠️</span>
            <span class="rc-item-name">${v.product_name||""}</span>
            <span class="rc-item-meta">${rcFmtDate(v.date, e.date)}</span>
            <span class="rc-item-meta" style="color:#6366f1">원장 ${vQty ? vQty+"개 " : ""}${vAmt.toLocaleString()}원</span>
            <span class="rc-arrow">→</span>
            <span class="rc-item-meta" style="color:#0891b2">매입 ${eQty ? eQty+"개 " : ""}${eAmt.toLocaleString()}원</span>
            <span class="rc-item-meta" style="color:${diff > 0 ? '#dc2626' : '#16a34a'};font-weight:600">차액 ${diff > 0 ? "+" : ""}${diff.toLocaleString()}원</span>
            ${reason ? `<span class="rc-item-meta" style="color:#f59e0b;font-size:10px;width:100%;padding-left:24px">💡 ${reason}</span>` : ""}
            ${r.sales_verify ? (() => {
              const sv = r.sales_verify;
              const vc = sv.verdict_code;
              const vColor = vc==='vendor'?'#b91c1c':vc==='erp'?'#15803d':'#374151';
              const bgColor = vc==='vendor'?'#fef2f2':vc==='erp'?'#f0fdf4':'#f9fafb';
              const borderColor = vc==='vendor'?'#fca5a5':vc==='erp'?'#86efac':'#d1d5db';
              let html = `<div style="width:100%;padding-left:24px;margin-top:4px">`;
              const aiReason = sv.ai_reason || "";
              html += `<div style="background:${bgColor};border:1px solid ${borderColor};border-radius:8px;padding:10px 12px;font-size:11px">`;
              html += `<div style="color:${vColor};font-weight:700;margin-bottom:4px;font-size:12px">📊 ${sv.verdict}</div>`;
              if (aiReason) {
                html += `<div style="color:#374151;font-size:11px;margin-bottom:8px;line-height:1.5">🤖 ${aiReason}</div>`;
              }
              if (sv.sales_details && sv.sales_details.length > 0) {
                html += `<table style="width:100%;font-size:11px;border-collapse:collapse;margin-top:4px">`;
                html += `<thead><tr style="color:#1f2937;font-weight:700;border-bottom:2px solid #9ca3af">
                  <th style="text-align:left;padding:4px 8px">판매일자</th>
                  <th style="text-align:left;padding:4px 8px">거래처</th>
                  <th style="text-align:right;padding:4px 8px">수량</th>
                  <th style="text-align:right;padding:4px 8px">금액</th>
                </tr></thead><tbody>`;
                sv.sales_details.forEach(sd => {
                  html += `<tr style="color:#111827;border-bottom:1px solid #e5e7eb">
                    <td style="padding:4px 8px;font-weight:500">${rcFmtDate("", sd.date)}</td>
                    <td style="padding:4px 8px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${sd.cust_name||""}</td>
                    <td style="text-align:right;padding:4px 8px;font-weight:600">${sd.qty||0}개</td>
                    <td style="text-align:right;padding:4px 8px;font-weight:500">${(sd.amount||0).toLocaleString()}원</td>
                  </tr>`;
                });
                html += `</tbody><tfoot><tr style="color:#111827;font-weight:700;border-top:2px solid #6b7280;background:rgba(0,0,0,0.03)">
                  <td style="padding:5px 8px">합계</td>
                  <td style="padding:5px 8px">${sv.sales_count}건</td>
                  <td style="text-align:right;padding:5px 8px">${sv.sales_total_qty}개</td>
                  <td style="text-align:right;padding:5px 8px">${(sv.sales_total_amt||0).toLocaleString()}원</td>
                </tr></tfoot></table>`;
              }
              html += `</div></div>`;
              return html;
            })() : ''}
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
            <span class="rc-item-meta">${rcFmtDate("", eDate)}</span>
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
              const regularTotal = vs.matched_count + vs.unmatched_count + (vs.amount_mismatch_count || 0);
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
/** ERP 날짜에서 전표번호 추출하여 MM.DD-NN 형식으로 반환 */
function rcFmtDate(vendorDate, erpDate) {
  const ed = String(erpDate || "");
  if (ed) {
    // "20260303-14" → "03.03-14"
    const m = ed.match(/(?:\d{4})?(\d{2})(\d{2})-(\d+)/);
    if (m) return `${m[1]}.${m[2]}-${m[3]}`;
    // "03-14" or "03.03-14"
    const m2 = ed.match(/(\d{1,2})[.\-\/](\d{1,2})[.\-](\d+)/);
    if (m2) return `${m2[1].padStart(2,"0")}.${m2[2].padStart(2,"0")}-${m2[3]}`;
  }
  return vendorDate || ed || "";
}

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
    // vendor_total_match 플래그만으로 총액 일치 여부 판단 (amount_mismatch 있으면 불일치)
    const allTotalMatch = (_rc.batchResult.vendor_results || []).every(vr =>
      vr.error || vr.vendor_total_match === true
    );
    if (allTotalMatch) {
      toast("모든 거래처 총액이 일치하여 추가 입력이 필요 없습니다.", "success");
      return;
    }
    // 총액 불일치 거래처가 있으면 → 빈 상태로 step 3 진입 (수동 입력 가능)
    _renderPurchaseEditCards();
    reconcileSetStep(3);
    toast("총액 불일치 거래처가 있습니다. 차액분을 수동으로 추가 입력하세요.", "warning");
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


// ═══════════════════════════════════════════════════════
// 리베이트 자동계산기
// ═══════════════════════════════════════════════════════
(function() {
  'use strict';

  let rbRunId = null;
  let rbResult = null;
  let rbSettings = null;
  let _rbInitialized = false;

  function rbFmt(n) { return Number(n).toLocaleString('ko-KR'); }

  function rbToast(msg, type) {
    const el = document.createElement('div');
    el.className = `rb-toast ${type || 'info'}`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3500);
  }

  function rbApi(method, path, body, isForm) {
    const opts = { method, headers: {} };
    const token = localStorage.getItem("token");
    if (token) opts.headers["Authorization"] = "Bearer " + token;
    if (body && !isForm) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    } else if (body && isForm) {
      opts.body = body;
    }
    return fetch(path, opts).then(async res => {
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      return res.json();
    });
  }

  // 탭 전환
  window.switchRebateTab = function(tabId) {
    document.querySelectorAll('.rb-tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.rebate-tab-content').forEach(c => c.classList.remove('active'));
    const btn = document.querySelector(`.rb-tab[data-rtab="${tabId}"]`);
    if (btn) btn.classList.add('active');
    const content = document.getElementById('rtab-' + tabId);
    if (content) content.classList.add('active');
    if (tabId === 'history') rbLoadHistory();
    if (tabId === 'rsettings') rbLoadSettings();
  };

  // 페이지 초기화
  window.initRebatePage = function() {
    if (_rbInitialized) return;
    _rbInitialized = true;

    // 업로드 영역
    const area = document.getElementById('rebateUploadArea');
    const fileInput = document.getElementById('rebateCsvFile');
    const btnCalc = document.getElementById('btnRebateCalc');

    area.addEventListener('click', () => fileInput.click());
    area.addEventListener('dragover', e => { e.preventDefault(); area.classList.add('dragging'); });
    area.addEventListener('dragleave', () => { area.classList.remove('dragging'); });
    area.addEventListener('drop', e => {
      e.preventDefault();
      area.classList.remove('dragging');
      if (e.dataTransfer.files.length) {
        fileInput.files = e.dataTransfer.files;
        rbOnFile();
      }
    });
    fileInput.addEventListener('change', rbOnFile);

    function rbOnFile() {
      if (fileInput.files.length) {
        const f = fileInput.files[0];
        area.querySelector('.rb-upload-text').innerHTML =
          `<strong>${f.name}</strong> 선택됨 <span style="color:var(--gray-400)">(${(f.size/1024).toFixed(0)} KB)</span>`;
        area.querySelector('.rb-upload-hint').textContent = '파일이 준비되었습니다. 아래 버튼을 클릭하세요.';
        area.querySelector('.rb-upload-icon').style.color = 'var(--primary)';
        btnCalc.disabled = false;
      }
    }

    btnCalc.addEventListener('click', async () => {
      if (!fileInput.files.length) return;
      btnCalc.disabled = true;
      document.getElementById('rebateUploadSection').style.display = 'none';
      document.getElementById('rebateLoadingSection').style.display = 'block';
      document.getElementById('rebateResultSection').style.display = 'none';
      if (window._rbStopGuide) window._rbStopGuide();

      try {
        const fd = new FormData();
        fd.append('file', fileInput.files[0]);
        const result = await rbApi('POST', '/api/rebate/calculate', fd, true);
        rbRunId = result.run_id;
        rbResult = result;
        rbRenderResult(result);
        rbToast(`리베이트 계산 완료: ${result.summary.total_customers}개 거래처, ${rbFmt(result.summary.total_rebate)}원`, 'success');
      } catch (e) {
        rbToast(`계산 실패: ${e.message}`, 'error');
        document.getElementById('rebateUploadSection').style.display = 'block';
        if (window._rbStartGuide) window._rbStartGuide();
        btnCalc.disabled = false;
      } finally {
        document.getElementById('rebateLoadingSection').style.display = 'none';
      }
    });

    // 정산내역서 다운로드
    document.getElementById('btnRbExportCsv').addEventListener('click', () => rbExportCsv('all'));
    document.getElementById('btnRbExportSelected').addEventListener('click', () => rbExportCsv('selected'));

    // 전체선택 체크박스
    rbBindCheckAll();

    // ERP 제출
    document.getElementById('btnRbSubmitERP').addEventListener('click', rbSubmitERP);

    // 설정 저장 버튼들
    document.getElementById('btnRbSaveAllowed').addEventListener('click', rbSaveAllowed);
    document.getElementById('btnRbSaveTier').addEventListener('click', rbSaveTier);
    document.getElementById('btnRbSaveRates').addEventListener('click', rbSaveRates);
    document.getElementById('btnRbSaveExceptions').addEventListener('click', rbSaveExceptions);
    document.getElementById('btnRbAddException').addEventListener('click', rbAddException);
    document.getElementById('btnRbSaveRateUpgrade').addEventListener('click', rbSaveRateUpgrade);
    document.getElementById('btnRbAddRateUpgrade').addEventListener('click', rbAddRateUpgrade);
    document.getElementById('btnRbSaveExcluded').addEventListener('click', rbSaveExcluded);
    document.getElementById('btnRbSaveEmployees').addEventListener('click', rbSaveEmployees);
    document.getElementById('btnRbAddEmployee').addEventListener('click', rbAddEmployee);
    document.getElementById('btnRbUploadMaster').addEventListener('click', rbUploadMaster);
    document.getElementById('btnRbSaveErp').addEventListener('click', rbSaveErpDefaults);
  };

  // 결과 렌더링
  function rbRenderResult(result) {
    const s = result.summary;
    const month = result.target_month || '-';
    document.getElementById('rbSumMonth').textContent = month;
    document.getElementById('rbSumCustomers').textContent = s.total_customers;
    document.getElementById('rbSumTierInfo').textContent = `10%: ${s.tier_10_count}개 / 5%: ${s.tier_5_count}개`;
    document.getElementById('rbSumTotal').textContent = rbFmt(s.total_rebate);
    document.getElementById('rbSum10').textContent = rbFmt(s.tier_10_rebate);
    document.getElementById('rbSum5').textContent = rbFmt(s.tier_5_rebate);

    if (month && month.includes('-')) {
      // 대상월의 다음 달 마지막 일요일을 전표일자로 설정
      const [y, m] = month.split('-').map(Number);
      let nextMonth = m + 1, nextYear = y;
      if (nextMonth > 12) { nextMonth = 1; nextYear++; }
      // 다음 달의 마지막 날부터 역으로 일요일 찾기
      const lastDayOfNext = new Date(nextYear, nextMonth, 0);
      const dayOfWeek = lastDayOfNext.getDay(); // 0=일요일
      const lastSunday = new Date(lastDayOfNext);
      lastSunday.setDate(lastDayOfNext.getDate() - dayOfWeek);
      const sy = lastSunday.getFullYear();
      const sm = String(lastSunday.getMonth() + 1).padStart(2, '0');
      const sd = String(lastSunday.getDate()).padStart(2, '0');
      document.getElementById('rbIoDate').value = `${sy}${sm}${sd}`;
    }

    rbRenderTable(result.customers);
    document.getElementById('rebateResultSection').style.display = 'block';

    // Feature 1: 이중 지급 방지 - 경고 배너 표시
    if (result.duplicate_warning) {
      const banner = document.createElement('div');
      banner.className = 'rb-warning-banner';
      banner.style.cssText = 'padding:16px;background:#fef3c7;border-left:4px solid #f59e0b;border-radius:6px;margin-bottom:16px;color:#92400e;font-size:13px';
      banner.innerHTML = `<strong style="display:block;margin-bottom:4px">⚠ 중복 리베이트 경고</strong>${result.duplicate_warning.message}<br/><span style="font-size:11px;color:#b45309">기존: ID ${result.duplicate_warning.existing_run_id}</span>`;
      const section = document.getElementById('rbSubmitSection');
      if (section) section.parentNode.insertBefore(banner, section);
    }

    if (result.status === 'submitted') {
      document.getElementById('rbSubmitSection').innerHTML = `
        <h3 class="rb-section-title">ERP 전표 생성</h3>
        <div style="padding:24px;text-align:center;color:var(--success);border-radius:8px;background:#f0fdf4">
          <svg width="32" height="32" viewBox="0 0 20 20" fill="currentColor" style="margin-bottom:8px"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"/></svg>
          <div style="font-size:14px;font-weight:600">이미 제출 완료된 리베이트입니다.</div>
        </div>`;
    } else {
      // Feature 3: 승인 워크플로우 로드
      rbLoadAnomaliesAndApproval();
    }
  }

  // 체크박스 선택 상태 관리
  let rbCheckedSet = new Set();

  function rbUpdateSelectionUI() {
    const count = rbCheckedSet.size;
    const countEl = document.getElementById('rbSelectedCount');
    const btnSelected = document.getElementById('btnRbExportSelected');
    if (countEl) countEl.textContent = count > 0 ? `${count}개 선택됨` : '';
    if (btnSelected) btnSelected.disabled = count === 0;
    // 전체선택 체크박스 상태
    const checkAll = document.getElementById('rbCheckAll');
    if (checkAll && rbResult) {
      const total = rbResult.customers.length;
      checkAll.checked = count > 0 && count === total;
      checkAll.indeterminate = count > 0 && count < total;
    }
  }

  function rbRenderTable(customers) {
    const tbody = document.getElementById('rbTableBody');
    tbody.innerHTML = '';
    rbCheckedSet.clear();
    customers.forEach((c, idx) => {
      const finalRebate = c.total_rebate + (c.manual_adjustment || 0);
      const tierCls = c.tier === '10%' ? 't10' : 't5';
      // 미분류 매출 계산
      const classifiedSales = (c.main_sales||0) + (c.lanstar3_sales||0) + (c.lanstar5_sales||0) + (c.printer_sales||0);
      const unclassifiedSales = (c.total_sales||0) - classifiedSales;

      const tr = document.createElement('tr');
      if (c.is_excluded) tr.className = 'excluded';
      tr.setAttribute('data-cust-id', c.customer_name);
      tr.innerHTML = `
        <td><input type="checkbox" class="rb-check rb-row-check" data-rbidx="${idx}" onchange="rbOnRowCheck(${idx},this.checked)"></td>
        <td><button class="btn btn-outline btn-sm" data-rbidx="${idx}" onclick="rbToggleDetail(${idx})">▶</button></td>
        <td>${c.customer_name}${c.customer_code ? `<span style="font-size:11px;color:var(--gray-400);margin-left:4px">${c.customer_code}</span>` : ''}</td>
        <td><span class="rb-tier ${tierCls}${c.is_exception ? ' exc' : ''}">${c.tier}${c.is_exception ? ' 예외' : ''}${c.is_rate_upgrade ? ' ↑' : ''}</span></td>
        <td class="r">${rbFmt(c.total_sales)}</td>
        <td class="r">${rbFmt(c.total_rebate)}</td>
        <td class="r" style="color:${c.manual_adjustment ? 'var(--primary)' : 'inherit'}">${c.manual_adjustment ? rbFmt(c.manual_adjustment) : '-'}</td>
        <td class="r" style="font-weight:600">${rbFmt(finalRebate)}</td>
        <td style="text-align:center"><button class="btn btn-sm ${c.is_excluded ? 'btn-outline' : 'btn-danger'}" onclick="rbToggleExclude(${c.id},${c.is_excluded?'false':'true'})">${c.is_excluded ? '복원' : '제외'}</button></td>
      `;
      tbody.appendChild(tr);

      const detailTr = document.createElement('tr');
      detailTr.className = 'rb-detail-row';
      detailTr.id = `rb-detail-${idx}`;
      detailTr.innerHTML = `
        <td colspan="9">
          <div class="rb-breakdown-grid">
            <div class="rb-breakdown-item">
              <div class="rb-breakdown-label">메인 (수입/심천/plus/단종)</div>
              <div class="rb-breakdown-amount">${rbFmt(c.main_sales)}</div>
              <div class="rb-breakdown-calc">×${c.main_sales?((c.main_rebate/c.main_sales)*100).toFixed(0):(c.tier==='10%'?10:5)}% = ${rbFmt(c.main_rebate)}</div>
            </div>
            <div class="rb-breakdown-item">
              <div class="rb-breakdown-label">랜스타 3%</div>
              <div class="rb-breakdown-amount">${rbFmt(c.lanstar3_sales)}</div>
              <div class="rb-breakdown-calc">×${c.lanstar3_sales?((c.lanstar3_rebate/c.lanstar3_sales)*100).toFixed(0):3}% = ${rbFmt(c.lanstar3_rebate)}</div>
            </div>
            <div class="rb-breakdown-item">
              <div class="rb-breakdown-label">랜스타 5%</div>
              <div class="rb-breakdown-amount">${rbFmt(c.lanstar5_sales)}</div>
              <div class="rb-breakdown-calc">×${c.lanstar5_sales?((c.lanstar5_rebate/c.lanstar5_sales)*100).toFixed(0):5}% = ${rbFmt(c.lanstar5_rebate)}</div>
            </div>
            <div class="rb-breakdown-item">
              <div class="rb-breakdown-label">프린터서버류</div>
              <div class="rb-breakdown-amount">${rbFmt(c.printer_sales)}</div>
              <div class="rb-breakdown-calc">×${c.printer_sales?((c.printer_rebate/c.printer_sales)*100).toFixed(0):7}% = ${rbFmt(c.printer_rebate)}</div>
            </div>
            ${unclassifiedSales !== 0 ? `
            <div class="rb-breakdown-item rb-breakdown-unclassified">
              <div class="rb-breakdown-label">미분류 매출</div>
              <div class="rb-breakdown-amount">${rbFmt(unclassifiedSales)}</div>
              <div class="rb-breakdown-calc">리베이트 미적용 (배송비·차감 등)</div>
            </div>` : ''}
          </div>
          ${c.is_rate_upgrade ? '<div style="margin-top:10px;font-size:12px;color:var(--primary);font-weight:500">⬆ 할인율 상향 적용 업체</div>' : ''}
          <div class="rb-breakdown-controls">
            <div class="rb-control-group">
              <label class="rb-field-label">금액 조정</label>
              <input type="number" class="rb-field-input" style="max-width:140px" placeholder="0" value="${c.manual_adjustment||''}" onchange="rbUpdateAdj(${c.id},this.value)">
            </div>
            <div class="rb-control-group">
              <label class="rb-field-label">담당 사원</label>
              <input type="text" class="rb-field-input" style="max-width:100px" placeholder="사원코드" value="${c.emp_cd||''}" onchange="rbUpdateEmp(${c.id},this.value)">
            </div>
          </div>
        </td>
      `;
      tbody.appendChild(detailTr);
    });
    rbUpdateSelectionUI();
  }

  // 행 체크박스 토글
  window.rbOnRowCheck = function(idx, checked) {
    if (checked) rbCheckedSet.add(idx);
    else rbCheckedSet.delete(idx);
    rbUpdateSelectionUI();
  };

  // 전체 선택 체크박스
  function rbBindCheckAll() {
    const el = document.getElementById('rbCheckAll');
    if (!el) return;
    el.addEventListener('change', function() {
      const checks = document.querySelectorAll('.rb-row-check');
      checks.forEach(cb => {
        cb.checked = el.checked;
        const idx = parseInt(cb.dataset.rbidx);
        if (el.checked) rbCheckedSet.add(idx);
        else rbCheckedSet.delete(idx);
      });
      rbUpdateSelectionUI();
    });
  }

  window.rbToggleDetail = function(idx) {
    const row = document.getElementById(`rb-detail-${idx}`);
    const btn = document.querySelector(`button[data-rbidx="${idx}"]`);
    if (row.classList.contains('open')) {
      row.classList.remove('open'); btn.textContent = '▶';
    } else {
      row.classList.add('open'); btn.textContent = '▼';
    }
  };

  window.rbToggleExclude = async function(detailId, exclude) {
    try {
      await rbApi('PUT', `/api/rebate/detail/${detailId}`, { is_excluded: exclude });
      const result = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
      rbResult = result;
      rbRenderResult(result);
      rbToast(exclude ? '거래처 제외됨' : '거래처 복원됨', 'info');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  };

  window.rbUpdateAdj = async function(detailId, value) {
    try {
      await rbApi('PUT', `/api/rebate/detail/${detailId}`, { manual_adjustment: parseInt(value)||0 });
      const result = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
      rbResult = result;
      rbRenderResult(result);
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  };

  window.rbUpdateEmp = async function(detailId, value) {
    try {
      await rbApi('PUT', `/api/rebate/detail/${detailId}`, { emp_cd: value });
      rbToast('담당 사원 업데이트', 'info');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  };

  function rbExportCsv(mode) {
    if (!rbResult) return;
    let customers = rbResult.customers;
    let suffix = '전체';

    if (mode === 'selected') {
      if (rbCheckedSet.size === 0) { rbToast('거래처를 선택해주세요.', 'error'); return; }
      customers = customers.filter((_, idx) => rbCheckedSet.has(idx));
      suffix = `선택${customers.length}건`;
    }

    const rows = [['거래처명','거래처코드','등급','총매출','메인매출','랜스타3%매출','랜스타5%매출','프린터서버류매출','미분류매출','메인리베이트','랜스타3%리베이트','랜스타5%리베이트','프린터서버류리베이트','총리베이트','조정','최종리베이트','예외','제외']];
    customers.forEach(c => {
      const classified = (c.main_sales||0) + (c.lanstar3_sales||0) + (c.lanstar5_sales||0) + (c.printer_sales||0);
      const unclassified = (c.total_sales||0) - classified;
      rows.push([c.customer_name,c.customer_code||'',c.tier,c.total_sales,c.main_sales,c.lanstar3_sales,c.lanstar5_sales,c.printer_sales,unclassified,c.main_rebate,c.lanstar3_rebate,c.lanstar5_rebate,c.printer_rebate,c.total_rebate,c.manual_adjustment||0,c.total_rebate+(c.manual_adjustment||0),c.is_exception?'Y':'',c.is_excluded?'Y':'']);
    });
    const csv = '\uFEFF' + rows.map(r => r.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `리베이트_정산내역서_${rbResult.target_month||'result'}_${suffix}.csv`;
    a.click();
    rbToast(`정산내역서 다운로드 완료 (${customers.length}건)`, 'success');
  }

  async function rbSubmitERP() {
    const ioDate = document.getElementById('rbIoDate').value.trim();
    if (!ioDate || ioDate.length !== 8) { rbToast('전표 일자를 YYYYMMDD 형식으로 입력하세요.', 'error'); return; }
    if (!rbRunId) return;
    const activeCount = rbResult.customers.filter(c => !c.is_excluded && c.total_rebate > 0).length;
    if (!confirm(`${activeCount}개 거래처에 대해 ERP 전표를 생성합니다.\n\n전표일자: ${ioDate}\n\n진행하시겠습니까?`)) return;

    document.getElementById('btnRbSubmitERP').disabled = true;
    document.getElementById('rbSubmitStatus').textContent = '처리 중...';
    try {
      const resp = await rbApi('POST', '/api/rebate/submit', { run_id: rbRunId, io_date: ioDate });
      rbToast(`ERP 전표 생성 완료: 성공 ${resp.success_count}건, 실패 ${resp.fail_count}건`, resp.fail_count ? 'error' : 'success');
      document.getElementById('rbSubmitStatus').textContent = `성공 ${resp.success_count}건 / 실패 ${resp.fail_count}건 / 총 ${rbFmt(resp.total_rebate)}원`;
      const result = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
      rbResult = result;
      rbRenderResult(result);
    } catch (e) {
      if (e.message && e.message.includes('다른 리베이트가 이미 제출')) {
        if (confirm(e.message + '\n\n강제로 제출하시겠습니까?')) {
          try {
            const resp = await rbApi('POST', '/api/rebate/submit', { run_id: rbRunId, io_date: ioDate, force: true });
            rbToast(`ERP 전표 생성 완료: 성공 ${resp.success_count}건`, 'success');
            document.getElementById('rbSubmitStatus').textContent = `성공 ${resp.success_count}건 / 총 ${rbFmt(resp.total_rebate)}원`;
            const result = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
            rbResult = result;
            rbRenderResult(result);
          } catch (e2) { rbToast(`강제 제출 실패: ${e2.message}`, 'error'); }
        }
      } else {
        rbToast(`ERP 제출 실패: ${e.message}`, 'error');
        document.getElementById('rbSubmitStatus').textContent = `오류: ${e.message}`;
      }
      document.getElementById('btnRbSubmitERP').disabled = false;
    }
  }

  // ═════════════════════════════════════════════════════════
  // Feature 1: 이중 지급 방지 - 결과 렌더링에서 경고 표시
  // Feature 3: 승인 워크플로우
  // ═════════════════════════════════════════════════════════

  // Feature 1, 3 통합: rbRenderResult 호출 후 추가 처리
  async function rbLoadAnomaliesAndApproval() {
    if (!rbRunId || !rbResult) return;

    // Feature 3: 승인 상태 확인 및 UI 업데이트
    try {
      const run = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
      rbResult._approval_status = run.approval_status || 'pending';
      rbResult._approved_by = run.approved_by;
      rbResult._approved_at = run.approved_at;

      // 승인 UI 업데이트
      rbUpdateApprovalUI();
    } catch (e) { /* ignore */ }
  }

  function rbUpdateApprovalUI() {
    const approvalSection = document.getElementById('rbApprovalSection');
    if (!approvalSection || !rbResult) return;

    const status = rbResult._approval_status || 'pending';
    const approvedBy = rbResult._approved_by;
    const approvedAt = rbResult._approved_at;

    if (status === 'approved') {
      approvalSection.innerHTML = `
        <h3 class="rb-section-title">승인 상태</h3>
        <div style="padding:16px;background:#f0fdf4;border-radius:8px;border-left:4px solid var(--success)">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--success)"></span>
            <strong style="color:var(--success)">승인됨</strong>
          </div>
          <div style="font-size:12px;color:#666">승인자: ${approvedBy || '-'}</div>
          <div style="font-size:12px;color:#666">승인시간: ${approvedAt || '-'}</div>
        </div>
      `;
    } else if (status === 'rejected') {
      approvalSection.innerHTML = `
        <h3 class="rb-section-title">승인 상태</h3>
        <div style="padding:16px;background:#fee2e2;border-radius:8px;border-left:4px solid var(--danger)">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--danger)"></span>
            <strong style="color:var(--danger)">거부됨</strong>
          </div>
          <div style="font-size:12px;color:#666">거부자: ${approvedBy || '-'}</div>
        </div>
      `;
    } else {
      // pending - 승인 요청 UI
      approvalSection.innerHTML = `
        <h3 class="rb-section-title">승인 워크플로우</h3>
        <div style="padding:16px;background:#f9fafb;border-radius:8px;border:1px solid #e5e7eb">
          <div style="font-size:13px;color:#666;margin-bottom:12px">이 리베이트를 제출하려면 먼저 승인이 필요합니다.</div>
          <div style="display:flex;gap:8px">
            <input type="text" id="rbEmpCd" placeholder="사원코드" style="flex:1;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:12px">
            <input type="text" id="rbApprovalNote" placeholder="메모 (선택사항)" style="flex:2;padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:12px">
            <button class="btn btn-success btn-sm" onclick="rbApproveRequest()">승인</button>
            <button class="btn btn-outline btn-sm" onclick="rbRejectRequest()">거부</button>
          </div>
        </div>
      `;
    }
  }

  window.rbApproveRequest = async function() {
    if (!rbRunId) return;
    const empCd = document.getElementById('rbEmpCd')?.value.trim() || '';
    const note = document.getElementById('rbApprovalNote')?.value.trim() || '';
    try {
      await rbApi('POST', '/api/rebate/approve', {
        run_id: rbRunId,
        action: 'approve',
        emp_cd: empCd,
        note: note
      });
      rbToast('승인되었습니다.', 'success');
      const result = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
      rbResult = result;
      rbUpdateApprovalUI();
    } catch (e) { rbToast(`승인 실패: ${e.message}`, 'error'); }
  };

  window.rbRejectRequest = async function() {
    if (!rbRunId) return;
    if (!confirm('거부하시겠습니까?')) return;
    const empCd = document.getElementById('rbEmpCd')?.value.trim() || '';
    const note = document.getElementById('rbApprovalNote')?.value.trim() || '거부';
    try {
      await rbApi('POST', '/api/rebate/approve', {
        run_id: rbRunId,
        action: 'reject',
        emp_cd: empCd,
        note: note
      });
      rbToast('거부되었습니다.', 'success');
      const result = await rbApi('GET', `/api/rebate/preview/${rbRunId}`);
      rbResult = result;
      rbUpdateApprovalUI();
    } catch (e) { rbToast(`거부 실패: ${e.message}`, 'error'); }
  };

  // Feature 4: 정산내역서 PDF 인쇄
  window.rbGenerateStatementPDF = async function() {
    if (!rbRunId || !rbResult) return;

    const customers = rbResult.customers.filter(c => !c.is_excluded && c.total_rebate > 0);
    if (customers.length === 0) { rbToast('인쇄할 거래처가 없습니다.', 'error'); return; }

    const month = rbResult.target_month || '월명';
    let html = `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>정산내역서</title>
        <style>
          @media print { body { margin: 0; padding: 0; } }
          body { font-family: 'Arial', sans-serif; margin: 20px; background: #f5f5f5; }
          .statement { page-break-after: always; background: white; padding: 40px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
          .header { text-align: center; margin-bottom: 30px; border-bottom: 2px solid #6366f1; padding-bottom: 15px; }
          .company-name { font-size: 18px; font-weight: bold; color: #6366f1; }
          .doc-title { font-size: 16px; font-weight: bold; margin-top: 10px; }
          .meta { display: flex; justify-content: space-between; margin-top: 15px; font-size: 12px; color: #666; }
          .customer-info { margin-bottom: 20px; }
          .info-row { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 13px; }
          .info-label { font-weight: bold; color: #333; }
          .info-value { color: #666; }
          table { width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 12px; }
          th { background: #f0f0f0; border: 1px solid #ddd; padding: 8px; text-align: left; font-weight: bold; }
          td { border: 1px solid #ddd; padding: 8px; text-align: right; }
          td:first-child { text-align: left; }
          .total-row { background: #f9f9f9; font-weight: bold; }
          .signature { display: flex; justify-content: space-between; margin-top: 40px; }
          .sig-box { width: 40%; border-top: 1px solid #333; padding-top: 10px; text-align: center; font-size: 11px; }
          .sig-label { color: #666; margin-top: 5px; }
        </style>
      </head>
      <body>
    `;

    customers.forEach(c => {
      const finalRebate = c.total_rebate + (c.manual_adjustment || 0);
      html += `
        <div class="statement">
          <div class="header">
            <div class="company-name">LANstar Co., Ltd.</div>
            <div class="doc-title">정산내역서</div>
            <div class="meta">
              <span>대상월: ${month}</span>
              <span>작성일: ${new Date().toLocaleDateString('ko-KR')}</span>
            </div>
          </div>

          <div class="customer-info">
            <div class="info-row">
              <span class="info-label">거래처명:</span>
              <span class="info-value">${c.customer_name}</span>
            </div>
            <div class="info-row">
              <span class="info-label">거래처코드:</span>
              <span class="info-value">${c.customer_code || '-'}</span>
            </div>
            <div class="info-row">
              <span class="info-label">등급:</span>
              <span class="info-value">${c.tier}</span>
            </div>
          </div>

          <table>
            <thead>
              <tr>
                <th>항목</th>
                <th>판매액</th>
                <th>요율</th>
                <th>정산액</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>메인상품</td>
                <td style="text-align: right">${c.main_sales.toLocaleString()}</td>
                <td style="text-align: center">${c.main_sales ? ((c.main_rebate/c.main_sales)*100).toFixed(0) : '-'}%</td>
                <td>${c.main_rebate.toLocaleString()}</td>
              </tr>
              <tr>
                <td>LANSTAR3</td>
                <td style="text-align: right">${c.lanstar3_sales.toLocaleString()}</td>
                <td style="text-align: center">3%</td>
                <td>${c.lanstar3_rebate.toLocaleString()}</td>
              </tr>
              <tr>
                <td>LANSTAR5</td>
                <td style="text-align: right">${c.lanstar5_sales.toLocaleString()}</td>
                <td style="text-align: center">5%</td>
                <td>${c.lanstar5_rebate.toLocaleString()}</td>
              </tr>
              <tr>
                <td>프린터</td>
                <td style="text-align: right">${c.printer_sales.toLocaleString()}</td>
                <td style="text-align: center">7%</td>
                <td>${c.printer_rebate.toLocaleString()}</td>
              </tr>
      `;

      if (c.manual_adjustment) {
        html += `
              <tr>
                <td>수동조정</td>
                <td style="text-align: right">-</td>
                <td style="text-align: center">-</td>
                <td>${c.manual_adjustment.toLocaleString()}</td>
              </tr>
        `;
      }

      html += `
              <tr class="total-row">
                <td>합계</td>
                <td style="text-align: right">${c.total_sales.toLocaleString()}</td>
                <td style="text-align: center">-</td>
                <td>${finalRebate.toLocaleString()}</td>
              </tr>
            </tbody>
          </table>

          <div class="signature">
            <div class="sig-box">
              <div class="sig-label">발행처</div>
            </div>
            <div class="sig-box">
              <div class="sig-label">인수 (${c.customer_name})</div>
            </div>
          </div>
        </div>
      `;
    });

    html += `</body></html>`;

    const win = window.open('', '_blank');
    win.document.write(html);
    win.document.close();
    setTimeout(() => win.print(), 250);
    rbToast('정산내역서가 열렸습니다.', 'info');
  };


  // ── ERP 가이드 모션그래픽 ──
  (function initGuideMotion() {
    let guideTimer = null;
    let guideStep = 0;

    function resetGuide() {
      const els = {
        menu: document.getElementById('rbGuideMenu'),
        date: document.getElementById('rbGuideDate'),
        arrow: document.getElementById('rbGuideArrow'),
        dropdown: document.getElementById('rbGuideDropdown'),
        csv: document.getElementById('rbGuideCsv'),
        cursor: document.getElementById('rbGuideCursor'),
      };
      if (!els.menu) return els;
      els.menu.classList.remove('rb-erp-highlight','rb-click-effect');
      if(els.date) els.date.classList.remove('rb-erp-date-highlight','rb-erp-highlight');
      if(els.arrow) els.arrow.classList.remove('rb-erp-arrow-highlight','rb-erp-highlight','rb-click-effect');
      if(els.dropdown) els.dropdown.classList.remove('rb-erp-dropdown-show');
      if(els.csv) els.csv.classList.remove('rb-erp-dd-csv-highlight','rb-click-effect');
      ['rbStep1','rbStep2','rbStep3','rbStep4'].forEach(id => {
        const s = document.getElementById(id);
        if (s) s.classList.remove('active');
      });
      if(els.cursor) { els.cursor.style.opacity = '0'; }
      return els;
    }

    function moveCursor(cursor, target, offsetX, offsetY) {
      if (!cursor || !target) return;
      const parent = cursor.closest('.rb-guide-screen');
      if (!parent) return;
      const pRect = parent.getBoundingClientRect();
      const tRect = target.getBoundingClientRect();
      cursor.style.left = (tRect.left - pRect.left + (offsetX||0)) + 'px';
      cursor.style.top = (tRect.top - pRect.top + (offsetY||0)) + 'px';
      cursor.style.opacity = '1';
    }

    function runGuideAnimation() {
      const els = resetGuide();
      if (!els.menu) return;
      guideStep = 0;

      // Step 1: 판매현황 클릭 (0ms ~ 1800ms)
      setTimeout(() => {
        const s1 = document.getElementById('rbStep1');
        if(s1) s1.classList.add('active');
        moveCursor(els.cursor, els.menu, 30, 8);
      }, 300);
      setTimeout(() => {
        els.menu.classList.add('rb-erp-highlight','rb-click-effect');
      }, 900);

      // Step 2: 기준일자 입력 (2000ms ~ 3800ms)
      setTimeout(() => {
        const s1 = document.getElementById('rbStep1');
        if(s1) s1.classList.remove('active');
        const s2 = document.getElementById('rbStep2');
        if(s2) s2.classList.add('active');
        els.menu.classList.remove('rb-erp-highlight','rb-click-effect');
        if(els.date) moveCursor(els.cursor, els.date, 40, 6);
      }, 2000);
      setTimeout(() => {
        if(els.date) els.date.classList.add('rb-erp-date-highlight');
      }, 2600);

      // Step 3: 화살표 클릭 (4000ms ~ 5800ms)
      setTimeout(() => {
        const s2 = document.getElementById('rbStep2');
        if(s2) s2.classList.remove('active');
        const s3 = document.getElementById('rbStep3');
        if(s3) s3.classList.add('active');
        if(els.date) els.date.classList.remove('rb-erp-date-highlight');
        moveCursor(els.cursor, els.arrow, 4, 4);
      }, 4000);
      setTimeout(() => {
        if(els.arrow) els.arrow.classList.add('rb-erp-arrow-highlight','rb-click-effect');
        if(els.dropdown) els.dropdown.classList.add('rb-erp-dropdown-show');
      }, 4600);

      // Step 4: CSV 클릭 (6000ms ~ 7800ms)
      setTimeout(() => {
        const s3 = document.getElementById('rbStep3');
        if(s3) s3.classList.remove('active');
        const s4 = document.getElementById('rbStep4');
        if(s4) s4.classList.add('active');
        if(els.arrow) els.arrow.classList.remove('rb-erp-arrow-highlight','rb-click-effect');
        moveCursor(els.cursor, els.csv, 20, 6);
      }, 6000);
      setTimeout(() => {
        if(els.csv) els.csv.classList.add('rb-erp-dd-csv-highlight','rb-click-effect');
      }, 6600);

      // 리셋 후 반복 (8500ms)
      setTimeout(() => {
        resetGuide();
      }, 8200);
    }

    function startGuide() {
      if (guideTimer) return;
      runGuideAnimation();
      guideTimer = setInterval(runGuideAnimation, 9000);
    }

    function stopGuide() {
      if (guideTimer) { clearInterval(guideTimer); guideTimer = null; }
      resetGuide();
    }

    // 탭 전환 시 가이드 시작/중지
    const origSwitch = window.switchRebateTab;
    window.switchRebateTab = function(tabId) {
      origSwitch(tabId);
      if (tabId === 'calc') {
        const uploadSec = document.getElementById('rebateUploadSection');
        if (uploadSec && uploadSec.style.display !== 'none') startGuide();
      } else {
        stopGuide();
      }
    };

    // 페이지 로드 시 자동 시작 (리베이트 탭 활성화 시)
    setTimeout(() => {
      const uploadSec = document.getElementById('rebateUploadSection');
      const calcTab = document.getElementById('rtab-calc');
      if (uploadSec && calcTab && calcTab.classList.contains('active') &&
          uploadSec.style.display !== 'none') {
        startGuide();
      }
    }, 500);

    // 결과 표시 시 가이드 중지
    window._rbStopGuide = stopGuide;
    window._rbStartGuide = startGuide;
  })();

  // ── 이력 ──
  function rbFormatKST(dtStr) {
    if (!dtStr) return '-';
    // 이미 KST로 저장된 값이면 그대로 표시, ISO 형식이면 변환
    try {
      // "2026-04-06 16:09:32" 형식이면 그대로 반환
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(dtStr)) return dtStr;
      // ISO 형식 "2026-04-06T07:09:32.334393" → KST 변환
      const d = new Date(dtStr + (dtStr.includes('+') || dtStr.includes('Z') ? '' : 'Z'));
      if (isNaN(d.getTime())) return dtStr;
      const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
      const pad = n => String(n).padStart(2, '0');
      return `${kst.getUTCFullYear()}-${pad(kst.getUTCMonth()+1)}-${pad(kst.getUTCDate())} ${pad(kst.getUTCHours())}:${pad(kst.getUTCMinutes())}:${pad(kst.getUTCSeconds())}`;
    } catch { return dtStr; }
  }

  async function rbLoadHistory() {
    try {
      const runs = await rbApi('GET', '/api/rebate/history');
      const tbody = document.getElementById('rbHistoryBody');
      tbody.innerHTML = '';
      runs.forEach(r => {
        const statusCls = r.status === 'submitted' ? 'submitted' : 'pending';
        const executor = r.executed_by_name ? `${r.executed_by_name}` : '-';
        tbody.innerHTML += `
          <tr>
            <td>${r.id}</td>
            <td>${r.target_month}</td>
            <td>${r.total_customers}</td>
            <td class="r">${rbFmt(r.total_rebate)}</td>
            <td><span class="status-badge ${statusCls}">${r.status==='submitted'?'제출완료':'계산완료'}</span></td>
            <td>${executor}</td>
            <td>${rbFormatKST(r.created_at)}</td>
            <td>${rbFormatKST(r.submitted_at)}</td>
            <td><button class="btn btn-outline btn-sm" onclick="rbLoadRun(${r.id})">보기</button></td>
          </tr>`;
      });
      if (!runs.length) tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:40px;color:#64748b">실행 이력이 없습니다.</td></tr>';
    } catch (e) { rbToast(`이력 로드 실패: ${e.message}`, 'error'); }
  }

  window.rbLoadRun = async function(runId) {
    switchRebateTab('calc');
    document.getElementById('rebateUploadSection').style.display = 'none';
    document.getElementById('rebateLoadingSection').style.display = 'block';
    document.getElementById('rebateResultSection').style.display = 'none';
    try {
      const result = await rbApi('GET', `/api/rebate/preview/${runId}`);
      rbRunId = runId;
      rbResult = result;
      rbRenderResult(result);
    } catch (e) { rbToast(`데이터 로드 실패: ${e.message}`, 'error'); }
    finally { document.getElementById('rebateLoadingSection').style.display = 'none'; }
  };

  // ── 설정 ──
  const RB_RATE_LABELS = {
    main: { label: '메인', group: '수입제품/심천시장/plus/단종/재고소진' },
    lanstar_3: { label: '랜스타 3%', group: '수입제품(랜스타)★ 3%할인품목' },
    lanstar_5: { label: '랜스타 5%', group: '수입제품(랜스타)★ 5%할인품목' },
    printer: { label: '프린터서버류', group: '프린터서버류(매출별할인) 5%/7%' },
  };

  async function rbLoadSettings() {
    try {
      rbSettings = await rbApi('GET', '/api/rebate/settings');
      rbRenderSettings(rbSettings);
    } catch (e) { rbToast(`설정 로드 실패: ${e.message}`, 'error'); }
  }

  function rbRenderSettings(s) {
    document.getElementById('rbUseAllowed').checked = s.use_allowed_list || false;
    document.getElementById('rbAllowedText').value = (s.allowed_customers || []).join('\n');
    document.getElementById('rbTier10').value = s.tier_thresholds.tier_10_min;
    document.getElementById('rbTier5').value = s.tier_thresholds.tier_5_min;

    const rBody = document.getElementById('rbRatesBody');
    rBody.innerHTML = '';
    for (const [key, rates] of Object.entries(s.discount_rates)) {
      const info = RB_RATE_LABELS[key] || { label: key, group: key };
      rBody.innerHTML += `<tr>
        <td>${info.label}</td>
        <td style="font-size:12px;color:#64748b">${info.group}</td>
        <td><input type="number" step="0.01" data-rbrate="${key}" data-rbtier="5" value="${rates['5%']}" style="max-width:80px"></td>
        <td><input type="number" step="0.01" data-rbrate="${key}" data-rbtier="10" value="${rates['10%']}" style="max-width:80px"></td>
      </tr>`;
    }

    rbRenderExceptions(s.exception_customers);
    rbRenderRateUpgrades(s.rate_upgrade_customers || []);
    document.getElementById('rbExcludedText').value = (s.excluded_customers || []).join('\n');
    rbRenderEmployees(s.customer_employees || {});

    const erp = s.erp_defaults || {};
    document.getElementById('rbWhCd').value = erp.wh_cd || '10';
    document.getElementById('rbIoType').value = erp.io_type || '1Z';
    document.getElementById('rbProdCd').value = erp.prod_cd || '';
    document.getElementById('rbRemarks').value = erp.remarks_format || '';

    rbLoadMasterCount();
  }

  async function rbSaveAllowed() {
    try {
      const customers = document.getElementById('rbAllowedText').value.split('\n').map(s=>s.trim()).filter(Boolean);
      await rbApi('PUT', '/api/rebate/settings/allowed-customers', { use_allowed_list: document.getElementById('rbUseAllowed').checked, customers, aliases: {} });
      rbToast(`허용 목록 저장 완료 (${customers.length}개 거래처)`, 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  function rbRenderExceptions(exceptions) {
    const container = document.getElementById('rbExceptionsContainer');
    container.innerHTML = '';
    (exceptions || []).forEach((e, i) => {
      container.innerHTML += `<div class="form-row" data-exc-idx="${i}">
        <input type="text" placeholder="거래처명" value="${e.name}" data-field="name" style="flex:2">
        <input type="text" placeholder="거래처코드" value="${e.code}" data-field="code" style="flex:1">
        <select data-field="min_tier" style="flex:0.5">
          <option value="5%" ${e.min_tier==='5%'?'selected':''}>5%</option>
          <option value="10%" ${e.min_tier==='10%'?'selected':''}>10%</option>
        </select>
        <button class="btn btn-danger btn-sm" onclick="rbRemoveException(${i})">삭제</button>
      </div>`;
    });
  }

  window.rbRemoveException = function(idx) {
    rbSettings.exception_customers.splice(idx, 1);
    rbRenderExceptions(rbSettings.exception_customers);
  };

  function rbAddException() {
    if (!rbSettings) rbSettings = { exception_customers: [] };
    rbSettings.exception_customers.push({ name: '', code: '', min_tier: '5%' });
    rbRenderExceptions(rbSettings.exception_customers);
  }

  async function rbSaveExceptions() {
    try {
      const exceptions = [];
      document.querySelectorAll('#rbExceptionsContainer .form-row').forEach(row => {
        exceptions.push({ name: row.querySelector('[data-field="name"]').value, code: row.querySelector('[data-field="code"]').value, min_tier: row.querySelector('[data-field="min_tier"]').value });
      });
      await rbApi('PUT', '/api/rebate/settings/exceptions', exceptions);
      rbSettings.exception_customers = exceptions;
      rbToast('예외 업체 저장 완료', 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  function rbRenderRateUpgrades(entries) {
    const container = document.getElementById('rbRateUpgradeContainer');
    container.innerHTML = '';
    (entries || []).forEach((e, i) => {
      container.innerHTML += `<div class="form-row" data-rup-idx="${i}" style="flex-wrap:wrap;gap:8px">
        <input type="text" placeholder="거래처명" value="${e.name}" data-field="name" style="flex:1.5;min-width:120px">
        <input type="text" placeholder="설명" value="${e.description||''}" data-field="desc" style="flex:2;min-width:150px">
        <div style="display:flex;flex-direction:column;gap:4px;flex:2;min-width:200px">
          ${Object.entries(e.upgrades||{}).map(([from,to],j) => `
            <div class="form-row" style="margin:0;gap:4px" data-upgrade-idx="${j}">
              <input type="number" step="0.01" placeholder="원래율" value="${from}" data-field="from" style="max-width:70px">
              <span style="color:#64748b">→</span>
              <input type="number" step="0.01" placeholder="상향율" value="${to}" data-field="to" style="max-width:70px">
              <button class="btn btn-danger btn-sm" onclick="this.parentElement.remove()" style="padding:2px 6px">×</button>
            </div>
          `).join('')}
          <button class="btn btn-outline btn-sm" onclick="rbAddUpgradeRule(this)" style="align-self:flex-start;font-size:11px;padding:2px 8px">+ 규칙</button>
        </div>
        <button class="btn btn-danger btn-sm" onclick="rbRemoveRateUpgrade(${i})">삭제</button>
      </div>`;
    });
  }

  window.rbAddUpgradeRule = function(btn) {
    const div = document.createElement('div');
    div.className = 'form-row';
    div.style.cssText = 'margin:0;gap:4px';
    div.innerHTML = `<input type="number" step="0.01" placeholder="원래율" data-field="from" style="max-width:70px">
      <span style="color:#64748b">→</span>
      <input type="number" step="0.01" placeholder="상향율" data-field="to" style="max-width:70px">
      <button class="btn btn-danger btn-sm" onclick="this.parentElement.remove()" style="padding:2px 6px">×</button>`;
    btn.parentElement.insertBefore(div, btn);
  };

  window.rbRemoveRateUpgrade = function(idx) {
    if (!rbSettings.rate_upgrade_customers) return;
    rbSettings.rate_upgrade_customers.splice(idx, 1);
    rbRenderRateUpgrades(rbSettings.rate_upgrade_customers);
  };

  function rbAddRateUpgrade() {
    if (!rbSettings) rbSettings = {};
    if (!rbSettings.rate_upgrade_customers) rbSettings.rate_upgrade_customers = [];
    rbSettings.rate_upgrade_customers.push({ name: '', description: '', upgrades: {'0.05': 0.10} });
    rbRenderRateUpgrades(rbSettings.rate_upgrade_customers);
  }

  async function rbSaveRateUpgrade() {
    try {
      const entries = [];
      document.querySelectorAll('#rbRateUpgradeContainer > .form-row').forEach(row => {
        const name = row.querySelector('[data-field="name"]').value.trim();
        const desc = row.querySelector('[data-field="desc"]').value.trim();
        const upgrades = {};
        row.querySelectorAll('[data-upgrade-idx], [data-field="from"]').forEach(el => {
          const container = el.closest('.form-row[style]');
          if (!container) return;
          const fromEl = container.querySelector('[data-field="from"]');
          const toEl = container.querySelector('[data-field="to"]');
          if (fromEl && toEl && fromEl.value && toEl.value) upgrades[fromEl.value] = parseFloat(toEl.value);
        });
        if (name) entries.push({ name, description: desc, upgrades });
      });
      await rbApi('PUT', '/api/rebate/settings/rate-upgrade-customers', entries);
      rbSettings.rate_upgrade_customers = entries;
      rbToast(`할인율 상향 업체 저장 완료 (${entries.length}개)`, 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  async function rbSaveTier() {
    try {
      await rbApi('PUT', '/api/rebate/settings/tier-thresholds', {
        tier_10_min: parseInt(document.getElementById('rbTier10').value),
        tier_5_min: parseInt(document.getElementById('rbTier5').value),
      });
      rbToast('등급 기준 저장 완료', 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  async function rbSaveRates() {
    try {
      const rates = [];
      document.querySelectorAll('#rbRatesBody input[data-rbrate]').forEach(input => {
        const cat = input.dataset.rbrate;
        const tier = input.dataset.rbtier;
        let entry = rates.find(r => r.category === cat);
        if (!entry) { entry = { category: cat, rate_5: 0, rate_10: 0 }; rates.push(entry); }
        if (tier === '5') entry.rate_5 = parseFloat(input.value);
        if (tier === '10') entry.rate_10 = parseFloat(input.value);
      });
      await rbApi('PUT', '/api/rebate/settings/discount-rates', rates);
      rbToast('할인율 저장 완료', 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  async function rbSaveExcluded() {
    try {
      const list = document.getElementById('rbExcludedText').value.split('\n').map(s=>s.trim()).filter(Boolean);
      await rbApi('PUT', '/api/rebate/settings/excluded-customers', list);
      rbToast('제외 거래처 저장 완료', 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  function rbRenderEmployees(map) {
    const container = document.getElementById('rbEmployeesContainer');
    container.innerHTML = '';
    Object.entries(map).forEach(([name, code]) => {
      container.innerHTML += `<div class="form-row">
        <input type="text" placeholder="거래처명" value="${name}" data-field="name" style="flex:2">
        <input type="text" placeholder="사원코드" value="${code}" data-field="code" style="flex:1">
        <button class="btn btn-danger btn-sm" onclick="this.parentElement.remove()">삭제</button>
      </div>`;
    });
  }

  function rbAddEmployee() {
    const container = document.getElementById('rbEmployeesContainer');
    const div = document.createElement('div');
    div.className = 'form-row';
    div.innerHTML = `<input type="text" placeholder="거래처명" data-field="name" style="flex:2">
      <input type="text" placeholder="사원코드" data-field="code" style="flex:1">
      <button class="btn btn-danger btn-sm" onclick="this.parentElement.remove()">삭제</button>`;
    container.appendChild(div);
  }

  async function rbSaveEmployees() {
    try {
      const mappings = [];
      document.querySelectorAll('#rbEmployeesContainer .form-row').forEach(row => {
        const name = row.querySelector('[data-field="name"]').value.trim();
        const code = row.querySelector('[data-field="code"]').value.trim();
        if (name && code) mappings.push({ customer_name: name, emp_cd: code });
      });
      await rbApi('PUT', '/api/rebate/settings/customer-employees', mappings);
      rbToast('담당 사원 저장 완료', 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  async function rbUploadMaster() {
    const file = document.getElementById('rbMasterFile').files[0];
    if (!file) { rbToast('파일을 선택하세요', 'error'); return; }
    try {
      const fd = new FormData();
      fd.append('file', file);
      const resp = await rbApi('POST', '/api/rebate/settings/customer-master/upload', fd, true);
      rbToast(`거래처 마스터 ${resp.count}건 업로드 완료`, 'success');
      rbLoadMasterCount();
    } catch (e) { rbToast(`업로드 실패: ${e.message}`, 'error'); }
  }

  async function rbLoadMasterCount() {
    try {
      const data = await rbApi('GET', '/api/rebate/settings/customer-master');
      document.getElementById('rbMasterCount').textContent = `등록된 거래처: ${data.length}건`;
    } catch (e) { /* ignore */ }
  }

  async function rbSaveErpDefaults() {
    try {
      await rbApi('PUT', '/api/rebate/settings/erp-defaults', {
        wh_cd: document.getElementById('rbWhCd').value,
        io_type: document.getElementById('rbIoType').value,
        prod_cd: document.getElementById('rbProdCd').value,
        prod_des: '리베이트',
        remarks_format: document.getElementById('rbRemarks').value,
      });
      rbToast('ERP 기본값 저장 완료', 'success');
    } catch (e) { rbToast(`오류: ${e.message}`, 'error'); }
  }

  console.log('리베이트 모듈 로드 완료');
})();


// ═══════════════════════════════════════════════════════════
// MAP Monitor - 지도가 감시 시스템
// ═══════════════════════════════════════════════════════════
(function() {
  let _mapSettings = null;
  const _platClass = {'네이버':'naver','쿠팡':'coupang','G마켓':'gmarket','옥션':'auction','11번가':'11st'};

  window.initMapMonitor = async function() { await mapLoadDashboard(); };

  window.switchMapTab = async function(tabId) {
    document.querySelectorAll('.map-tab-btn').forEach(b => b.classList.toggle('active', b.dataset.mapTab === tabId));
    document.querySelectorAll('.map-tab-content').forEach(c => c.classList.toggle('active', c.id === 'map-tab-'+tabId));
    if (tabId === 'dashboard') mapLoadDashboard();
    else if (tabId === 'violations') mapLoadViolations();
    else if (tabId === 'products') mapLoadProducts();
    else if (tabId === 'sellers') mapLoadSellers();
    else if (tabId === 'settings') {
      if (!_mapAdminAuth) {
        const pw = prompt('관리자 비밀번호를 입력하세요:');
        if (!pw) { switchMapTab('dashboard'); return; }
        try {
          const r = await api.post('/api/map/settings/verify', { password: pw });
          if (r.ok) { _mapAdminAuth = pw; mapLoadSettings(); }
          else { alert('비밀번호가 올바르지 않습니다.'); switchMapTab('dashboard'); }
        } catch(e) { alert('비밀번호가 올바르지 않습니다.'); switchMapTab('dashboard'); }
      } else { mapLoadSettings(); }
    }
  };
  let _mapAdminAuth = null;

  function sev(s) { return `<span class="map-sev map-sev-${s}">${s}</span>`; }
  function plat(p) { return `<span class="map-plat map-plat-${_platClass[p]||'naver'}">${p}</span>`; }
  function won(n) { return (n||0).toLocaleString() + '원'; }

  // ── 대시보드 ──
  async function mapLoadDashboard() {
    try {
      const d = await api.get('/api/map/dashboard');
      _mapSettings = d.settings_summary;
      document.getElementById('map-status-text').textContent =
        `감시 중: ${d.monitored_count}개 제품 · 하루 ${(d.settings_summary.schedules||[]).length}회`;
      const vs = d.violation_stats || {};
      document.getElementById('map-stats-cards').innerHTML = `
        <div class="map-stat-card"><div class="label">📦 감시 제품</div><div class="value">${d.monitored_count}</div><div class="sub">${won(d.settings_summary.min_price)} 이상</div></div>
        <div class="map-stat-card"><div class="label">🚨 위반 제품</div><div class="value" style="color:#dc2626">${vs.product_count||0}개</div><div class="sub">위반 건수 ${vs.total||0}건 (7일)</div></div>
        <div class="map-stat-card"><div class="label">🔴 긴급</div><div class="value" style="color:#dc2626">${vs.critical||0}</div><div class="sub">CRITICAL</div></div>`;
      const rv = d.recent_violations || [];
      if (!rv.length) { document.getElementById('map-recent-violations').innerHTML = '<p style="color:#94a3b8;padding:20px;text-align:center">최근 위반 없음</p>'; }
      else {
        let h = '<table class="map-tbl"><thead><tr><th>심각도</th><th>제품</th><th>셀러</th><th>지도가</th><th>판매가</th><th>편차</th><th>탐지</th></tr></thead><tbody>';
        rv.forEach(v => { const link = v.evidence_url ? `<a href="${v.evidence_url}" target="_blank" style="color:#1e293b;text-decoration:none;border-bottom:1px dashed #94a3b8">${v.product_name||''}</a>` : (v.product_name||''); h += `<tr><td>${sev(v.severity)}</td><td><b>${link}</b><br><span style="font-size:11px;color:#94a3b8">${v.model_name||''}</span></td><td>${v.seller_name}</td><td style="color:#64748b">${won(v.map_price)}</td><td style="color:#dc2626;font-weight:700">${won(v.violated_price)}</td><td style="color:#dc2626">-${v.deviation_pct}%</td><td style="font-size:11px;color:#64748b;white-space:nowrap">${(v.detected_at||'').slice(0,16)}</td></tr>`; });
        h += '</tbody></table>';
        document.getElementById('map-recent-violations').innerHTML = h;
      }
      const ts = d.top_sellers || [];
      if (!ts.length) { document.getElementById('map-top-sellers').innerHTML = '<p style="color:#94a3b8">위반 이력 없음</p>'; }
      else {
        let h = '';
        ts.forEach((s,i) => { h += `<div style="display:flex;align-items:center;gap:10px;padding:8px 4px;border-bottom:1px solid #f1f5f9"><span style="width:22px;height:22px;border-radius:50%;background:${i<2?'#fef2f2':'#f8fafc'};display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:${i<2?'#dc2626':'#64748b'}">${i+1}</span><div style="flex:1"><b style="font-size:13px">${s.seller_name}</b> <span style="font-size:11px;color:#94a3b8">${s.platform}</span></div><span style="font-weight:700;color:${s.cnt>=5?'#dc2626':'#f59e0b'};font-size:13px">${s.cnt}건</span></div>`; });
        document.getElementById('map-top-sellers').innerHTML = h;
      }
      const lc = d.last_collection;
      document.getElementById('map-summary').innerHTML = `<div style="display:flex;flex-direction:column;gap:12px">
        <div style="display:flex;justify-content:space-between"><span style="color:#64748b;font-size:13px">전체 제품</span><b>${d.total_products}개</b></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#64748b;font-size:13px">감시 대상</span><b style="color:#2563eb">${d.monitored_count}개</b></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#64748b;font-size:13px">수집 횟수</span><b>하루 ${(d.settings_summary.schedules||[]).length}회</b></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#64748b;font-size:13px">쇼핑몰</span><b>${(d.settings_summary.platforms||[]).length}개</b></div>
        <div style="height:1px;background:#e2e8f0"></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#64748b;font-size:13px">마지막 수집</span><span style="font-size:12px;color:${lc?'#16a34a':'#94a3b8'};font-weight:600">${lc?(lc.finished_at||lc.started_at||'').slice(0,16)+' ✓':'미실행'}</span></div></div>`;
    } catch(e) { console.error('MAP 대시보드:', e); }
  }

  // ── 위반현황 ──
  let _vioSortCol = 'violation_count', _vioSortDir = 'desc';
  let _vioSearchTimer = null;

  window.mapLoadViolations = async function() {
    const sv = document.getElementById('map-vio-severity')?.value || '';
    const dy = document.getElementById('map-vio-days')?.value || '7';
    const q = (document.getElementById('map-vio-search')?.value || '').trim();
    const grouped = document.getElementById('map-vio-grouped')?.checked;

    try {
      if (grouped) {
        const d = await api.get(`/api/map/violations/grouped?severity=${sv}&days=${dy}&search=${encodeURIComponent(q)}`);
        const prods = d.products || [];
        if (!prods.length) { document.getElementById('map-violations-table').innerHTML = '<p style="color:#94a3b8;padding:20px;text-align:center">위반 없음</p>'; return; }
        let h = `<p style="font-size:12px;color:#64748b;margin:0 0 8px">위반 제품 <b>${d.total_products}개</b> · 총 위반 건수 <b style="color:#dc2626">${d.total_violations}건</b> ${q?`· "${q}" 검색`:''}</p>`;
        h += `<table class="map-tbl"><thead><tr>
          <th style="cursor:pointer" onclick="mapVioSort('model_name')">모델명 ${_vioSortIcon('model_name')}</th>
          <th>제품명</th>
          <th style="cursor:pointer" onclick="mapVioSort('violation_count')">위반건수 ${_vioSortIcon('violation_count')}</th>
          <th style="cursor:pointer" onclick="mapVioSort('seller_count')">셀러수 ${_vioSortIcon('seller_count')}</th>
          <th style="cursor:pointer" onclick="mapVioSort('map_price')">지도가 ${_vioSortIcon('map_price')}</th>
          <th style="cursor:pointer" onclick="mapVioSort('min_price')">최저판매가 ${_vioSortIcon('min_price')}</th>
          <th style="cursor:pointer" onclick="mapVioSort('max_deviation')">최대편차 ${_vioSortIcon('max_deviation')}</th>
          <th>위반 셀러</th>
          <th style="cursor:pointer" onclick="mapVioSort('last_detected')">최근탐지 ${_vioSortIcon('last_detected')}</th>
        </tr></thead><tbody>`;
        _sortProds(prods);
        prods.forEach(p => {
          const sellers = (p.sellers||[]).slice(0,3).join(', ') + (p.sellers.length > 3 ? ` 외 ${p.sellers.length-3}개` : '');
          h += `<tr style="cursor:pointer" onclick="mapToggleVioDetail(this,${p.product_id},${parseInt(dy)})" title="클릭하여 상세 위반 보기">
            <td style="font-family:monospace;font-weight:600">${p.model_name} <span style="font-size:10px;color:#94a3b8">▼</span></td>
            <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.product_name}</td>
            <td style="text-align:center"><span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:10px;font-weight:700;font-size:13px">${p.violation_count}건</span></td>
            <td style="text-align:center;font-weight:600">${p.seller_count}개</td>
            <td style="color:#64748b;text-align:right">${won(p.map_price)}</td>
            <td style="color:#dc2626;font-weight:700;text-align:right">${won(p.min_price)}</td>
            <td style="color:#dc2626;text-align:right">-${p.max_deviation}%</td>
            <td style="font-size:12px;color:#475569;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(p.sellers||[]).join(', ')}">${sellers}</td>
            <td style="font-size:11px;color:#64748b;white-space:nowrap">${(p.last_detected||'').slice(0,16)}</td>
          </tr>`;
        });
        h += '</tbody></table>';
        document.getElementById('map-violations-table').innerHTML = h;
      } else {
        // 상세 뷰 (기존)
        const d = await api.get(`/api/map/violations?severity=${sv}&days=${dy}&search=${encodeURIComponent(q)}&limit=200`);
        const vios = d.violations || [];
        if (!vios.length) { document.getElementById('map-violations-table').innerHTML = '<p style="color:#94a3b8;padding:20px;text-align:center">위반 없음</p>'; return; }
        let h = `<p style="font-size:12px;color:#64748b;margin:0 0 8px">총 <b style="color:#dc2626">${d.total}건</b> ${q?`· "${q}" 검색`:''}</p>`;
        h += `<table class="map-tbl"><thead><tr>
          <th style="cursor:pointer" onclick="mapVioSort('severity')">심각도</th>
          <th style="cursor:pointer" onclick="mapVioSort('model_name')">모델</th><th>제품</th>
          <th style="cursor:pointer" onclick="mapVioSort('seller_name')">셀러</th>
          <th style="cursor:pointer" onclick="mapVioSort('map_price')">지도가</th>
          <th style="cursor:pointer" onclick="mapVioSort('violated_price')">판매가</th>
          <th style="cursor:pointer" onclick="mapVioSort('deviation_pct')">편차</th>
          <th style="cursor:pointer" onclick="mapVioSort('detected_at')">탐지</th><th>조치</th>
        </tr></thead><tbody>`;
        vios.forEach(v => {
          const link = v.evidence_url ? `<a href="${v.evidence_url}" target="_blank" style="color:#1e293b;text-decoration:none;border-bottom:1px dashed #94a3b8">${v.product_name||''}</a>` : (v.product_name||'');
          h += `<tr><td>${sev(v.severity)}</td><td style="font-family:monospace;font-size:12px">${v.model_name||''}</td><td><b>${link}</b></td><td>${v.evidence_url?`<a href="${v.evidence_url}" target="_blank" style="color:#334155;text-decoration:none;border-bottom:1px dashed #cbd5e1">${v.seller_name}</a>`:v.seller_name}</td><td style="color:#64748b;text-align:right">${won(v.map_price)}</td><td style="color:#dc2626;font-weight:700;text-align:right">${won(v.violated_price)}</td><td style="color:#dc2626;text-align:right">-${v.deviation_pct}%</td><td style="font-size:11px;color:#64748b;white-space:nowrap">${(v.detected_at||'').slice(0,16)}</td><td style="white-space:nowrap"><button class="btn btn-xs" onclick="mapScreenshot(${v.id})" style="font-size:10px;padding:2px 6px">📸</button> <button class="btn btn-xs" onclick="mapWarningEmail(${v.id})" style="font-size:10px;padding:2px 6px">📧</button> <button class="btn btn-xs" onclick="mapResolveVio(${v.id})" style="font-size:10px;padding:2px 6px">해결</button></td></tr>`;
        });
        h += '</tbody></table>';
        document.getElementById('map-violations-table').innerHTML = h;
      }
    } catch(e) { console.error('위반 로드:', e); }
  };

  function _vioSortIcon(col) { return _vioSortCol === col ? (_vioSortDir === 'asc' ? '▲' : '▼') : ''; }
  function _sortProds(arr) {
    const c = _vioSortCol, d = _vioSortDir === 'asc' ? 1 : -1;
    arr.sort((a,b) => {
      let va = a[c], vb = b[c];
      if (typeof va === 'string') return va.localeCompare(vb) * d;
      return ((va||0) - (vb||0)) * d;
    });
  }
  // 제품 행 클릭 → 개별 위반 드롭다운
  window.mapToggleVioDetail = async function(tr, productId, days) {
    const next = tr.nextElementSibling;
    if (next && next.classList.contains('map-vio-detail')) {
      next.remove(); // 이미 열려있으면 닫기
      tr.querySelector('span:last-child').textContent = '▼';
      return;
    }
    tr.querySelector('span:last-child').textContent = '▲';
    const detailRow = document.createElement('tr');
    detailRow.className = 'map-vio-detail';
    detailRow.innerHTML = `<td colspan="9" style="padding:0"><div style="padding:8px 16px;background:#f8fafc;font-size:12px;color:#64748b">로딩 중...</div></td>`;
    tr.after(detailRow);
    try {
      const vios = await api.get(`/api/map/violations/by-product/${productId}?days=${days}`);
      if (!vios.length) { detailRow.innerHTML = `<td colspan="9" style="padding:8px 16px;background:#f8fafc;font-size:12px;color:#94a3b8">위반 상세 없음</td>`; return; }
      let h = `<td colspan="9" style="padding:0"><table style="width:100%;font-size:12px;background:#f8fafc">
        <tr style="background:#eef2ff"><td style="padding:6px 12px;font-weight:600;color:#4f46e5">심각도</td><td style="padding:6px 12px;font-weight:600;color:#4f46e5">셀러</td><td style="padding:6px 12px;font-weight:600;color:#4f46e5">판매가</td><td style="padding:6px 12px;font-weight:600;color:#4f46e5">편차</td><td style="padding:6px 12px;font-weight:600;color:#4f46e5">탐지</td><td style="padding:6px 12px;font-weight:600;color:#4f46e5">조치</td></tr>`;
      vios.forEach(v => {
        const link = v.evidence_url ? `<a href="${v.evidence_url}" target="_blank" style="color:#334155;text-decoration:none;border-bottom:1px dashed #94a3b8">${v.seller_name}</a>` : v.seller_name;
        h += `<tr style="border-bottom:1px solid #e2e8f0">
          <td style="padding:6px 12px">${sev(v.severity)}</td>
          <td style="padding:6px 12px">${link}</td>
          <td style="padding:6px 12px;color:#dc2626;font-weight:600">${won(v.violated_price)}</td>
          <td style="padding:6px 12px;color:#dc2626">-${v.deviation_pct}%</td>
          <td style="padding:6px 12px;color:#64748b">${(v.detected_at||'').slice(0,16)}</td>
          <td style="padding:6px 12px"><button class="btn btn-xs" onclick="event.stopPropagation();mapResolveVio(${v.id})" style="font-size:10px;padding:2px 6px">해결</button></td>
        </tr>`;
      });
      h += '</table></td>';
      detailRow.innerHTML = h;
    } catch(e) { detailRow.innerHTML = `<td colspan="9" style="padding:8px;color:#dc2626">오류: ${e.message}</td>`; }
  };

  window.mapVioSort = function(col) {
    if (_vioSortCol === col) _vioSortDir = _vioSortDir === 'asc' ? 'desc' : 'asc';
    else { _vioSortCol = col; _vioSortDir = 'desc'; }
    mapLoadViolations();
  };

  // 위반 검색 자동완성
  window.mapVioSearchAuto = function(q) {
    clearTimeout(_vioSearchTimer);
    const dd = document.getElementById('map-vio-dropdown');
    if (!q || q.length < 1) { dd.style.display = 'none'; return; }
    _vioSearchTimer = setTimeout(() => {
      const term = q.toUpperCase();
      let matches = _allMapProducts.filter(p => p.model_name.toUpperCase().includes(term) || p.product_name.toUpperCase().includes(term)).slice(0, 10);
      if (!matches.length) { dd.style.display = 'none'; return; }
      dd.innerHTML = matches.map(p => `<div onclick="document.getElementById('map-vio-search').value='${p.model_name.replace(/'/g,"\\'")}';mapLoadViolations();mapHideVioDropdown()" style="padding:6px 12px;cursor:pointer;font-size:12px;border-bottom:1px solid #f1f5f9" onmouseover="this.style.background='#f0f9ff'" onmouseout="this.style.background=''"><span style="font-family:monospace;font-weight:600;color:#2563eb">${_highlight(p.model_name,term)}</span> <span style="color:#64748b">${_highlight(p.product_name,term)}</span></div>`).join('');
      dd.style.display = 'block';
    }, 200);
  };
  window.mapHideVioDropdown = function() { setTimeout(() => { const dd = document.getElementById('map-vio-dropdown'); if(dd) dd.style.display='none'; }, 200); };

  // 엑셀 다운로드
  window.mapExportViolations = function() {
    const sv = document.getElementById('map-vio-severity')?.value || '';
    const dy = document.getElementById('map-vio-days')?.value || '7';
    const q = (document.getElementById('map-vio-search')?.value || '').trim();
    window.open(`/api/map/violations/export?severity=${sv}&days=${dy}&search=${encodeURIComponent(q)}`, '_blank');
  };
  window.mapResolveVio = async function(id) {
    const note = prompt('해결 내용:'); if (note === null) return;
    try { await api.put(`/api/map/violations/${id}/resolve`, { resolution_note: note }); alert('해결 완료'); mapLoadViolations(); } catch(e) { alert('오류: '+e.message); }
  };
  window.mapScreenshot = async function(id) {
    if (!confirm('이 위반 페이지의 스크린샷을 캡처하시겠습니까? (수 초 소요)')) return;
    try {
      const r = await api.post(`/api/map/screenshot/${id}`);
      // 스크린샷을 새 창에 표시
      const win = window.open('', '_blank', 'width=1300,height=950');
      win.document.write(`<html><head><title>위반 증거 - ${r.model_name}</title></head><body style="margin:0;background:#111">
        <div style="padding:12px;color:#fff;font-size:13px">📸 ${r.filename} | ${r.url}</div>
        <img src="data:image/png;base64,${r.image_base64}" style="width:100%"></body></html>`);
      alert('스크린샷 캡처 완료');
    } catch(e) { alert('스크린샷 오류: '+e.message); }
  };
  window.mapWarningEmail = async function(id) {
    try {
      const r = await api.post(`/api/map/warning-email/generate/${id}`);
      const emailTo = prompt(`경고 메일이 생성되었습니다.\n\n수신: ${r.seller} (${r.platform})\n제목: ${r.subject}\n\n발송할 이메일 주소를 입력하세요 (빈칸이면 저장만):`, '');
      if (emailTo === null) return;
      if (emailTo) {
        const s = await api.put(`/api/map/warning-email/${r.id}/send?email_to=${encodeURIComponent(emailTo)}`);
        alert(s.message);
      } else {
        alert('경고 메일이 초안으로 저장되었습니다. 나중에 발송할 수 있습니다.');
      }
    } catch(e) { alert('경고 메일 오류: '+e.message); }
  };

  // ── 제품·지도가 ──
  let _allMapProducts = []; // 자동완성용 캐시
  let _searchTimer = null;

  window.mapLoadProducts = async function() {
    const q = (document.getElementById('map-prod-search')?.value || '').trim();
    try {
      const products = await api.get(`/api/map/products?search=${encodeURIComponent(q)}&active_only=false`);
      if (!q) _allMapProducts = products; // 전체 목록 캐시
      const mp = (_mapSettings||{}).min_price || 5000;
      if (!products.length) {
        document.getElementById('map-products-table').innerHTML = q
          ? `<p style="color:#94a3b8;padding:20px;text-align:center">'${q}' 검색 결과 없음</p>`
          : '<p style="color:#94a3b8;padding:20px;text-align:center">제품 없음. 엑셀 업로드 또는 제품 추가하세요.</p>';
        return;
      }
      let h = `<div style="display:flex;gap:8px;margin-bottom:8px;align-items:center;flex-wrap:wrap">
        <span style="font-size:12px;color:#64748b">ℹ️ ${q ? `"${q}" ${products.length}건` : `전체 ${products.length}건`} | ${won(mp)} 미만 감시 제외</span>
        <span style="flex:1"></span>
        <span style="font-size:12px;color:#64748b;font-weight:600">일괄:</span>
        <button class="btn btn-xs" onclick="mapBatch('monitor_on')" style="font-size:11px;padding:3px 10px;background:#eff6ff;color:#2563eb;border-color:#bfdbfe">감시 ON</button>
        <button class="btn btn-xs" onclick="mapBatch('monitor_off')" style="font-size:11px;padding:3px 10px;background:#fef2f2;color:#dc2626;border-color:#fecaca">감시 OFF</button>
      </div>
      <table class="map-tbl"><thead><tr>
        <th style="width:30px"><input type="checkbox" id="map-chk-all" onchange="mapToggleAll(this.checked)"></th>
        <th>모델명</th><th>제품명</th><th>지도가</th><th>최저가</th><th>상태</th><th>감시</th><th></th>
      </tr></thead><tbody>`;
      products.forEach(p => {
        const ex = p.map_price < mp;
        const monOff = !p.is_active;
        const st = monOff ? '<span style="background:#f1f5f9;color:#94a3b8;padding:2px 6px;border-radius:4px;font-size:11px">OFF</span>'
          : ex ? '<span style="background:#f1f5f9;color:#94a3b8;padding:2px 6px;border-radius:4px;font-size:11px">금액제외</span>'
          : (p.active_violations > 0 ? '<span style="background:#fef2f2;color:#dc2626;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600">위반</span>'
          : '<span style="background:#f0fdf4;color:#16a34a;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600">정상</span>');
        const dim = monOff || ex ? 0.4 : 1;
        h += `<tr style="opacity:${dim}">
          <td><input type="checkbox" class="map-chk" value="${p.id}"></td>
          <td style="font-family:monospace;font-weight:600">${p.model_name}</td>
          <td>${p.product_name}</td>
          <td><input type="number" value="${p.map_price}" step="100" style="width:90px;padding:3px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;font-weight:600;text-align:right" onchange="mapUpdatePrice(${p.id},this.value)">원</td>
          <td style="font-weight:700;color:${p.active_violations>0?'#dc2626':'#16a34a'}">${p.current_min_price?won(p.current_min_price):'-'}</td>
          <td>${st}</td>
          <td><button class="map-toggle ${p.is_active?'on':'off'}" onclick="mapToggleMonitor(${p.id},${p.is_active?0:1})" style="${p.is_active?'background:#2563eb':'background:#d1d5db'}"><div class="knob" style="left:${p.is_active?'20px':'2px'}"></div></button></td>
          <td><button class="btn btn-xs" onclick="mapDeleteProduct(${p.id})" style="font-size:11px;padding:2px 6px;color:#dc2626">삭제</button></td></tr>`;
      });
      h += '</tbody></table>';
      document.getElementById('map-products-table').innerHTML = h;
    } catch(e) { console.error('제품 로드:', e); }
  };

  // 드롭다운 자동완성
  window.mapSearchAutocomplete = function(q) {
    clearTimeout(_searchTimer);
    const dd = document.getElementById('map-search-dropdown');
    if (!q || q.length < 1) { dd.style.display = 'none'; return; }
    _searchTimer = setTimeout(() => {
      const term = q.toUpperCase();
      // 캐시된 목록에서 로컬 필터 (빠름)
      let matches = _allMapProducts.filter(p =>
        p.model_name.toUpperCase().includes(term) || p.product_name.toUpperCase().includes(term)
      ).slice(0, 15);
      if (!matches.length) {
        // 캐시가 비어있거나 결과 없으면 API 호출
        api.get(`/api/map/products?search=${encodeURIComponent(q)}`).then(results => {
          _renderDropdown(results.slice(0, 15), q);
        });
        return;
      }
      _renderDropdown(matches, q);
    }, 200);
  };

  function _renderDropdown(items, q) {
    const dd = document.getElementById('map-search-dropdown');
    if (!items.length) { dd.style.display = 'none'; return; }
    const term = q.toUpperCase();
    let h = '';
    items.forEach(p => {
      const mn = _highlight(p.model_name, term);
      const pn = _highlight(p.product_name, term);
      h += `<div onclick="mapSelectSearch('${p.model_name.replace(/'/g,"\\'")}')" style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f1f5f9;font-size:13px;display:flex;gap:8px;align-items:center" onmouseover="this.style.background='#f0f9ff'" onmouseout="this.style.background=''">
        <span style="font-family:monospace;font-weight:600;color:#2563eb;min-width:120px">${mn}</span>
        <span style="color:#334155;flex:1">${pn}</span>
        <span style="color:#94a3b8;font-size:11px">${won(p.map_price)}</span>
      </div>`;
    });
    dd.innerHTML = h;
    dd.style.display = 'block';
  }

  function _highlight(text, term) {
    if (!term) return text;
    const idx = text.toUpperCase().indexOf(term);
    if (idx < 0) return text;
    return text.slice(0, idx) + '<b style="color:#2563eb;background:#eff6ff">' + text.slice(idx, idx + term.length) + '</b>' + text.slice(idx + term.length);
  }

  window.mapSelectSearch = function(modelName) {
    document.getElementById('map-prod-search').value = modelName;
    document.getElementById('map-search-dropdown').style.display = 'none';
    mapLoadProducts();
  };

  window.mapHideDropdown = function() {
    setTimeout(() => { document.getElementById('map-search-dropdown').style.display = 'none'; }, 200);
  };

  // 드롭다운 외부 클릭 시 닫기
  document.addEventListener('click', function(e) {
    if (!e.target.closest('#map-prod-search') && !e.target.closest('#map-search-dropdown')) {
      const dd = document.getElementById('map-search-dropdown');
      if (dd) dd.style.display = 'none';
    }
  });
  window.mapUpdatePrice = async function(id, v) { try { await api.put(`/api/map/products/${id}/map-price?map_price=${v}`); } catch(e) { alert('오류: '+e.message); } };
  window.mapToggleWatch = async function(id, on) { try { await api.put(`/api/map/products/${id}/watch?watched=${!!on}`); mapLoadProducts(); } catch(e) { alert('오류: '+e.message); } };
  window.mapToggleMonitor = async function(id, on) {
    try { await api.put(`/api/map/products/${id}`, { is_active: !!on }); mapLoadProducts(); } catch(e) { alert('오류: '+e.message); }
  };
  window.mapDeleteProduct = async function(id) { if (!confirm('삭제하시겠습니까?')) return; try { await api.delete(`/api/map/products/${id}`); mapLoadProducts(); } catch(e) { alert('오류: '+e.message); } };
  window.mapToggleAll = function(checked) {
    document.querySelectorAll('.map-chk').forEach(c => c.checked = checked);
  };
  window.mapBatch = async function(action) {
    const ids = [...document.querySelectorAll('.map-chk:checked')].map(c => parseInt(c.value));
    if (!ids.length) { alert('제품을 선택해주세요 (체크박스)'); return; }
    const labels = {monitor_on:'감시 ON',monitor_off:'감시 OFF',watch_on:'상시감시 ON',watch_off:'상시감시 OFF'};
    if (!confirm(`선택한 ${ids.length}개 제품을 '${labels[action]}'(으)로 변경하시겠습니까?`)) return;
    const body = { product_ids: ids };
    if (action === 'monitor_on') body.is_active = true;
    else if (action === 'monitor_off') body.is_active = false;
    else if (action === 'watch_on') body.is_watched = true;
    else if (action === 'watch_off') body.is_watched = false;
    try {
      const r = await api.put('/api/map/products/bulk', body);
      alert(r.message);
      mapLoadProducts();
    } catch(e) { alert('오류: '+e.message); }
  };
  window.mapShowAddProduct = function() {
    const mn = prompt('모델명:'); if (!mn) return;
    const pn = prompt('제품명:'); if (!pn) return;
    const mp = prompt('지도가 (원):', '0'); if (!mp) return;
    const br = prompt('브랜드:', 'LANstar') || 'LANstar';
    api.post('/api/map/products', {model_name:mn, product_name:pn, map_price:parseInt(mp), brand:br})
      .then(() => { alert('등록 완료'); mapLoadProducts(); }).catch(e => alert('오류: '+e.message));
  };
  window.mapUploadExcel = async function() {
    const file = document.getElementById('map-excel-file').files[0]; if (!file) return;
    const fd = new FormData(); fd.append('file', file);
    try { const r = await api.postForm('/api/map/products/upload', fd); alert(r.message); mapLoadProducts(); } catch(e) { alert('업로드 오류: '+e.message); }
    document.getElementById('map-excel-file').value = '';
  };

  // ── 상시감시 ──
  async function mapLoadWatch() {
    try {
      const products = await api.get('/api/map/products?watched_only=true');
      if (!products.length) { document.getElementById('map-watch-table').innerHTML = '<p style="color:#94a3b8;padding:20px;text-align:center">상시 감시 제품 없음. \'제품·지도가\' 탭에서 켜세요.</p>'; return; }
      let h = '<table class="map-tbl"><thead><tr><th>모델명</th><th>제품명</th><th>지도가</th><th>최저가</th><th>상태</th><th>해제</th></tr></thead><tbody>';
      products.forEach(p => { h += `<tr><td style="font-family:monospace;font-weight:600;color:#7c3aed">${p.model_name}</td><td>${p.product_name}</td><td style="color:#64748b">${won(p.map_price)}</td><td style="font-weight:700;color:${p.active_violations>0?'#dc2626':'#16a34a'}">${p.current_min_price?won(p.current_min_price):'-'}</td><td>${p.active_violations>0?'<span style="background:#fef2f2;color:#dc2626;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600">위반</span>':'<span style="background:#f0fdf4;color:#16a34a;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600">정상</span>'}</td><td><button class="btn btn-xs" onclick="mapToggleWatch(${p.id},0)" style="font-size:11px;padding:2px 8px;color:#dc2626;border-color:#fecaca;background:#fef2f2">해제</button></td></tr>`; });
      h += '</tbody></table>';
      document.getElementById('map-watch-table').innerHTML = h;
    } catch(e) { console.error('상시감시:', e); }
  }

  // ── 셀러관리 ──
  async function mapLoadSellers() {
    try {
      const sellers = await api.get('/api/map/sellers');
      if (!sellers.length) { document.getElementById('map-sellers-table').innerHTML = '<p style="color:#94a3b8;padding:20px;text-align:center">셀러 없음. 수집 후 자동 등록됩니다.</p>'; return; }
      let h = '<table class="map-tbl"><thead><tr><th>위험도</th><th>셀러명</th><th>플랫폼</th><th>총 위반</th><th>최근 30일</th><th>마지막 위반</th></tr></thead><tbody>';
      sellers.forEach(s => { const rc = s.risk_level==='high'?'#dc2626':s.risk_level==='medium'?'#f59e0b':'#16a34a'; const rl = s.risk_level==='high'?'높음':s.risk_level==='medium'?'보통':'낮음'; h += `<tr><td><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${rc};margin-right:6px"></span><span style="font-size:12px;font-weight:600;color:${rc}">${rl}</span></td><td style="font-weight:600">${s.seller_name}</td><td>${plat(s.platform)}</td><td style="font-weight:700;color:${s.total_violations>5?'#dc2626':'#334155'}">${s.total_violations}건</td><td>${s.recent_violations||0}건</td><td style="font-size:12px;color:#64748b">${s.last_violation_at?(s.last_violation_at).slice(0,10):'-'}</td></tr>`; });
      h += '</tbody></table>';
      document.getElementById('map-sellers-table').innerHTML = h;
    } catch(e) { console.error('셀러:', e); }
  }

  // ── 설정 ──
  let _scheduleSet, _platformSet;
  async function mapLoadSettings() {
    try {
      const s = await api.get('/api/map/settings');
      _mapSettings = s;
      const schedules = s.schedules || [];
      const platforms = s.platforms || [];
      _scheduleSet = new Set(schedules);
      _platformSet = new Set(platforms);
      const allTimes = ['00:00','03:00','06:00','09:00','12:00','15:00','18:00','21:00'];
      const allPlats = [{n:'네이버 쇼핑',c:'#03c75a',m:'API'},{n:'쿠팡',c:'#e44332',m:'크롤링'},{n:'G마켓',c:'#6db33f',m:'크롤링'},{n:'옥션',c:'#ff6f00',m:'크롤링'},{n:'11번가',c:'#ff5a2e',m:'크롤링'}];
      let h = `<div class="map-settings-section"><div class="hdr">⚙️ 기본 감시 설정</div><div class="body">
        <div style="margin-bottom:20px"><label style="font-size:13px;font-weight:600;display:block;margin-bottom:6px">최소 감시 금액</label><input type="number" id="map-set-minprice" value="${s.min_price||5000}" step="1000" style="width:150px;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:15px;font-weight:600"> <span style="font-size:13px;color:#64748b">원 이상</span></div>
        <div style="margin-bottom:20px"><label style="font-size:13px;font-weight:600;display:block;margin-bottom:6px">허용 편차 (%)</label><input type="number" id="map-set-tolerance" value="${s.tolerance_pct||5}" min="0" max="30" style="width:80px;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:15px;font-weight:600"> <span style="font-size:13px;color:#64748b">%</span></div>
        <div style="margin-bottom:20px"><label style="font-size:13px;font-weight:600;display:block;margin-bottom:6px">정기 감시 스케줄</label><div style="display:flex;flex-wrap:wrap;gap:8px">`;
      allTimes.forEach(t => { const on = _scheduleSet.has(t); h += `<button class="btn btn-sm ${on?'':'btn-outline'}" onclick="mapToggleSched('${t}')" id="map-sched-${t.replace(':','')}" style="${on?'background:#2563eb;color:#fff;border-color:#2563eb':''}">${t}</button>`; });
      h += `</div><p style="font-size:11px;color:#94a3b8;margin-top:6px">하루 ${schedules.length}회</p></div>
        <div><label style="font-size:13px;font-weight:600;display:block;margin-bottom:6px">상시감시 간격</label><select id="map-set-watchint" style="padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;font-weight:600">`;
      [1,2,3,4].forEach(h2 => { h += `<option value="${h2}" ${s.watch_interval_hours==h2?'selected':''}>${h2}시간</option>`; });
      h += `</select></div></div></div>`;
      h += `<div class="map-settings-section"><div class="hdr">🏪 감시 대상 쇼핑몰</div><div class="body"><div style="display:flex;flex-wrap:wrap;gap:10px">`;
      allPlats.forEach(p => { const on = _platformSet.has(p.n); h += `<button onclick="mapTogglePlat('${p.n}')" id="map-plat-${p.n.replace(/\s/g,'')}" style="display:flex;align-items:center;gap:8px;padding:10px 18px;border-radius:8px;cursor:pointer;border:${on?'2px solid '+p.c:'1px solid #e2e8f0'};background:${on?p.c+'15':'#fff'}"><span style="width:8px;height:8px;border-radius:50%;background:${on?p.c:'#d1d5db'}"></span><span style="font-weight:600;color:${on?'#1e293b':'#94a3b8'};font-size:13px">${p.n}</span><span style="font-size:10px;color:#94a3b8;background:#f1f5f9;padding:1px 5px;border-radius:3px">${p.m}</span></button>`; });
      h += `</div></div></div>`;
      h += `<div style="text-align:right;margin-top:8px"><button class="btn" onclick="mapSaveSettings()" style="padding:10px 32px">💾 설정 저장</button></div>`;
      document.getElementById('map-settings-panel').innerHTML = h;
    } catch(e) { console.error('설정 로드:', e); }
  }
  window.mapToggleSched = function(t) {
    if (_scheduleSet.has(t)) _scheduleSet.delete(t); else _scheduleSet.add(t);
    const btn = document.getElementById('map-sched-'+t.replace(':',''));
    if (btn) { if (_scheduleSet.has(t)) { btn.style.background='#2563eb'; btn.style.color='#fff'; btn.style.borderColor='#2563eb'; btn.classList.remove('btn-outline'); } else { btn.style.background=''; btn.style.color=''; btn.style.borderColor=''; btn.classList.add('btn-outline'); } }
  };
  window.mapTogglePlat = function(p) {
    if (_platformSet.has(p)) _platformSet.delete(p); else _platformSet.add(p);
    const colors = {'네이버 쇼핑':'#03c75a','쿠팡':'#e44332','G마켓':'#6db33f','옥션':'#ff6f00','11번가':'#ff5a2e'};
    const c = colors[p] || '#6b7280';
    const on = _platformSet.has(p);
    const btn = document.getElementById('map-plat-'+p.replace(/\s/g,''));
    if (btn) {
      btn.style.border = on ? '2px solid '+c : '1px solid #e2e8f0';
      btn.style.background = on ? c+'15' : '#fff';
      btn.querySelector('span').style.background = on ? c : '#d1d5db';
      btn.querySelectorAll('span')[1].style.color = on ? '#1e293b' : '#94a3b8';
    }
  };
  window.mapSaveSettings = async function() {
    if (!_mapAdminAuth) { alert('관리자 인증이 필요합니다.'); return; }
    const data = { min_price: parseInt(document.getElementById('map-set-minprice').value), tolerance_pct: parseFloat(document.getElementById('map-set-tolerance').value), watch_interval_hours: parseInt(document.getElementById('map-set-watchint').value), admin_password: _mapAdminAuth };
    if (_scheduleSet) data.schedules = [..._scheduleSet].sort();
    if (_platformSet) data.platforms = [..._platformSet];
    try { const r = await api.put('/api/map/settings', data); _mapSettings = r; alert('설정 저장 완료'); mapLoadSettings(); } catch(e) { alert('오류: '+e.message); }
  };

  // ── 즉시 수집 (백그라운드 + 진행률 폴링) ──
  let _pollTimer = null;
  window.mapRunCollect = async function() {
    const btn = document.getElementById('map-collect-btn');
    btn.disabled = true; btn.textContent = '⏳ 시작 중...';
    try {
      const r = await api.post('/api/map/collect/run');
      if (r.status === 'running') btn.textContent = '⏳ 이미 수집 중...';
      _startProgressPoll();
    } catch(e) { alert('수집 시작 오류: '+e.message); btn.disabled = false; btn.textContent = '▶ 즉시 수집'; }
  };
  function _startProgressPoll() {
    const btn = document.getElementById('map-collect-btn');
    const st = document.getElementById('map-status-text');
    if (_pollTimer) clearInterval(_pollTimer);
    _pollTimer = setInterval(async () => {
      try {
        const p = await api.get('/api/map/collect/progress');
        btn.textContent = `⏳ ${p.percent||0}% 수집 중...`;
        st.textContent = p.running
          ? `수집 중 ${p.percent}% | ${p.current_platform} → ${p.current_product} | 가격 ${p.prices_collected}건 · 위반 ${p.violations_found}건 · 오류 ${p.errors_count}건`
          : (p.message || '수집 완료');
        if (!p.running) {
          clearInterval(_pollTimer); _pollTimer = null;
          btn.disabled = false; btn.textContent = '▶ 즉시 수집';
          if (p.percent >= 100 && p.message) alert(p.message);
          mapLoadDashboard();
        }
      } catch(e) { console.error('폴링 오류:', e); }
    }, 2000);
  }

  console.log('MAP Monitor 모듈 로드 완료');
})();

// ═══════════════════════════════════════════════════════
//  메일 자동화 (Mail Automation) 모듈
// ═══════════════════════════════════════════════════════
(function(){
  let _mailAuth = false;

  window.mailSaveTemplate = function() {
    const body = document.getElementById('mail-template-body').value;
    localStorage.setItem('mail_auto_template', body);
    alert('템플릿이 저장되었습니다.');
  };

  function mailLoadTemplate() {
    const saved = localStorage.getItem('mail_auto_template');
    if (saved) {
      const el = document.getElementById('mail-template-body');
      if (el) el.value = saved;
    }
  }

  // ─── 자동 실행 제어 ───────────────────
  let _autoStatusTimer = null;

  window.mailToggleAuto = async function(enabled) {
    const interval = parseInt(document.getElementById('mail-auto-interval').value) || 10;
    try {
      if (enabled) {
        const autoReply = document.getElementById('mail-auto-reply-check')?.checked || false;
        const replyTemplate = document.getElementById('mail-template-body')?.value || '';
        await fetch('/api/mail-auto/scheduler/start', {
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({interval_min: interval, auto_reply: autoReply, reply_template: replyTemplate})
        });
        _startAutoStatusPoll();
      } else {
        await fetch('/api/mail-auto/scheduler/stop', {method:'POST'});
        _stopAutoStatusPoll();
      }
      _updateAutoUI(enabled);
    } catch(e) { alert('스케줄러 제어 실패: '+e.message); }
  };

  function _updateAutoUI(enabled) {
    const badge = document.getElementById('mail-auto-status-badge');
    const dot = document.getElementById('mail-auto-toggle-dot');
    const toggle = document.getElementById('mail-auto-toggle');
    if (badge) {
      badge.textContent = enabled ? 'ON' : 'OFF';
      badge.style.background = enabled ? '#dcfce7' : '#fee2e2';
      badge.style.color = enabled ? '#16a34a' : '#dc2626';
    }
    if (dot) dot.style.transform = enabled ? 'translateX(22px)' : 'translateX(0)';
    if (toggle) {
      toggle.checked = enabled;
      toggle.parentElement.querySelector('span:first-of-type').style.background = enabled ? '#22c55e' : '#cbd5e1';
    }
  }

  async function _pollAutoStatus() {
    try {
      const r = await fetch('/api/mail-auto/scheduler/status');
      const s = await r.json();
      const lastCheck = document.getElementById('mail-auto-last-check');
      const lastResult = document.getElementById('mail-auto-last-result');
      if (lastCheck && s.last_check) {
        lastCheck.textContent = '마지막 확인: ' + s.last_check.substring(11,19);
      }
      if (lastResult && s.last_result) {
        const lr = s.last_result;
        if (lr.error) {
          lastResult.innerHTML = '<span style="color:#dc2626">⚠️ 오류: '+lr.error+'</span>';
        } else if (lr.new_processed > 0) {
          lastResult.innerHTML = '<span style="color:#16a34a">✅ '+lr.new_processed+'건 처리 완료 ('+lr.timestamp?.substring(11,19)+')</span>';
          mailLoadDashboard();
          mailLoadLogs();
          mailLoadPending();
        } else if (lr.new_found > 0) {
          lastResult.innerHTML = '<span style="color:#d97706">📬 '+lr.new_found+'건 신규 발견 — 승인 대기</span>';
          mailLoadPending();
        } else {
          lastResult.textContent = '신규 메일 없음';
        }
      }
      _updateAutoUI(s.enabled);
    } catch(e) {}
  }

  function _startAutoStatusPoll() {
    if (_autoStatusTimer) clearInterval(_autoStatusTimer);
    _autoStatusTimer = setInterval(_pollAutoStatus, 10000); // 10초마다 상태 확인
  }

  function _stopAutoStatusPoll() {
    if (_autoStatusTimer) { clearInterval(_autoStatusTimer); _autoStatusTimer = null; }
  }

  // ─── 승인 대기 관리 ───────────────────────
  async function mailLoadPending() {
    try {
      const r = await fetch('/api/mail-auto/pending');
      const d = await r.json();
      const section = document.getElementById('mail-pending-section');
      const list = document.getElementById('mail-pending-list');
      const count = document.getElementById('mail-pending-count');
      
      if (d.pending && d.pending.length > 0) {
        section.style.display = 'block';
        count.textContent = d.pending.length + '건';
        let html = '<table class="table table-sm" style="font-size:13px"><thead><tr><th><input type="checkbox" id="pending-check-all" onchange="mailTogglePendingAll(this.checked)" checked></th><th>수신일</th><th>제목</th><th>첨부</th></tr></thead><tbody>';
        d.pending.forEach(p => {
          html += `<tr>
            <td><input type="checkbox" class="pending-check" value="${p.message_id}" checked></td>
            <td style="font-size:12px;white-space:nowrap">${(p.received_at||'').substring(0,16)}</td>
            <td>${p.subject||''}</td>
            <td style="text-align:center">${p.attachment_count}개</td>
          </tr>`;
        });
        html += '</tbody></table>';
        list.innerHTML = html;
      } else {
        section.style.display = 'none';
      }
    } catch(e) {}
  }

  window.mailTogglePendingAll = function(checked) {
    document.querySelectorAll('.pending-check').forEach(cb => cb.checked = checked);
  };

  window.mailApproveAll = async function() {
    const checks = document.querySelectorAll('.pending-check:checked');
    if (!checks.length) return alert('승인할 메일을 선택하세요');
    
    const ids = Array.from(checks).map(cb => cb.value);
    const autoReply = document.getElementById('mail-auto-reply-check')?.checked || false;
    const template = document.getElementById('mail-template-body')?.value || '';
    
    if (!confirm(`${ids.length}건 승인 → HS코드 입력 + ERP 전표 전송을 시작합니다.\n계속하시겠습니까?`)) return;
    
    const btn = event.target;
    btn.disabled = true; btn.textContent = '⏳ 처리 중...';
    
    try {
      const r = await fetch('/api/mail-auto/approve', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({message_ids: ids, auto_reply: autoReply, reply_template: template})
      });
      const d = await r.json();
      alert(`✅ ${d.new_processed||0}건 처리 완료!`);
      mailLoadPending();
      mailLoadDashboard();
      mailLoadLogs();
    } catch(e) { alert('처리 실패: '+e.message); }
    btn.disabled = false; btn.textContent = '✅ 전체 승인 → 자동 처리';
  };

  window.mailRejectAll = async function() {
    const checks = document.querySelectorAll('.pending-check:checked');
    if (!checks.length) return;
    if (!confirm(`${checks.length}건을 스킵합니다.`)) return;
    
    const ids = Array.from(checks).map(cb => cb.value);
    await fetch('/api/mail-auto/reject', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({message_ids: ids})
    });
    mailLoadPending();
  };

  // ─── 품목코드 매핑 관리 ───────────────────
  window.mailAddMapping = async function() {
    const model = document.getElementById('mapping-model').value.trim();
    const prodcd = document.getElementById('mapping-prodcd').value.trim();
    if (!model || !prodcd) return alert('모델명과 품목코드를 입력하세요');
    try {
      const r = await fetch('/api/mail-auto/product-mapping/add', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({model_name: model, prod_cd: prodcd})
      });
      const d = await r.json();
      if (d.success) {
        alert(`✅ 매핑 추가: ${model} → ${prodcd}`);
        document.getElementById('mapping-model').value = '';
        document.getElementById('mapping-prodcd').value = '';
        mailLoadMappingCount();
      } else { alert('추가 실패: ' + (d.error || '')); }
    } catch(e) { alert('오류: '+e.message); }
  };

  window.mailSearchMapping = async function() {
    const q = document.getElementById('mapping-search').value.trim();
    if (!q) return;
    const r = await fetch('/api/mail-auto/product-mapping/search?q=' + encodeURIComponent(q));
    const d = await r.json();
    const div = document.getElementById('mapping-search-result');
    if (d.results.length === 0) {
      div.innerHTML = '<span style="color:#999">검색 결과 없음</span>';
      return;
    }
    let html = '<table class="table table-sm" style="font-size:12px"><thead><tr><th>모델명</th><th>품목코드</th></tr></thead><tbody>';
    d.results.forEach(r => { html += `<tr><td>${r.model_name}</td><td><b>${r.prod_cd}</b></td></tr>`; });
    html += '</tbody></table>';
    div.innerHTML = html;
  };

  window.mailUploadMapping = async function(input) {
    if (!input.files.length) return;
    if (!confirm('기존 매핑을 새 파일로 교체합니다. 계속하시겠습니까?')) { input.value=''; return; }
    const fd = new FormData();
    fd.append('file', input.files[0]);
    const r = await fetch('/api/mail-auto/product-mapping/upload', {method:'POST', body: fd});
    const d = await r.json();
    alert(d.success ? `✅ ${d.loaded}건 매핑 업로드 완료` : '❌ 업로드 실패: ' + (d.error||''));
    input.value = '';
    mailLoadMappingCount();
  };

  async function mailLoadMappingCount() {
    try {
      const r = await fetch('/api/mail-auto/product-mapping/count');
      const d = await r.json();
      const el = document.getElementById('mail-mapping-count');
      if (el) el.textContent = `(${d.count?.toLocaleString()}건)`;
    } catch(e) {}
  }

  window.initMailAutoPage = async function() {
    if (!_mailAuth) {
      document.getElementById('mail-auto-content').style.display='none';
      document.getElementById('mail-auto-login').style.display='block';
      return;
    }
    document.getElementById('mail-auto-login').style.display='none';
    document.getElementById('mail-auto-content').style.display='block';
    mailLoadTemplate();
    await mailLoadDashboard();
    await mailLoadRate();
    await mailLoadLogs();
    mailLoadMappingCount();
    mailLoadPending();
    // 자동 실행 상태 복원
    try {
      const r = await fetch('/api/mail-auto/scheduler/status');
      const s = await r.json();
      _updateAutoUI(s.enabled);
      if (s.enabled) _startAutoStatusPoll();
    } catch(e) {}
  };

  window.mailAutoLogin = async function() {
    const pw = document.getElementById('mail-auto-pw').value;
    try {
      const r = await fetch('/api/mail-auto/auth', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({password: pw})
      });
      if (r.ok) {
        _mailAuth = true;
        initMailAutoPage();
      } else {
        alert('비밀번호가 올바르지 않습니다');
      }
    } catch(e) { alert('인증 오류: '+e.message); }
  };

  async function mailLoadDashboard() {
    try {
      const r = await fetch('/api/mail-auto/dashboard');
      const d = await r.json();
      const s = d.stats || {};
      document.getElementById('mail-stat-today').textContent = s.today_count || 0;
      document.getElementById('mail-stat-week').textContent = s.week_count || 0;
      document.getElementById('mail-stat-total').textContent = s.total_count || 0;
      document.getElementById('mail-stat-hs').textContent = s.total_hs || 0;
      document.getElementById('mail-stat-oem').textContent = s.pending_oem || 0;
    } catch(e) { console.error('대시보드 로드 실패:', e); }
  }

  async function mailLoadRate() {
    try {
      const r = await fetch('/api/mail-auto/exchange-rate');
      const d = await r.json();
      const rate = d.rate ? Math.round(d.rate * 100) / 100 : null;
      const baseRate = d.base_rate ? Math.round(d.base_rate * 100) / 100 : null;
      const spread = d.spread || 1.75;
      document.getElementById('mail-rate-value').textContent = rate ? rate.toLocaleString() : '-';
      document.getElementById('mail-rate-input').value = rate || '';
      document.getElementById('mail-rate-updated').textContent = d.updated ? d.updated.substring(0,16) : '';
      const detDiv = document.getElementById('mail-rate-detail');
      if (detDiv && baseRate) {
        detDiv.innerHTML = `<span style="font-size:11px;color:#666">매매기준율 ${baseRate.toLocaleString()}원 + 스프레드 ${spread}% = <b>매도율 ${rate?.toLocaleString()}원</b></span>`;
      }
    } catch(e) { console.error('환율 로드 실패:', e); }
  }

  window.mailSetRate = async function() {
    const v = document.getElementById('mail-rate-input').value;
    if (!v || parseFloat(v) <= 0) return alert('유효한 환율을 입력하세요');
    try {
      await fetch('/api/mail-auto/exchange-rate', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({rate: parseFloat(v)})
      });
      await mailLoadRate();
    } catch(e) { alert('환율 설정 실패'); }
  };

  window.mailRefreshRate = async function() {
    try {
      // force refresh by clearing cache first
      const r = await fetch('/api/mail-auto/exchange-rate');
      const d = await r.json();
      document.getElementById('mail-rate-value').textContent = d.rate ? Math.round(d.rate).toLocaleString() : '-';
      document.getElementById('mail-rate-input').value = d.rate ? Math.round(d.rate) : '';
    } catch(e) { alert('환율 조회 실패'); }
  };

  async function mailLoadLogs() {
    try {
      const r = await fetch('/api/mail-auto/logs?limit=20');
      const d = await r.json();
      const tbody = document.getElementById('mail-logs-tbody');
      if (!d.logs || d.logs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#999">처리 이력이 없습니다</td></tr>';
        return;
      }
      tbody.innerHTML = d.logs.map(l => {
        const ds = l.result || {};
        const hsInfo = ds.hs_filled !== undefined ? `${ds.hs_filled}/${ds.total_items||0}` : (l.hs_code_count||0);
        const erpInfo = ds.erp_success ? '✅' : (ds.erp_lines_sent > 0 ? '❌' : '—');
        return `
        <tr onclick="mailShowDetail(${l.id})" style="cursor:pointer">
          <td style="font-size:12px">${(l.processed_at||'').substring(0,16)}</td>
          <td title="${l.subject}" style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${l.subject||''}</td>
          <td style="text-align:center">${l.attachment_count||0}</td>
          <td style="text-align:center"><b>${hsInfo}</b></td>
          <td style="text-align:center">${erpInfo}</td>
          <td style="text-align:center">${l.reply_sent ? '✅' : '—'}</td>
          <td><span class="badge ${l.status==='completed'?'badge-success':'badge-warn'}">${l.status}</span></td>
        </tr>
      `}).join('');
    } catch(e) { console.error('로그 로드 실패:', e); }
  }

  window.mailPreviewEmails = async function() {
    const btn = document.getElementById('mail-preview-btn');
    btn.disabled = true; btn.textContent = '검색 중...';
    try {
      const r = await fetch('/api/mail-auto/preview-emails?days_back=30');
      const d = await r.json();
      const div = document.getElementById('mail-preview-result');
      if (!d.emails || d.emails.length === 0) {
        div.innerHTML = '<p style="color:#999">새 메일이 없습니다</p>';
      } else {
        div.innerHTML = '<table class="table table-sm"><thead><tr><th>날짜</th><th>제목</th><th>첨부</th><th>상태</th></tr></thead><tbody>' +
          d.emails.map(e => `<tr style="${e.already_processed?'opacity:0.5':''}">
            <td style="font-size:12px">${(e.date||'').substring(0,16)}</td>
            <td>${e.subject}</td>
            <td>${e.attachments.map(a=>a.filename).join(', ')}</td>
            <td>${e.already_processed?'<span class="badge badge-muted">처리됨</span>':'<span class="badge badge-info">신규</span>'}</td>
          </tr>`).join('') + '</tbody></table>';
      }
    } catch(e) { div.innerHTML = '<p style="color:red">오류: '+e.message+'</p>'; }
    btn.disabled = false; btn.textContent = '📬 메일 검색';
  };

  window.mailRunPipeline = async function() {
    if (!confirm('메일 자동화 파이프라인을 실행합니다.\nHS코드 입력 + ERP 구매전표 생성이 자동 실행됩니다.\n계속하시겠습니까?')) return;
    const btn = document.getElementById('mail-run-btn');
    btn.disabled = true; btn.textContent = '⏳ 처리 중...';
    const resultDiv = document.getElementById('mail-run-result');
    resultDiv.innerHTML = '<p>처리 중입니다... IMAP 접속 → Excel 파싱 → HS코드 입력 → ERP 전표 생성</p>';
    try {
      const autoReply = document.getElementById('mail-auto-reply-check')?.checked || false;
      const rate = document.getElementById('mail-rate-input').value;
      const replyBody = document.getElementById('mail-template-body')?.value || '';
      const r = await fetch('/api/mail-auto/trigger', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          days_back: 30, auto_reply: autoReply, auto_erp: true,
          exchange_rate: rate ? parseFloat(rate) : null,
          reply_template: replyBody
        })
      });
      const d = await r.json();
      let html = `<div class="card" style="margin-top:12px;padding:16px">
        <h4>✅ 처리 완료</h4>
        <p>총 메일: <b>${d.total_emails}</b>건 | 신규 처리: <b>${d.new_processed}</b>건 | 기처리: ${d.already_processed}건 | 환율: ${d.exchange_rate?.toLocaleString()}원</p>`;
      if (d.results && d.results.length > 0) {
        html += '<table class="table table-sm"><thead><tr><th>제목</th><th>첨부</th><th>HS입력</th><th>ERP</th><th>OEM</th></tr></thead><tbody>';
        d.results.forEach(r => {
          const hsCount = r.attachments_processed?.reduce((s,a) => s + (a.stats?.hs_filled||0), 0) || 0;
          const erpOk = r.erp_result?.success ? '✅' : (r.erp_result ? '❌' : '—');
          const oemCount = r.oem_items?.length || 0;
          html += `<tr>
            <td>${r.subject?.substring(0,40)}</td>
            <td>${r.attachments_processed?.length||0}개</td>
            <td><b>${hsCount}</b>건</td>
            <td>${erpOk}</td>
            <td>${oemCount > 0 ? '<span class="badge badge-warn">'+oemCount+'</span>' : '—'}</td>
          </tr>`;
        });
        html += '</tbody></table>';
      }
      html += '</div>';
      resultDiv.innerHTML = html;
      await mailLoadDashboard();
      await mailLoadLogs();
    } catch(e) {
      resultDiv.innerHTML = '<p style="color:red">오류: '+e.message+'</p>';
    }
    btn.disabled = false; btn.textContent = '🚀 파이프라인 실행';
  };

  window.mailShowDetail = async function(logId) {
    try {
      const r = await fetch('/api/mail-auto/logs?limit=100');
      const d = await r.json();
      const log = d.logs.find(l => l.id === logId);
      if (!log) return;
      
      const ds = log.result || {};
      const div = document.getElementById('mail-run-result');
      let html = `<div class="card" style="padding:16px;margin-top:12px;border:2px solid #2563eb20">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <h4 style="margin:0">📋 처리 상세 — ${log.subject?.substring(0,50)}</h4>
          <button onclick="this.parentElement.parentElement.remove()" class="btn btn-sm">✕ 닫기</button>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px;margin-bottom:12px">
          <div>📅 처리일시: <b>${(log.processed_at||'').substring(0,16)}</b></div>
          <div>📨 발신자: ${log.sender}</div>
          <div>📎 첨부파일: ${ds.filenames?.join(', ') || log.attachment_count+'개'}</div>
          <div>💱 환율: ${log.result?.exchange_rate || '—'}</div>
        </div>
        <table class="table table-sm" style="font-size:13px">
          <tr><td style="width:150px;color:#64748b">HS코드 총 품목</td><td><b>${ds.total_items||0}</b>개</td></tr>
          <tr><td style="color:#059669">✅ HS코드 입력</td><td><b>${ds.hs_filled||0}</b>건</td></tr>
          <tr><td style="color:#64748b">⏭ HS코드 스킵</td><td>${ds.hs_skipped||0}건 (패치코드/케이블 등)</td></tr>
          <tr><td style="color:#dc2626">⚠️ HS코드 미매칭</td><td>${ds.hs_unknown||0}건</td></tr>
          <tr><td colspan="2" style="border-top:2px solid #e2e8f0"></td></tr>
          <tr><td style="color:#7c3aed">📊 ERP 전표</td><td>${ds.erp_success ? '✅ 성공' : (ds.erp_lines_sent > 0 ? '❌ 실패' : '⏭ 미실행')} (${ds.erp_lines_sent||0}건 전송)</td></tr>`;
      if (ds.erp_unmapped && ds.erp_unmapped.length > 0) {
        html += `<tr><td style="color:#dc2626">⚠️ 품목코드 미매핑</td><td>${ds.erp_unmapped.length}건: ${ds.erp_unmapped.join(', ')}</td></tr>`;
      }
      html += `<tr><td>✉️ 회신 발송</td><td>${ds.reply_sent ? '✅ 발송 완료' : '⏭ 미발송'}</td></tr>
        </table></div>`;
      div.innerHTML = html;
    } catch(e) { console.error('상세 로드 실패:', e); }
  };

  // Excel 파일 테스트 업로드
  window.mailTestUpload = async function() {
    const input = document.getElementById('mail-test-file');
    if (!input.files.length) return alert('Excel 파일을 선택하세요');
    const fd = new FormData();
    fd.append('file', input.files[0]);
    const btn = document.getElementById('mail-test-btn');
    btn.disabled = true; btn.textContent = '처리 중...';
    try {
      const r = await fetch('/api/mail-auto/process-file', {method:'POST', body: fd});
      const d = await r.json();
      const div = document.getElementById('mail-test-result');
      const unknowns = d.items.filter(i => i.confidence === 'unknown');
      let html = `<div class="card" style="padding:12px;margin-top:8px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <div>
            <p style="margin:0"><b>통계:</b> 총 ${d.stats.total}개 | HS입력: <b style="color:#059669">${d.stats.hs_filled}</b>개 | 스킵: ${d.stats.skipped}개 | <span style="color:#dc2626">미매칭: ${d.stats.unknown}개</span></p>
            <p style="margin:4px 0 0"><b>ERP 대상:</b> ${d.erp_lines.length}개 | <b>OEM:</b> ${d.oem_items.length}개</p>
          </div>
          <button onclick="mailDownloadProcessed()" class="btn btn-primary" style="padding:8px 16px">📥 HS코드 입력된 Excel 다운로드</button>
        </div>`;
      // 미매칭 경고
      if (unknowns.length > 0) {
        html += '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:12px;margin-bottom:12px">';
        html += `<b style="color:#dc2626">⚠️ 미매칭 ${unknowns.length}건 — HS코드 규칙 추가 필요:</b>`;
        html += '<table class="table table-sm" style="font-size:12px;margin-top:8px"><thead><tr><th>모델명</th><th>카테고리</th><th>조치 필요</th></tr></thead><tbody>';
        unknowns.forEach(i => {
          html += `<tr style="background:#fef2f2"><td><b>${i.model}</b></td><td>${i.category}</td><td style="color:#dc2626">규칙 미등록</td></tr>`;
        });
        html += '</tbody></table></div>';
      }
      // 전체 목록
      if (d.items && d.items.length > 0) {
        html += '<table class="table table-sm" style="font-size:12px"><thead><tr><th>모델명</th><th>카테고리</th><th>HS코드</th><th>규칙</th></tr></thead><tbody>';
        d.items.forEach(i => {
          const isUnknown = i.confidence === 'unknown';
          const rowStyle = isUnknown ? 'background:#fef2f2;color:#dc2626' : '';
          const hsDisplay = i.hs_code ? `<b style="color:#059669">${i.hs_code}</b>` : '<span style="color:#999">—</span>';
          html += `<tr style="${rowStyle}"><td>${i.model}</td><td>${i.category}</td><td>${hsDisplay}</td><td>${i.rule}${isUnknown ? ' ⚠️' : ''}</td></tr>`;
        });
        html += '</tbody></table>';
      }
      html += '</div>';
      div.innerHTML = html;
    } catch(e) { alert('처리 실패: '+e.message); }
    btn.disabled = false; btn.textContent = '🔍 HS코드 테스트';
  };

  window.mailDownloadProcessed = function() {
    const input = document.getElementById('mail-test-file');
    if (!input.files.length) return alert('파일을 먼저 선택하고 테스트를 실행하세요');
    const fd = new FormData();
    fd.append('file', input.files[0]);
    fetch('/api/mail-auto/process-file/download', {method:'POST', body: fd})
      .then(r => {
        if (!r.ok) throw new Error('다운로드 실패');
        return r.blob();
      })
      .then(blob => {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'HS_' + input.files[0].name;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      })
      .catch(e => alert('다운로드 실패: ' + e.message));
  };

  // ─── ERP 구매전표 테스트 ───────────────────
  window.mailTestERP = async function() {
    const input = document.getElementById('mail-test-file');
    if (!input.files.length) return alert('Excel 파일을 선택하세요');
    const fd = new FormData();
    fd.append('file', input.files[0]);
    const rateInput = document.getElementById('mail-rate-input');
    if (rateInput && rateInput.value) fd.append('exchange_rate', rateInput.value);
    
    const btn = document.getElementById('mail-erp-btn');
    btn.disabled = true; btn.textContent = '조회 중...';
    try {
      const r = await fetch('/api/mail-auto/test-erp', {method:'POST', body: fd});
      const d = await r.json();
      const div = document.getElementById('mail-erp-result');
      let html = `<div class="card" style="padding:16px;margin-top:8px;border:2px solid #7c3aed20">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <div>
            <h4 style="margin:0;color:#7c3aed">📋 ERP 구매전표 미리보기</h4>
            <p style="font-size:12px;color:#666;margin:4px 0 0">거래처: ${d.cust_code} | 전표일자: ${d.io_date} | 환율: ${d.exchange_rate?.toLocaleString()}원</p>
          </div>
          <button onclick="mailSubmitERP()" class="btn" style="background:#7c3aed;color:#fff;padding:8px 16px" id="mail-erp-submit-btn">
            ⚡ ERP 전표 전송
          </button>
        </div>
        <table class="table table-sm" style="font-size:12px">
          <thead><tr><th>모델명</th><th>품목코드</th><th style="text-align:right">수량</th><th style="text-align:right">USD단가</th><th style="text-align:center">세율</th><th style="text-align:right">KRW단가</th><th style="text-align:right">공급가</th></tr></thead>
          <tbody>`;
      d.erp_lines.forEach(l => {
        const taxLabel = l.tax_rate === 1.22 ? '<span style="color:#dc2626">×1.22</span>' : '<span style="color:#2563eb">×1.18</span>';
        const prodLabel = l.prod_cd ? l.prod_cd : '<span style="color:#dc2626">⚠️ 미매핑</span>';
        const rowStyle = l.prod_cd ? '' : 'background:#fef2f2';
        html += `<tr style="${rowStyle}">
          <td style="font-size:11px">${l.model_name||l.prod_cd}</td>
          <td><b>${prodLabel}</b></td>
          <td style="text-align:right">${l.qty?.toLocaleString()}</td>
          <td style="text-align:right">\$${l.price_usd?.toFixed(3)}</td>
          <td style="text-align:center">${taxLabel}</td>
          <td style="text-align:right">₩${l.price_krw?.toLocaleString()}</td>
          <td style="text-align:right">₩${l.supply_amt?.toLocaleString()}</td>
        </tr>`;
      });
      html += `</tbody>
        <tfoot><tr style="font-weight:700;border-top:2px solid #e2e8f0">
          <td colspan="2">합계 (${d.total_lines}건)</td><td></td><td></td><td></td><td></td>
          <td style="text-align:right;color:#7c3aed">₩${d.total_amount?.toLocaleString()}</td>
        </tr></tfoot></table>`;
      if (d.unmapped_models && d.unmapped_models.length > 0) {
        html += `<div style="margin-top:10px;padding:10px 14px;background:#fef2f2;border:1px solid #fca5a5;border-radius:6px;font-size:12px">
          <b style="color:#dc2626">⚠️ 품목코드 미매핑 ${d.unmapped_models.length}건</b> — ERP 전송 시 제외됩니다: ${d.unmapped_models.join(', ')}
        </div>`;
      }
      if (d.oem_items && d.oem_items.length > 0) {
        html += `<div style="margin-top:10px;padding:8px 12px;background:#fef3c7;border-radius:6px;font-size:12px">
          <b>⚠️ OEM 미매핑 ${d.oem_items.length}건</b> (수동 입력 필요): ${d.oem_items.map(o=>o.description?.substring(0,30)).join(', ')}
        </div>`;
      }
      html += '</div>';
      div.innerHTML = html;
      // 전송용 데이터 저장
      window._erpPreviewData = d;
    } catch(e) { alert('ERP 미리보기 실패: '+e.message); }
    btn.disabled = false; btn.textContent = '📋 ERP 전표 미리보기';
  };

  window.mailSubmitERP = async function() {
    if (!window._erpPreviewData) return alert('먼저 미리보기를 실행하세요');
    if (!confirm('ERP에 구매전표를 전송합니다.\n계속하시겠습니까?')) return;
    const d = window._erpPreviewData;
    const btn = document.getElementById('mail-erp-submit-btn');
    btn.disabled = true; btn.textContent = '전송 중...';
    try {
      const r = await fetch('/api/mail-auto/submit-erp', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          erp_lines: d.erp_lines,
          exchange_rate: d.exchange_rate,
          io_date: d.io_date,
        })
      });
      const result = await r.json();
      if (result.success) {
        alert('✅ ERP 전표 전송 완료!');
        btn.textContent = '✅ 전송 완료';
      } else {
        alert('❌ 전송 실패: ' + (result.error || JSON.stringify(result)));
        btn.disabled = false; btn.textContent = '⚡ ERP 전표 전송';
      }
    } catch(e) {
      alert('전송 오류: '+e.message);
      btn.disabled = false; btn.textContent = '⚡ ERP 전표 전송';
    }
  };

  console.log('Mail Auto 모듈 로드 완료');
})();

// ══════════════════════════════════════════════════════════
//  DataLab (데이터랩) Module - 네이버 쇼핑인사이트 AI 소싱
// ══════════════════════════════════════════════════════════
let _dlKeywords = [];
let _dlAnalysisData = null;
let _dlHistoryId = null;
let _dlCharts = {};

function initDatalabPage() {
  // 1차 카테고리 로드
  dlLoadCategories();
  // 기본 날짜 설정 (최근 2년)
  const now = new Date();
  const twoYearsAgo = new Date(now.getFullYear() - 2, now.getMonth(), 1);
  document.getElementById('dl-start-date').value = twoYearsAgo.toISOString().slice(0, 10);
  document.getElementById('dl-end-date').value = now.toISOString().slice(0, 10);
  // 초기화
  _dlKeywords = [];
  _dlAnalysisData = null;
  _dlHistoryId = null;
  dlRenderKeywordTags();
  document.getElementById('dl-results').style.display = 'none';
  document.getElementById('dl-loading').style.display = 'none';
}

// ── 카테고리 캐스케이딩 ──
async function dlLoadCategories() {
  try {
    const res = await fetch('/api/datalab/categories', {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    const sel = document.getElementById('dl-cat1');
    sel.innerHTML = '<option value="">-- 선택 --</option>';
    (data.categories || []).forEach(c => {
      sel.innerHTML += `<option value="${c.cid}">${c.name}</option>`;
    });
  } catch (e) {
    console.error('[DataLab] 카테고리 로드 실패:', e);
  }
}

async function dlOnCat1Change(cid) {
  const cat2 = document.getElementById('dl-cat2');
  const cat3 = document.getElementById('dl-cat3');
  cat2.innerHTML = '<option value="">로딩 중...</option>';
  cat2.disabled = true;
  cat3.innerHTML = '<option value="">-- 2분류를 먼저 선택 --</option>';
  cat3.disabled = true;

  if (!cid) {
    cat2.innerHTML = '<option value="">-- 1분류를 먼저 선택 --</option>';
    return;
  }

  try {
    const res = await fetch(`/api/datalab/categories?parent_cid=${cid}`, {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    cat2.innerHTML = '<option value="">-- 선택 --</option>';
    (data.categories || []).forEach(c => {
      cat2.innerHTML += `<option value="${c.cid}">${c.name}</option>`;
    });
    cat2.disabled = false;
  } catch (e) {
    cat2.innerHTML = '<option value="">로드 실패</option>';
    console.error('[DataLab] 2분류 로드 실패:', e);
  }
}

async function dlOnCat2Change(cid) {
  const cat3 = document.getElementById('dl-cat3');
  cat3.innerHTML = '<option value="">로딩 중...</option>';
  cat3.disabled = true;

  if (!cid) {
    cat3.innerHTML = '<option value="">-- 2분류를 먼저 선택 --</option>';
    return;
  }

  try {
    const res = await fetch(`/api/datalab/categories?parent_cid=${cid}`, {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    const cats = data.categories || [];
    if (cats.length === 0) {
      cat3.innerHTML = '<option value="">-- 하위 카테고리 없음 --</option>';
    } else {
      cat3.innerHTML = '<option value="">-- 선택 (선택사항) --</option>';
      cats.forEach(c => {
        cat3.innerHTML += `<option value="${c.cid}">${c.name}</option>`;
      });
      cat3.disabled = false;
    }
  } catch (e) {
    cat3.innerHTML = '<option value="">로드 실패</option>';
    console.error('[DataLab] 3분류 로드 실패:', e);
  }
}

// ── 키워드 관리 ──
let _dlComposing = false;  // 한글 IME 조합 상태 추적

function dlKeywordCompositionStart() { _dlComposing = true; }
function dlKeywordCompositionEnd(e) {
  _dlComposing = false;
  // compositionend 후 Enter 처리
  if (e && e.target && e.target.dataset.pendingEnter === 'true') {
    e.target.dataset.pendingEnter = 'false';
    const kw = e.target.value.trim();
    if (kw && !_dlKeywords.includes(kw) && _dlKeywords.length < 20) {
      _dlKeywords.push(kw);
      e.target.value = '';
      dlRenderKeywordTags();
    }
  }
}

function dlKeywordKeydown(e) {
  if (e.key === 'Enter') {
    e.preventDefault();
    // 한글 조합 중이면 compositionend에서 처리
    if (_dlComposing) {
      e.target.dataset.pendingEnter = 'true';
      return;
    }
    const input = document.getElementById('dl-keyword-input');
    const kw = input.value.trim();
    if (kw && !_dlKeywords.includes(kw) && _dlKeywords.length < 20) {
      _dlKeywords.push(kw);
      input.value = '';
      dlRenderKeywordTags();
    }
  }
}

function dlRemoveKeyword(idx) {
  _dlKeywords.splice(idx, 1);
  dlRenderKeywordTags();
}

function dlAddKeyword(kw) {
  if (kw && !_dlKeywords.includes(kw) && _dlKeywords.length < 20) {
    _dlKeywords.push(kw);
    dlRenderKeywordTags();
  }
}

function dlRenderKeywordTags() {
  const container = document.getElementById('dl-keyword-tags');
  container.innerHTML = _dlKeywords.map((kw, i) =>
    `<span class="dl-keyword-tag">${kw}<span class="dl-tag-remove" onclick="dlRemoveKeyword(${i})">&times;</span></span>`
  ).join('');
  document.getElementById('dl-keyword-count').textContent = `${_dlKeywords.length}/20`;
}

async function dlSuggestKeywords() {
  const input = document.getElementById('dl-keyword-input');
  const query = input.value.trim() || (_dlKeywords.length > 0 ? _dlKeywords[0] : '');
  if (!query) {
    alert('키워드를 입력하거나 기존 키워드가 있어야 합니다.');
    return;
  }
  try {
    const res = await fetch(`/api/datalab/suggest-keywords?query=${encodeURIComponent(query)}`, {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const popup = document.getElementById('dl-suggest-popup');
    const list = document.getElementById('dl-suggest-list');
    if (!res.ok) {
      const errData = await res.json().catch(() => ({}));
      list.innerHTML = `<div style="padding:12px;color:#ef4444">오류: ${errData.detail || res.status}</div>`;
      popup.style.display = 'block';
      return;
    }
    const data = await res.json();
    const suggestions = data.suggestions || [];
    list.innerHTML = suggestions.length > 0
      ? suggestions.map(s =>
          `<div class="dl-suggest-item" onclick="dlAddKeyword('${s.replace(/'/g, "\\'")}'); this.remove();">${s}</div>`
        ).join('')
      : '<div style="padding:12px;color:var(--text-secondary)">추천 결과가 없습니다</div>';
    popup.style.display = 'block';
  } catch (e) {
    console.error('[DataLab] 키워드 추천 실패:', e);
    const popup = document.getElementById('dl-suggest-popup');
    const list = document.getElementById('dl-suggest-list');
    list.innerHTML = '<div style="padding:12px;color:#ef4444">네트워크 오류가 발생했습니다</div>';
    popup.style.display = 'block';
  }
}

async function dlLoadSeedKeywords() {
  const catCode = document.getElementById('dl-cat3').value || document.getElementById('dl-cat2').value || document.getElementById('dl-cat1').value;
  if (!catCode) {
    alert('카테고리를 먼저 선택해주세요.');
    return;
  }
  try {
    const res = await fetch(`/api/datalab/seed-keywords?category_code=${catCode}`, {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    const popup = document.getElementById('dl-suggest-popup');
    const list = document.getElementById('dl-suggest-list');
    const kws = data.keywords || [];
    if (kws.length === 0) {
      list.innerHTML = '<div style="padding:12px;color:var(--text-secondary)">축적된 인기 키워드가 없습니다. 분석을 실행하면 자동 축적됩니다.</div>';
    } else {
      list.innerHTML = kws.map(k =>
        `<div class="dl-suggest-item" onclick="dlAddKeyword('${k.keyword.replace(/'/g, "\\'")}')">
          ${k.keyword} <small style="color:var(--text-secondary)">(검색 ${k.count}회, 점수 ${k.score || '-'})</small>
        </div>`
      ).join('');
    }
    popup.style.display = 'block';
  } catch (e) {
    console.error('[DataLab] 시드 키워드 로드 실패:', e);
  }
}

// ── 트렌드 분석 실행 ──
async function dlRunAnalysis() {
  const catCode = document.getElementById('dl-cat3').value || document.getElementById('dl-cat2').value || document.getElementById('dl-cat1').value;
  if (!catCode) { alert('카테고리를 선택해주세요.'); return; }
  if (_dlKeywords.length === 0) { alert('키워드를 1개 이상 입력해주세요.'); return; }

  const startDate = document.getElementById('dl-start-date').value;
  const endDate = document.getElementById('dl-end-date').value;
  if (!startDate || !endDate) { alert('분석 기간을 설정해주세요.'); return; }

  // 선택된 카테고리명 추출
  const cat1Sel = document.getElementById('dl-cat1');
  const cat2Sel = document.getElementById('dl-cat2');
  const cat3Sel = document.getElementById('dl-cat3');
  let catName = '';
  if (cat3Sel.value && cat3Sel.selectedIndex > 0) catName = cat3Sel.options[cat3Sel.selectedIndex].text;
  else if (cat2Sel.value && cat2Sel.selectedIndex > 0) catName = cat2Sel.options[cat2Sel.selectedIndex].text;
  else if (cat1Sel.value && cat1Sel.selectedIndex > 0) catName = cat1Sel.options[cat1Sel.selectedIndex].text;

  // 연령 필터
  const ages = [];
  document.querySelectorAll('.dl-age-chips input:checked').forEach(cb => ages.push(cb.value));

  const body = {
    category_code: catCode,
    category_name: catName,
    keywords: _dlKeywords,
    start_date: startDate,
    end_date: endDate,
    time_unit: document.getElementById('dl-time-unit').value,
    device: document.getElementById('dl-device').value,
    gender: document.getElementById('dl-gender').value,
    ages: ages,
  };

  document.getElementById('dl-loading').style.display = 'block';
  document.getElementById('dl-results').style.display = 'none';
  document.getElementById('dl-analyze-btn').disabled = true;

  try {
    const res = await fetch('/api/datalab/analyze', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (localStorage.getItem('token') || '')
      },
      body: JSON.stringify(body)
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || '분석 실패');
    }

    _dlAnalysisData = await res.json();
    _dlHistoryId = _dlAnalysisData.history_id || null;
    console.log('[DataLab] 분석 결과:', JSON.stringify(_dlAnalysisData).substring(0, 500));

    if (_dlAnalysisData.error) {
      alert('⚠️ ' + _dlAnalysisData.error);
    } else if (!_dlAnalysisData.keywords || _dlAnalysisData.keywords.length === 0) {
      alert('분석 결과: 키워드 트렌드 데이터가 없습니다.\n네이버 API 키 설정을 확인해주세요.\n\n설정 > API 키 관리 > 네이버 DataLab Client ID/Secret');
    }

    dlRenderResults();

  } catch (e) {
    alert('분석 오류: ' + e.message);
    console.error('[DataLab] 분석 실패:', e);
  } finally {
    document.getElementById('dl-loading').style.display = 'none';
    document.getElementById('dl-analyze-btn').disabled = false;
  }
}

// ── 결과 렌더링 ──
function dlRenderResults() {
  if (!_dlAnalysisData) return;
  document.getElementById('dl-results').style.display = 'block';

  // 요약 카드
  dlRenderSummaryCards();
  // 데이터 테이블
  dlRenderTable();
  // 차트
  dlRenderTrendChart();
  dlRenderSeasonChart();
  dlRenderDemographicCharts();
  // AI 탭 초기화
  document.getElementById('dl-ai-content').innerHTML = `
    <div class="dl-ai-placeholder">
      <p>🤖 분석이 완료되었습니다. AI 인사이트를 생성하시겠습니까?</p>
      <button class="btn btn-primary" onclick="dlGenerateAI()" id="dl-ai-btn">🤖 AI 인사이트 생성</button>
    </div>`;
  // 첫 번째 탭 활성화
  dlSwitchTab('trend');
}

function dlRenderSummaryCards() {
  const kws = _dlAnalysisData.keywords || [];
  const hot = kws.filter(k => k.momentum === '급상승' || k.momentum === '상승').slice(0, 3);
  const caution = kws.filter(k => k.momentum === '하락').slice(0, 3);
  const steady = kws.filter(k => k.trust_score >= 80).slice(0, 3);

  let html = '';
  html += `<div class="dl-summary-card dl-card-info"><h4>분석 키워드</h4><div class="dl-card-value">${kws.length}개</div><div class="dl-card-sub">API ${_dlAnalysisData.api_calls || 0}회 호출</div></div>`;

  if (kws.length === 0) {
    html += `<div class="dl-summary-card dl-card-caution" style="grid-column:span 3"><h4>⚠️ 데이터 없음</h4><div class="dl-card-sub">네이버 API에서 키워드 트렌드 데이터를 받지 못했습니다.<br>설정 &gt; API 키 관리에서 네이버 DataLab Client ID/Secret을 확인해주세요.</div></div>`;
    document.getElementById('dl-summary-cards').innerHTML = html;
    return;
  }

  if (hot.length > 0) {
    html += `<div class="dl-summary-card dl-card-hot"><h4>🔥 주목 키워드</h4><div class="dl-card-value">${hot[0].keyword}</div><div class="dl-card-sub">${hot.map(h => h.keyword).join(', ')}</div></div>`;
  }
  if (caution.length > 0) {
    html += `<div class="dl-summary-card dl-card-caution"><h4>⚠️ 주의 키워드</h4><div class="dl-card-value">${caution[0].keyword}</div><div class="dl-card-sub">${caution.map(c => c.keyword).join(', ')}</div></div>`;
  }
  if (steady.length > 0) {
    html += `<div class="dl-summary-card dl-card-steady"><h4>✅ 스테디셀러</h4><div class="dl-card-value">${steady[0].keyword}</div><div class="dl-card-sub">신뢰도 ${steady[0].trust_score}점</div></div>`;
  }

  document.getElementById('dl-summary-cards').innerHTML = html;
}

function dlRenderTable() {
  const tbody = document.getElementById('dl-result-tbody');
  const kws = _dlAnalysisData.keywords || [];
  tbody.innerHTML = kws.map(kw => {
    const momClass = kw.momentum === '급상승' ? 'dl-momentum-surge' : kw.momentum === '상승' ? 'dl-momentum-up' : kw.momentum === '하락' ? 'dl-momentum-down' : 'dl-momentum-stable';
    const peakStr = (kw.peak_months || []).map(m => m + '월').join(', ') || '-';
    const lowStr = (kw.low_months || []).map(m => m + '월').join(', ') || '-';
    const sparkData = (kw.trend_data || []).slice(-12);
    const sparkSvg = dlMiniSpark(sparkData);
    return `<tr>
      <td><strong>${kw.keyword}</strong></td>
      <td>${kw.trust_score || 0}</td>
      <td>${kw.overall_score || 0}</td>
      <td class="${momClass}">${kw.momentum || '?'} (${kw.momentum_pct > 0 ? '+' : ''}${(kw.momentum_pct || 0).toFixed(1)}%)</td>
      <td>${peakStr}</td>
      <td>${lowStr}</td>
      <td>${sparkSvg}</td>
    </tr>`;
  }).join('');
}

function dlMiniSpark(data) {
  if (!data || data.length === 0) return '-';
  const vals = data.map(d => d.ratio || 0);
  const max = Math.max(...vals, 1);
  const w = 80, h = 24;
  const step = w / (vals.length - 1 || 1);
  const points = vals.map((v, i) => `${i * step},${h - (v / max) * h}`).join(' ');
  return `<svg class="dl-mini-spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><polyline points="${points}" fill="none" stroke="var(--primary)" stroke-width="1.5"/></svg>`;
}

// ── 차트 렌더링 ──
function dlDestroyChart(key) {
  if (_dlCharts[key]) { _dlCharts[key].destroy(); delete _dlCharts[key]; }
}

function dlRenderTrendChart() {
  dlDestroyChart('trend');
  const canvas = document.getElementById('dl-trend-chart');
  if (!canvas) return;
  const kws = _dlAnalysisData.keywords || [];
  if (kws.length === 0) return;

  const colors = ['#6366f1','#ef4444','#22c55e','#f59e0b','#8b5cf6','#06b6d4','#ec4899','#14b8a6','#f97316','#a855f7','#64748b','#0ea5e9','#d946ef','#84cc16','#e11d48','#0891b2','#7c3aed','#059669','#dc2626','#2563eb'];
  const labels = (kws[0].trend_data || []).map(d => d.period.slice(0, 7));
  const datasets = kws.slice(0, 10).map((kw, i) => ({
    label: kw.keyword,
    data: (kw.trend_data || []).map(d => d.ratio),
    borderColor: colors[i % colors.length],
    backgroundColor: 'transparent',
    borderWidth: 2,
    tension: 0.3,
    pointRadius: 1,
  }));

  _dlCharts['trend'] = new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      plugins: { title: { display: true, text: '키워드별 검색 트렌드 추이' }, legend: { position: 'bottom' } },
      scales: { y: { beginAtZero: true, max: 100, title: { display: true, text: '검색 비율' } } },
      interaction: { intersect: false, mode: 'index' },
    }
  });
}

function dlRenderSeasonChart() {
  dlDestroyChart('season');
  const canvas = document.getElementById('dl-season-chart');
  if (!canvas) return;
  const kws = _dlAnalysisData.keywords || [];
  if (kws.length === 0) return;

  const months = ['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
  const colors = ['#6366f1','#ef4444','#22c55e','#f59e0b','#8b5cf6','#06b6d4'];
  const datasets = kws.slice(0, 5).map((kw, i) => {
    const monthlyAvg = kw.monthly_avg || {};
    const data = months.map((_, mi) => monthlyAvg[String(mi + 1)] || 0);
    return {
      label: kw.keyword,
      data,
      backgroundColor: colors[i % colors.length] + '88',
      borderColor: colors[i % colors.length],
      borderWidth: 1,
    };
  });

  _dlCharts['season'] = new Chart(canvas, {
    type: 'bar',
    data: { labels: months, datasets },
    options: {
      responsive: true,
      plugins: { title: { display: true, text: '월별 계절성 패턴 (연간 평균)' }, legend: { position: 'bottom' } },
      scales: { y: { beginAtZero: true, title: { display: true, text: '평균 비율' } } },
    }
  });
}

function dlRenderDemographicCharts() {
  // 기기별
  dlDestroyChart('device');
  const devCanvas = document.getElementById('dl-device-chart');
  if (devCanvas && _dlAnalysisData.device_data) {
    const results = (_dlAnalysisData.device_data.results || [])[0];
    if (results && results.data) {
      const grouped = {};
      results.data.forEach(d => {
        if (!grouped[d.group]) grouped[d.group] = [];
        grouped[d.group].push({ period: d.period, ratio: d.ratio });
      });
      const labels = (grouped['mo'] || grouped['pc'] || []).map(d => d.period.slice(0, 7));
      const datasets = [];
      if (grouped['mo']) datasets.push({ label: '모바일', data: grouped['mo'].map(d => d.ratio), borderColor: '#6366f1', backgroundColor: 'transparent', borderWidth: 2, tension: 0.3 });
      if (grouped['pc']) datasets.push({ label: 'PC', data: grouped['pc'].map(d => d.ratio), borderColor: '#f59e0b', backgroundColor: 'transparent', borderWidth: 2, tension: 0.3 });
      _dlCharts['device'] = new Chart(devCanvas, { type: 'line', data: { labels, datasets }, options: { responsive: true, plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true, max: 100 } } } });
    }
  }

  // 성별
  dlDestroyChart('gender');
  const genCanvas = document.getElementById('dl-gender-chart');
  if (genCanvas && _dlAnalysisData.gender_data) {
    const results = (_dlAnalysisData.gender_data.results || [])[0];
    if (results && results.data) {
      const grouped = {};
      results.data.forEach(d => {
        if (!grouped[d.group]) grouped[d.group] = [];
        grouped[d.group].push({ period: d.period, ratio: d.ratio });
      });
      const labels = (grouped['f'] || grouped['m'] || []).map(d => d.period.slice(0, 7));
      const datasets = [];
      if (grouped['f']) datasets.push({ label: '여성', data: grouped['f'].map(d => d.ratio), borderColor: '#ec4899', backgroundColor: 'transparent', borderWidth: 2, tension: 0.3 });
      if (grouped['m']) datasets.push({ label: '남성', data: grouped['m'].map(d => d.ratio), borderColor: '#3b82f6', backgroundColor: 'transparent', borderWidth: 2, tension: 0.3 });
      _dlCharts['gender'] = new Chart(genCanvas, { type: 'line', data: { labels, datasets }, options: { responsive: true, plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true, max: 100 } } } });
    }
  }

  // 연령별
  dlDestroyChart('age');
  const ageCanvas = document.getElementById('dl-age-chart');
  if (ageCanvas && _dlAnalysisData.age_data) {
    const results = (_dlAnalysisData.age_data.results || [])[0];
    if (results && results.data) {
      const ageLabels = { '10': '10대', '20': '20대', '30': '30대', '40': '40대', '50': '50대', '60': '60+' };
      const ageColors = { '10': '#f97316', '20': '#ef4444', '30': '#8b5cf6', '40': '#22c55e', '50': '#06b6d4', '60': '#64748b' };
      const grouped = {};
      results.data.forEach(d => {
        if (!grouped[d.group]) grouped[d.group] = [];
        grouped[d.group].push({ period: d.period, ratio: d.ratio });
      });
      const labels = (Object.values(grouped)[0] || []).map(d => d.period.slice(0, 7));
      const datasets = Object.keys(grouped).map(g => ({
        label: ageLabels[g] || g,
        data: grouped[g].map(d => d.ratio),
        borderColor: ageColors[g] || '#999',
        backgroundColor: 'transparent',
        borderWidth: 2,
        tension: 0.3,
      }));
      _dlCharts['age'] = new Chart(ageCanvas, { type: 'line', data: { labels, datasets }, options: { responsive: true, plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true, max: 100 } } } });
    }
  }
}

// ── 탭 전환 ──
function dlSwitchTab(tabId) {
  document.querySelectorAll('.dl-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
  document.querySelectorAll('.dl-tab-content').forEach(c => c.classList.toggle('active', c.id === 'dl-tab-' + tabId));
}

// ── AI 인사이트 생성 ──
async function dlGenerateAI() {
  if (!_dlHistoryId) { alert('분석을 먼저 실행해주세요.'); return; }
  const btn = document.getElementById('dl-ai-btn');
  const content = document.getElementById('dl-ai-content');
  if (btn) btn.disabled = true;
  content.innerHTML = '<div class="dl-loading"><div class="dl-spinner"></div><p>Claude AI가 인사이트를 생성 중입니다...</p></div>';
  dlSwitchTab('ai');

  try {
    const res = await fetch('/api/datalab/ai-insight', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (localStorage.getItem('token') || '')
      },
      body: JSON.stringify({ history_id: _dlHistoryId })
    });

    if (!res.ok) throw new Error('AI 인사이트 생성 실패');
    const insight = await res.json();

    if (insight.error) {
      content.innerHTML = `<div class="dl-ai-placeholder"><p style="color:#ef4444">❌ ${insight.error}</p>
        <button class="btn btn-primary" onclick="dlGenerateAI()" id="dl-ai-btn">재시도</button></div>`;
      return;
    }

    let html = '';
    // Executive Summary
    if (insight.executive_summary) {
      html += `<div class="dl-ai-section"><h4>📋 시장 요약</h4><div class="dl-ai-summary">${insight.executive_summary}</div></div>`;
    }
    // Hot keywords
    if (insight.hot_keywords && insight.hot_keywords.length) {
      html += '<div class="dl-ai-section"><h4>🔥 주목 키워드</h4><ul class="dl-ai-list">';
      insight.hot_keywords.forEach(k => {
        html += `<li class="hot"><strong>${k.keyword}</strong> — ${k.reason}<br><small>💡 ${k.action}</small></li>`;
      });
      html += '</ul></div>';
    }
    // Caution keywords
    if (insight.caution_keywords && insight.caution_keywords.length) {
      html += '<div class="dl-ai-section"><h4>⚠️ 주의 키워드</h4><ul class="dl-ai-list">';
      insight.caution_keywords.forEach(k => {
        html += `<li class="caution"><strong>${k.keyword}</strong> — ${k.reason} <span style="font-size:11px;background:#f59e0b22;padding:2px 6px;border-radius:4px">${k.risk_level}</span></li>`;
      });
      html += '</ul></div>';
    }
    // Seasonal advice
    if (insight.seasonal_advice) {
      html += `<div class="dl-ai-section"><h4>📅 시즌 어드바이스</h4><div class="dl-ai-summary">${insight.seasonal_advice}</div></div>`;
    }
    // Target insight
    if (insight.target_insight) {
      html += `<div class="dl-ai-section"><h4>🎯 타겟 인사이트</h4><div class="dl-ai-summary">${insight.target_insight}</div></div>`;
    }
    // Action items
    if (insight.action_items && insight.action_items.length) {
      html += '<div class="dl-ai-section"><h4>✅ 실행 항목</h4><div class="dl-ai-actions"><ol>';
      insight.action_items.forEach(a => { html += `<li>${a}</li>`; });
      html += '</ol></div></div>';
    }
    // Meta
    if (insight._meta) {
      html += `<div style="text-align:right;font-size:11px;color:var(--text-secondary);margin-top:12px">생성: ${insight._meta.generated_at?.slice(0, 19) || ''} | 모델: ${insight._meta.model || ''}</div>`;
    }

    content.innerHTML = html;

  } catch (e) {
    content.innerHTML = `<div class="dl-ai-placeholder"><p style="color:#ef4444">❌ ${e.message}</p>
      <button class="btn btn-primary" onclick="dlGenerateAI()" id="dl-ai-btn">재시도</button></div>`;
  }
}

// ── 이력 ──
async function dlLoadHistory() {
  document.getElementById('dl-history-modal').style.display = 'flex';
  const list = document.getElementById('dl-history-list');
  list.innerHTML = '<div class="dl-loading"><div class="dl-spinner"></div></div>';

  try {
    const res = await fetch('/api/datalab/history?limit=20', {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    const items = data.history || [];
    if (items.length === 0) {
      list.innerHTML = '<p style="text-align:center;color:var(--text-secondary);padding:20px">분석 이력이 없습니다.</p>';
      return;
    }
    list.innerHTML = items.map(h => `
      <div class="dl-history-item" onclick="dlLoadHistoryDetail(${h.id})">
        <div class="dl-hist-cat">${h.category_name || h.category_code}</div>
        <div class="dl-hist-kw">${(h.keywords || []).join(', ')}</div>
        <div class="dl-hist-date">${h.created_at || ''}</div>
      </div>
    `).join('');
  } catch (e) {
    list.innerHTML = '<p style="color:#ef4444;padding:12px">이력 로드 실패</p>';
  }
}

async function dlLoadHistoryDetail(id) {
  document.getElementById('dl-history-modal').style.display = 'none';
  document.getElementById('dl-loading').style.display = 'block';
  document.getElementById('dl-results').style.display = 'none';

  try {
    const res = await fetch(`/api/datalab/history/${id}`, {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    _dlAnalysisData = data.trend_data || {};
    _dlHistoryId = id;
    _dlKeywords = data.keywords || [];
    dlRenderKeywordTags();
    dlRenderResults();
  } catch (e) {
    alert('이력 로드 실패: ' + e.message);
  } finally {
    document.getElementById('dl-loading').style.display = 'none';
  }
}

// ── 블랙리스트 ──
async function dlShowBlacklist() {
  document.getElementById('dl-blacklist-modal').style.display = 'flex';
  const list = document.getElementById('dl-blacklist-list');
  list.innerHTML = '<div class="dl-loading"><div class="dl-spinner"></div></div>';

  try {
    const res = await fetch('/api/datalab/brand-blacklist', {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    const data = await res.json();
    const items = data.blacklist || [];
    if (items.length === 0) {
      list.innerHTML = '<p style="text-align:center;color:var(--text-secondary)">등록된 제외 브랜드가 없습니다.</p>';
    } else {
      list.innerHTML = items.map(b =>
        `<div class="dl-bl-item"><span>${b.name}</span><button onclick="dlRemoveBlacklist(${b.id})">&times;</button></div>`
      ).join('');
    }
  } catch (e) {
    list.innerHTML = '<p style="color:#ef4444">로드 실패</p>';
  }
}

async function dlAddBlacklist() {
  const input = document.getElementById('dl-bl-input');
  const name = input.value.trim();
  if (!name) return;

  try {
    await fetch('/api/datalab/brand-blacklist', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (localStorage.getItem('token') || '')
      },
      body: JSON.stringify({ brand_name: name })
    });
    input.value = '';
    dlShowBlacklist();
  } catch (e) {
    alert('추가 실패');
  }
}

async function dlRemoveBlacklist(id) {
  try {
    await fetch(`/api/datalab/brand-blacklist/${id}`, {
      method: 'DELETE',
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('token') || '') }
    });
    dlShowBlacklist();
  } catch (e) {
    alert('삭제 실패');
  }
}

// ── 엑셀 내보내기 ──
async function dlExportExcel() {
  if (!_dlHistoryId) { alert('분석을 먼저 실행해주세요.'); return; }
  try {
    const res = await fetch('/api/datalab/export-excel', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (localStorage.getItem('token') || '')
      },
      body: JSON.stringify({ history_id: _dlHistoryId })
    });
    if (!res.ok) throw new Error('다운로드 실패');
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `datalab_analysis_${_dlHistoryId}.xlsx`;
    a.click();
    URL.revokeObjectURL(url);
  } catch (e) {
    alert('엑셀 다운로드 실패: ' + e.message);
  }
}
