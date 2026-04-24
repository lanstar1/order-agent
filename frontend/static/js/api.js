/* API 호출 헬퍼 (JWT 인증 지원) */
const API_BASE = window.location.origin;

const api = {
  // ── JWT 토큰 관리 ──
  getToken() {
    return localStorage.getItem("jwt_token") || "";
  },
  setToken(token) {
    localStorage.setItem("jwt_token", token);
  },
  clearToken() {
    localStorage.removeItem("jwt_token");
  },
  isLoggedIn() {
    return !!this.getToken();
  },

  // ── 공통 헤더 ──
  _headers(contentType = "application/json") {
    const h = {};
    if (contentType) h["Content-Type"] = contentType;
    const token = this.getToken();
    if (token) h["Authorization"] = `Bearer ${token}`;
    return h;
  },

  // ── 공통 요청 ──
  async request(method, path, body = null) {
    const opts = {
      method,
      headers: this._headers(),
    };
    if (body) opts.body = JSON.stringify(body);

    const res = await fetch(API_BASE + path, opts);

    // 401 → 로그인 필요
    if (res.status === 401) {
      this.clearToken();
      if (typeof window.onAuthRequired === "function") {
        window.onAuthRequired();
      }
      throw new Error("인증이 만료되었습니다. 다시 로그인해주세요.");
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      const msg = err.detail || "API 오류";
      const e = new Error(msg);
      e.status = res.status;
      throw e;
    }
    return res.json();
  },

  get:    (path) => api.request("GET", path),
  post:   (path, body) => api.request("POST", path, body),
  put:    (path, body) => api.request("PUT", path, body),
  delete: (path) => api.request("DELETE", path),

  // 폼 데이터 (파일 업로드) — JWT 헤더 포함
  async postForm(path, formData) {
    const headers = {};
    const token = this.getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;

    const res = await fetch(API_BASE + path, {
      method: "POST",
      body: formData,
      headers,
    });

    if (res.status === 401) {
      this.clearToken();
      if (typeof window.onAuthRequired === "function") window.onAuthRequired();
      throw new Error("인증이 만료되었습니다. 다시 로그인해주세요.");
    }
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },

  // ── 인증 API (토큰 불필요) ──
  async login(empCd, password) {
    const res = await fetch(API_BASE + "/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ emp_cd: empCd, password }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: "로그인 실패" }));
      throw new Error(err.detail || "로그인 실패");
    }
    const data = await res.json();
    if (data.token) this.setToken(data.token);
    return data;
  },

  async getEmployees() {
    const res = await fetch(API_BASE + "/api/auth/employees");
    if (!res.ok) throw new Error("직원 목록 조회 실패");
    return res.json();
  },

  async refreshToken() {
    try {
      const data = await this.post("/api/auth/refresh");
      if (data.token) this.setToken(data.token);
      return data;
    } catch (e) {
      this.clearToken();
      throw e;
    }
  },

  async getMe() {
    return this.get("/api/auth/me");
  },

  logout() {
    this.clearToken();
    if (typeof window.onAuthRequired === "function") window.onAuthRequired();
  },

  // ── 거래처 ──
  getCustomers:  () => api.get("/api/customers/"),
  customerSyncERP() { return api.post("/api/customers/sync-erp"); },
  customerCount() { return api.get("/api/customers/count"); },
  // ── 발주서 처리 ──
  processOrder:  (body) => api.post("/api/orders/process", body),
  confirmOrder:  (body) => api.post("/api/orders/confirm", body),
  submitERP:     (orderId, empCd = "") => api.post(`/api/orders/submit-erp/${orderId}?emp_cd=${encodeURIComponent(empCd)}`),
  listOrders:    (limit = 20) => api.get(`/api/orders/list?limit=${limit}`),
  getOrder:      (orderId) => api.get(`/api/orders/${orderId}`),
  // 이미지 OCR 처리
  async processImage(formData) {
    const headers = {};
    const token = this.getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;

    const res = await fetch(API_BASE + "/api/orders/process-image", {
      method: "POST",
      body: formData,
      headers,
    });
    if (res.status === 401) {
      this.clearToken();
      if (typeof window.onAuthRequired === "function") window.onAuthRequired();
      throw new Error("인증이 만료되었습니다.");
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || "OCR 처리 오류");
    }
    return res.json();
  },

  // ── 견적서입력 ──
  processSaleOrder:  (body) => api.post("/api/sale-orders/process", body),
  confirmSaleOrder:  (body) => api.post("/api/sale-orders/confirm", body),
  submitSaleOrderERP: (orderId, empCd = "") => api.post(`/api/sale-orders/submit-erp/${orderId}?emp_cd=${encodeURIComponent(empCd)}`),
  listSaleOrders:    (limit = 20) => api.get(`/api/sale-orders/list?limit=${limit}`),
  getSaleOrder:      (orderId) => api.get(`/api/sale-orders/${orderId}`),
  async processSaleOrderImage(formData) {
    const headers = {};
    const token = this.getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;

    const res = await fetch(API_BASE + "/api/sale-orders/process-image", {
      method: "POST",
      body: formData,
      headers,
    });
    if (res.status === 401) {
      this.clearToken();
      if (typeof window.onAuthRequired === "function") window.onAuthRequired();
      throw new Error("인증이 만료되었습니다.");
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || "OCR 처리 오류");
    }
    return res.json();
  },

  // ── 자료관리 ──
  materialsSyncAll:     () => api.post("/api/materials/sync"),
  materialsSyncOne:     (sourceId) => api.post(`/api/materials/sync/${sourceId}`),
  materialsSearch:      (q, vendor = "", limit = 20) => {
    const params = new URLSearchParams({ q, limit });
    if (vendor) params.append("vendor", vendor);
    return api.get(`/api/materials/search?${params}`);
  },
  materialsSearchDocs:  (q, category = "", limit = 20) => {
    const params = new URLSearchParams({ q, limit });
    if (category) params.append("category", category);
    return api.get(`/api/materials/search-docs?${params}`);
  },
  materialsSources:     () => api.get("/api/materials/sources"),
  materialsAddSource:   (body) => api.post("/api/materials/sources", body),
  materialsDeleteSource:(id) => api.delete(`/api/materials/sources/${id}`),
  materialsToggleSource:(id) => api.put(`/api/materials/sources/${id}/toggle`),

  // ── 자료검색 (Drive 문서 브라우저) ──
  driveCategories:     () => api.get("/api/materials/drive/categories"),
  driveDocuments:      (category = "", q = "", limit = 200, offset = 0) => {
    const params = new URLSearchParams({ limit, offset });
    if (category) params.append("category", category);
    if (q) params.append("q", q);
    return api.get(`/api/materials/drive/documents?${params}`);
  },

  // ── 단가표 조회 (Sheets 뷰어) ──
  priceSheetVendors:   () => api.get("/api/materials/price-sheets/vendors"),
  priceSheetTabs:      (sourceId) => api.get(`/api/materials/price-sheets/${sourceId}/tabs`),
  priceSheetData:      (sourceId, q = "", tab = "", limit = 500, offset = 0) => {
    const params = new URLSearchParams({ limit, offset });
    if (q) params.append("q", q);
    if (tab) params.append("tab", tab);
    return api.get(`/api/materials/price-sheets/${sourceId}?${params}`);
  },

  // ── 발주서 학습 (Training) ──
  trainingPreviewExcel: (formData) => api.postForm("/api/training/preview-excel", formData),
  trainingUpload: (formData) => api.postForm("/api/training/upload", formData),
  trainingSaveJson: (body) => api.post("/api/training/save-json", body),
  trainingPairs: (custCode = "", limit = 50) => {
    const params = new URLSearchParams({ limit });
    if (custCode) params.append("cust_code", custCode);
    return api.get(`/api/training/pairs?${params}`);
  },
  trainingPairDetail: (pairId) => api.get(`/api/training/pairs/${pairId}`),
  trainingDeletePair: (pairId) => api.delete(`/api/training/pairs/${pairId}`),
  trainingStats: () => api.get("/api/training/stats"),

  // ── 대량 학습 (Bulk Training) ──
  bulkCreateSession: (formData) => api.postForm("/api/training/bulk/create-session", formData),
  bulkExtractPo: (formData) => api.postForm("/api/training/bulk/extract-po", formData),
  bulkSuggestMatches: (sessionId) => {
    const fd = new FormData();
    fd.append("session_id", sessionId);
    return api.postForm("/api/training/bulk/suggest-matches", fd);
  },
  bulkConfirm: (body) => api.post("/api/training/bulk/confirm", body),
  bulkSession: (sessionId) => api.get(`/api/training/bulk/session/${sessionId}`),

  // ── 재고 조회 ──
  inventoryAutocomplete(q, limit = 15) {
    return api.get(`/api/inventory/autocomplete?q=${encodeURIComponent(q)}&limit=${limit}`);
  },
  inventorySearch(q, whCd = "", baseDate = "") {
    const params = new URLSearchParams({ q });
    if (whCd) params.append("wh_cd", whCd);
    if (baseDate) params.append("base_date", baseDate);
    return api.get(`/api/inventory/search?${params}`);
  },
  inventoryCheck(prodCd, whCd = "", baseDate = "") {
    return api.post("/api/inventory/check", { prod_cd: prodCd, wh_cd: whCd, base_date: baseDate });
  },

  // ── 오더리스트 ──
  orderlistSync(tab = "") {
    return api.post(`/api/orderlist/sync${tab ? '?tab=' + encodeURIComponent(tab) : ''}`);
  },
  orderlistData(query = "", tab = "", page = 1, pageSize = 50) {
    const params = new URLSearchParams({ page, page_size: pageSize });
    if (tab) params.set("tab", tab);
    if (query) params.set("query", query);
    return api.get(`/api/orderlist/data?${params}`);
  },
  orderlistTabs() {
    return api.get("/api/orderlist/tabs");
  },
  orderlistAutocomplete(q, limit = 15) {
    return api.get(`/api/orderlist/autocomplete?q=${encodeURIComponent(q)}&limit=${limit}`);
  },
  orderlistSummary() {
    return api.get("/api/orderlist/summary");
  },

  // ── AI 대시보드 ──
  dashboardStats(days = 30) {
    return api.get(`/api/dashboard/stats?days=${days}`);
  },
  dashboardThreshold(custCode) {
    return api.get(`/api/dashboard/threshold/${encodeURIComponent(custCode)}`);
  },

  // ── 설정 ──
  getModels:    () => api.get("/api/settings/models"),
  setModel:     (modelId) => api.post("/api/settings/models", { model_id: modelId }),
  getSettings:  () => api.get("/api/settings/"),

  // ── 자료/오더리스트 자동 동기화 스케줄 ──
  syncScheduleGet: () => api.get("/api/settings/sync-schedule"),
  syncScheduleSet: (hour, minute, enabled) => api.request("PUT", "/api/settings/sync-schedule", { hour, minute, enabled }),
  syncScheduleRunNow: () => api.post("/api/settings/sync-schedule/run-now"),

  // ── 활동 로그 (관리자) ──
  activityLogs(page = 1, pageSize = 50, empCd = "", action = "", dateFrom = "", dateTo = "") {
    const params = new URLSearchParams({ page, page_size: pageSize });
    if (empCd) params.set("emp_cd", empCd);
    if (action) params.set("action", action);
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);
    return api.get(`/api/activity/logs?${params}`);
  },
  activitySummary() {
    return api.get("/api/activity/summary");
  },
  activityEmployees() {
    return api.get("/api/activity/employees");
  },

  // ── 택배 발송 조회 ──
  shippingSearch(q = "", date = "", warehouse = "", page = 1, pageSize = 50) {
    const params = new URLSearchParams({ page, page_size: pageSize });
    if (q) params.set("q", q);
    if (date) params.set("date", date);
    if (warehouse) params.set("warehouse", warehouse);
    return api.get(`/api/shipping/search?${params}`);
  },
  shippingDaily(date, warehouse = "") {
    const params = new URLSearchParams({ date });
    if (warehouse) params.set("warehouse", warehouse);
    return api.get(`/api/shipping/daily?${params}`);
  },
  shippingTrack(slipNos) {
    return api.post("/api/shipping/track", { slip_nos: slipNos });
  },
  shippingRegister(data) {
    return api.post("/api/shipping/register", data);
  },
  shippingRegisterBulk(items) {
    return api.post("/api/shipping/register-bulk", { items });
  },
  shippingUploadExcel(formData) {
    return api.postForm("/api/shipping/upload-excel", formData);
  },
  shippingStats() {
    return api.get("/api/shipping/stats");
  },
  shippingSync(slipNos) {
    return api.post("/api/shipping/sync", { slip_nos: slipNos });
  },
  shippingAutoFetch(warehouse = "", fromDate = "", toDate = "", days = 7) {
    const params = new URLSearchParams({ days });
    if (warehouse) params.set("warehouse", warehouse);
    if (fromDate) params.set("from_date", fromDate);
    if (toDate) params.set("to_date", toDate);
    return api.post(`/api/shipping/auto-fetch?${params}`);
  },
  shippingDelete(id) {
    return api.request("DELETE", `/api/shipping/${id}`);
  },


  // ── 구매입력 ──
  processPurchase:    (body) => api.post("/api/purchases/process", body),
  confirmPurchase:    (body) => api.post("/api/purchases/confirm", body),
  submitPurchaseERP:  (orderId, empCd = "") => api.post(`/api/purchases/submit-erp/${orderId}?emp_cd=${encodeURIComponent(empCd)}`),
  listPurchases:      (limit = 20) => api.get(`/api/purchases/list?limit=${limit}`),
  getPurchase:        (orderId) => api.get(`/api/purchases/${orderId}`),
  async processPurchaseImage(formData) {
    const headers = {};
    const token = this.getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;

    const res = await fetch(API_BASE + "/api/purchases/process-image", {
      method: "POST",
      body: formData,
      headers,
    });
    if (res.status === 401) {
      this.clearToken();
      if (typeof window.onAuthRequired === "function") window.onAuthRequired();
      throw new Error("인증이 만료되었습니다.");
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || "OCR 처리 오류");
    }
    return res.json();
  },
};
