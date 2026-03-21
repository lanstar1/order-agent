/**
 * Super Agent — 프론트엔드 (바닐라 JS)
 * AI 업무 자동화 에이전트: 프롬프트 → 분석 → 문서 생성
 */

// ─── 상태 ───
const SA = {
  ws: null,
  currentJobId: null,
  history: [],
};

// ─── 초기화 ───
function initSuperAgent() {
  loadSuperAgentHistory();
  loadSA2Stats();
}

// ─── 통계 로드 ───
async function loadSA2Stats() {
  const el = document.getElementById("sa2-stats-bar");
  if (!el) return;

  try {
    const token = localStorage.getItem("order_agent_token");
    const resp = await fetch("/api/super-agent/stats", {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (!resp.ok) return;
    const data = await resp.json();
    const cs = data.cost_summary || {};
    const js = data.job_stats || {};

    el.innerHTML = `
      <span title="총 작업 수">📋 ${js.total || 0}건</span>
      <span title="총 비용" style="margin-left:16px">💰 $${(cs.total_cost || 0).toFixed(3)}</span>
      <span title="총 토큰" style="margin-left:16px">🔤 ${(cs.total_tokens || 0).toLocaleString()}</span>
      <span title="평균 비용" style="margin-left:16px">📊 평균 $${(cs.avg_cost_per_job || 0).toFixed(4)}/건</span>
    `;
  } catch {}
}

// ─── Job 생성 (프롬프트 + 파일) ───
async function submitSuperAgentJob() {
  const promptEl = document.getElementById("sa2-prompt");
  const fileEl = document.getElementById("sa2-file");
  const delivTypeEl = document.getElementById("sa2-deliverable-type");

  const prompt = promptEl?.value?.trim();
  if (!prompt) {
    toast("요청 내용을 입력해주세요.", "warning");
    return;
  }

  const deliverableType = delivTypeEl?.value || "report";

  // FormData 구성
  const fd = new FormData();
  fd.append("prompt", prompt);
  fd.append("deliverable_type", deliverableType);
  if (fileEl?.files?.[0]) {
    fd.append("file", fileEl.files[0]);
  }

  // UI: 실행 시작
  showSA2Progress();
  updateSA2Progress(0, "작업 제출 중...");

  try {
    const token = localStorage.getItem("order_agent_token");
    const resp = await fetch("/api/super-agent/jobs", {
      method: "POST",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: fd,
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${resp.status}`);
    }

    const data = await resp.json();
    SA.currentJobId = data.job_id;

    updateSA2Progress(5, "작업이 시작되었습니다...");

    // WebSocket 연결
    connectSA2WebSocket(data.job_id);

    // 프롬프트 초기화
    promptEl.value = "";
    if (fileEl) fileEl.value = "";

  } catch (e) {
    toast(`작업 생성 실패: ${e.message}`, "error");
    hideSA2Progress();
  }
}

// ─── WebSocket 연결 ───
function connectSA2WebSocket(jobId) {
  if (SA.ws) {
    try { SA.ws.close(); } catch {}
  }

  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${location.host}/api/super-agent/ws/${jobId}`;

  SA.ws = new WebSocket(wsUrl);

  SA.ws.onopen = () => {
    console.log("[SA-WS] Connected:", jobId);
    // 30초마다 ping
    SA._pingInterval = setInterval(() => {
      if (SA.ws?.readyState === WebSocket.OPEN) {
        SA.ws.send("ping");
      }
    }, 30000);
  };

  SA.ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleSA2WSMessage(msg);
    } catch {}
  };

  SA.ws.onclose = () => {
    clearInterval(SA._pingInterval);
    console.log("[SA-WS] Disconnected");
  };

  SA.ws.onerror = (err) => {
    console.error("[SA-WS] Error:", err);
  };
}

// ─── WebSocket 메시지 처리 ───
function handleSA2WSMessage(msg) {
  const type = msg.type;

  if (type === "pong") return;

  if (type === "progress" || type === "task_update") {
    updateSA2Progress(msg.progress_pct || 0, msg.message || "");

    // 태스크 상태 업데이트
    if (msg.task_key) {
      updateSA2TaskStatus(msg.task_key, msg.status);
    }
  }

  if (type === "completed") {
    updateSA2Progress(100, "작업이 완료되었습니다!");
    showSA2Result(msg.data);
    loadSuperAgentHistory();
  }

  if (type === "error") {
    updateSA2Progress(0, `오류: ${msg.message}`);
    const bar = document.getElementById("sa2-progress-fill");
    if (bar) bar.style.background = "#ef4444";
    toast(`작업 실패: ${msg.message}`, "error");
  }
}

// ─── 진행 상황 UI ───
function showSA2Progress() {
  const area = document.getElementById("sa2-progress-area");
  const resultArea = document.getElementById("sa2-result-area");
  if (area) area.style.display = "block";
  if (resultArea) resultArea.style.display = "none";
}

function hideSA2Progress() {
  const area = document.getElementById("sa2-progress-area");
  if (area) area.style.display = "none";
}

function updateSA2Progress(pct, msg) {
  const fill = document.getElementById("sa2-progress-fill");
  const text = document.getElementById("sa2-progress-text");
  const status = document.getElementById("sa2-status-text");

  if (fill) {
    fill.style.width = `${pct}%`;
    fill.style.background = pct === 100
      ? "linear-gradient(90deg, #10b981, #059669)"
      : "linear-gradient(90deg, #2563eb, #10b981)";
  }
  if (text) text.textContent = `${pct}%`;
  if (status) status.textContent = msg;
}

function updateSA2TaskStatus(taskKey, status) {
  const el = document.getElementById(`sa2-task-${taskKey}`);
  if (!el) return;

  const icon = el.querySelector(".task-icon");
  if (icon) {
    if (status === "running") icon.textContent = "⏳";
    else if (status === "succeeded") icon.textContent = "✅";
    else if (status === "failed") icon.textContent = "❌";
  }
}

// ─── 결과 표시 ───
function showSA2Result(data) {
  const resultArea = document.getElementById("sa2-result-area");
  if (!resultArea) return;

  resultArea.style.display = "block";

  // 다운로드 링크
  const downloadBtn = document.getElementById("sa2-download-btn");
  if (downloadBtn && SA.currentJobId) {
    downloadBtn.href = `/api/super-agent/jobs/${SA.currentJobId}/download`;
    downloadBtn.style.display = "inline-flex";
  }

  // 비용 정보
  const costEl = document.getElementById("sa2-cost-info");
  if (costEl && data?.cost_summary) {
    const cs = data.cost_summary;
    costEl.innerHTML = `
      <span>💰 비용: $${cs.total_cost?.toFixed(4) || "0"}</span>
      <span style="margin-left:12px">⏱️ ${((cs.elapsed_ms || 0) / 1000).toFixed(1)}초</span>
      <span style="margin-left:12px">🔤 ${(cs.total_tokens || 0).toLocaleString()} 토큰</span>
    `;
  }

  // 미리보기 로드
  loadSA2Preview();
}

async function loadSA2Preview() {
  if (!SA.currentJobId) return;

  try {
    const token = localStorage.getItem("order_agent_token");
    const resp = await fetch(`/api/super-agent/jobs/${SA.currentJobId}/preview`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (!resp.ok) return;

    const data = await resp.json();
    const previewEl = document.getElementById("sa2-preview-content");
    if (!previewEl) return;

    if (data.preview_type === "structured" && data.data) {
      const d = data.data;
      let html = `<h3 style="margin:0 0 12px;font-size:16px;color:#111827">${d.title || "보고서"}</h3>`;
      if (d.executive_summary) {
        html += `<div style="background:#f0f9ff;border-left:3px solid #2563eb;padding:12px 16px;margin-bottom:16px;font-size:14px;color:#1e40af;line-height:1.6">${d.executive_summary}</div>`;
      }
      (d.sections || []).forEach((s) => {
        html += `<h4 style="margin:16px 0 8px;font-size:14px;color:#374151">${s.heading || ""}</h4>`;
        const body = s.body;
        if (Array.isArray(body)) {
          html += "<ul style='margin:0;padding-left:20px'>" +
            body.map(b => `<li style="font-size:13px;color:#4b5563;margin-bottom:4px">${b}</li>`).join("") +
            "</ul>";
        } else {
          html += `<p style="font-size:13px;color:#4b5563;line-height:1.6;white-space:pre-wrap">${body || ""}</p>`;
        }
      });
      previewEl.innerHTML = html;
    } else {
      // 텍스트 미리보기
      const txt = data.data?.content || data.data?.summary || "";
      previewEl.innerHTML = `<pre style="white-space:pre-wrap;font-size:13px;color:#374151;line-height:1.6">${txt}</pre>`;
    }
  } catch (e) {
    console.error("[SA2] Preview error:", e);
  }
}

// ─── 히스토리 ───
async function loadSuperAgentHistory() {
  try {
    const token = localStorage.getItem("order_agent_token");
    const resp = await fetch("/api/super-agent/jobs?limit=20", {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (!resp.ok) return;

    const data = await resp.json();
    SA.history = data.items || [];
    renderSA2History();
  } catch {}
}

function renderSA2History() {
  const listEl = document.getElementById("sa2-history-list");
  if (!listEl) return;

  if (!SA.history.length) {
    listEl.innerHTML = '<div style="text-align:center;padding:20px;color:#9ca3af;font-size:13px">아직 작업 이력이 없습니다</div>';
    return;
  }

  listEl.innerHTML = SA.history.map(j => {
    const statusIcon = j.status === "completed" ? "✅"
      : j.status === "running" ? "⏳"
      : j.status === "failed" ? "❌" : "⏸️";

    const dt = j.created_at ? new Date(j.created_at).toLocaleString("ko-KR") : "";
    const jobType = j.job_type || "";

    return `
      <div class="sa2-history-item" onclick="viewSA2Job('${j.job_id}')" style="padding:10px 14px;border-bottom:1px solid #f3f4f6;cursor:pointer;transition:background .15s">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div>
            <span style="margin-right:6px">${statusIcon}</span>
            <span style="font-size:13px;font-weight:500;color:#111827">${j.title || "작업"}</span>
            ${jobType ? `<span style="margin-left:8px;font-size:11px;padding:2px 8px;background:#f3f4f6;color:#6b7280;border-radius:4px">${jobType}</span>` : ""}
          </div>
          <span style="font-size:11px;color:#9ca3af">${dt}</span>
        </div>
      </div>
    `;
  }).join("");
}

async function viewSA2Job(jobId) {
  SA.currentJobId = jobId;

  try {
    const token = localStorage.getItem("order_agent_token");
    const resp = await fetch(`/api/super-agent/jobs/${jobId}/result`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (!resp.ok) return;

    const data = await resp.json();

    if (data.status === "completed") {
      showSA2Progress();
      updateSA2Progress(100, "완료된 작업");
      showSA2Result({
        artifact: data.artifact,
        cost_summary: data.cost_summary,
      });
    } else if (data.status === "running") {
      showSA2Progress();
      updateSA2Progress(50, "실행 중...");
      connectSA2WebSocket(jobId);
    }
  } catch (e) {
    toast("작업 조회 실패", "error");
  }
}

// ─── 파일 드래그앤드롭 ───
function initSA2Dropzone() {
  const dropzone = document.getElementById("sa2-dropzone");
  const fileInput = document.getElementById("sa2-file");

  if (!dropzone || !fileInput) return;

  dropzone.addEventListener("click", () => fileInput.click());

  dropzone.addEventListener("dragover", (e) => {
    e.preventDefault();
    dropzone.classList.add("dragover");
  });

  dropzone.addEventListener("dragleave", () => {
    dropzone.classList.remove("dragover");
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    dropzone.classList.remove("dragover");
    if (e.dataTransfer?.files?.length) {
      fileInput.files = e.dataTransfer.files;
      updateSA2FileLabel(fileInput.files[0]);
    }
  });

  fileInput.addEventListener("change", () => {
    if (fileInput.files?.[0]) {
      updateSA2FileLabel(fileInput.files[0]);
    }
  });
}

function updateSA2FileLabel(file) {
  const label = document.getElementById("sa2-file-label");
  if (!label) return;

  const sizeKB = (file.size / 1024).toFixed(1);
  label.innerHTML = `📎 ${file.name} <span style="color:#9ca3af">(${sizeKB} KB)</span>`;
  label.style.display = "block";
}

// ─── 템플릿 로드 및 실행 ───
async function loadSA2Templates() {
  const container = document.getElementById("sa2-templates-grid");
  if (!container) return;

  try {
    const token = localStorage.getItem("order_agent_token");
    const resp = await fetch("/api/super-agent/templates", {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (!resp.ok) return;

    const data = await resp.json();
    const templates = data.templates || [];

    container.innerHTML = templates.map(t => `
      <div class="sa2-template-card" onclick="useSA2Template('${t.id}', '${t.prompt.replace(/'/g, "\\'")}', '${t.deliverable_type}')" title="${t.description}">
        <div style="font-size:24px;margin-bottom:6px">${t.icon}</div>
        <div style="font-size:12px;font-weight:600;color:#111827;margin-bottom:2px">${t.title}</div>
        <div style="font-size:11px;color:#9ca3af">${t.category}</div>
      </div>
    `).join("");
  } catch {}
}

function useSA2Template(templateId, prompt, delivType) {
  const promptEl = document.getElementById("sa2-prompt");
  const typeEl = document.getElementById("sa2-deliverable-type");
  if (promptEl) promptEl.value = prompt;
  if (typeEl) typeEl.value = delivType;
  promptEl?.focus();
}

// ─── 페이지 진입 시 초기화 ───
document.addEventListener("DOMContentLoaded", () => {
  const navItem = document.querySelector('[data-page="super_agent"]');
  if (navItem) {
    navItem.addEventListener("click", () => {
      setTimeout(() => {
        initSA2Dropzone();
        loadSuperAgentHistory();
        loadSA2Templates();
        loadSA2Stats();
      }, 100);
    });
  }
});
