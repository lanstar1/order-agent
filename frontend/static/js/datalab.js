/**
 * 네이버 트렌드 분석 관리자 콘솔 - 메인 애플리케이션
 * Vanilla JavaScript SPA with full feature parity to React version
 * Modified for integration into order-agent SPA
 */

(function() {
  'use strict';

  // Constants
  const API_BASE = '';
  const TREND_MONTHLY_START_PERIOD = '202001';
  const TREND_RESULT_COUNT_OPTIONS = [20, 40];

  const DEVICE_OPTIONS = [
    ['pc', 'PC'],
    ['mo', '모바일']
  ];

  const GENDER_OPTIONS = [
    ['f', '여성'],
    ['m', '남성']
  ];

  const AGE_OPTIONS = [
    ['10', '10대'],
    ['20', '20대'],
    ['30', '30대'],
    ['40', '40대'],
    ['50', '50대'],
    ['60', '60대 이상']
  ];

  // State
  let state = {
    trendBoard: null,
    currentRun: null,
    level1Categories: [],
    level2Categories: [],
    level3Categories: [],
    form: {
      category1: '',
      category2: '',
      category3: '',
      devices: [],
      genders: [],
      ages: [],
      resultCount: 20,
      excludeBrandProducts: false,
      customExcludedTerms: ''
    },
    loading: true,
    refreshing: false,
    submitting: false,
    actionSubmitting: false,
    error: null,
    feedback: null,
    snapshotPanel: null,
    actionModal: null,
    selectedPlannerMonth: '01',
    selectedKeyword: null,
    heatmapMode: 'season',
    detailLoadingRunId: null,
    currentTab: 'dashboard',
    pollingInterval: null,
    analysisProfiles: []
  };

  // API module
  async function api(path, options = {}) {
    try {
      const response = await fetch(`${API_BASE}/api/trends${path}`, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...(options.headers || {})
        }
      });

      const text = await response.text();

      if (!response.ok) {
        return {
          ok: false,
          code: `HTTP_${response.status}`,
          message: buildApiErrorMessage(response.status, text)
        };
      }

      try {
        return JSON.parse(text);
      } catch {
        return { ok: true };
      }
    } catch (error) {
      return {
        ok: false,
        code: 'NETWORK_ERROR',
        message: error instanceof Error ? error.message : 'API 연결에 실패했습니다.'
      };
    }
  }

  function buildApiErrorMessage(status, responseText) {
    const compact = responseText.replace(/\s+/g, ' ').trim();
    if (status === 404) {
      return '트렌드 API 경로를 찾지 못했습니다.';
    }
    return `트렌드 API 요청이 실패했습니다. (${status}) ${compact.slice(0, 140)}`;
  }

  // Utility functions
  function formatDate(iso) {
    if (!iso) return '-';
    const date = new Date(iso);
    return new Intl.DateTimeFormat('ko-KR', {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    }).format(date);
  }

  function formatNumber(n) {
    return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  function getStatusColor(status) {
    const colors = {
      'completed': '#10b981',
      'running': '#3b82f6',
      'queued': '#f59e0b',
      'cancelled': '#6b7280',
      'failed': '#ef4444'
    };
    return colors[status] || '#6b7280';
  }

  function getStatusLabel(status) {
    const labels = {
      'running': '수집 중',
      'queued': '대기',
      'completed': '완료',
      'cancelled': '중지됨',
      'failed': '실패'
    };
    return labels[status] || status;
  }

  function debounce(fn, ms) {
    let timeoutId = null;
    return function(...args) {
      if (timeoutId) clearTimeout(timeoutId);
      timeoutId = setTimeout(() => fn(...args), ms);
    };
  }

  function toggleValue(arr, target) {
    return arr.includes(target)
      ? arr.filter(v => v !== target)
      : [...arr, target];
  }

  // Toast notifications
  function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);

    setTimeout(() => {
      toast.remove();
    }, 3000);
  }

  // DOM manipulation
  function setErrorBanner(message) {
    const banner = document.getElementById('error-banner');
    const text = document.getElementById('error-text');
    if (message) {
      text.textContent = message;
      banner.style.display = 'block';
    } else {
      banner.style.display = 'none';
    }
    state.error = message;
  }

  function setFeedbackBanner(message, tone = 'info') {
    const banner = document.getElementById('feedback-banner');
    const text = document.getElementById('feedback-text');
    banner.className = `banner banner-${tone}`;
    text.textContent = message;
    banner.style.display = 'block';
    state.feedback = { tone, text: message };
  }

  function hideFeedbackBanner() {
    const banner = document.getElementById('feedback-banner');
    banner.style.display = 'none';
    state.feedback = null;
  }

  function showLoadingOverlay(text = '로딩 중...') {
    const overlay = document.getElementById('loading-overlay');
    const loadingText = document.getElementById('loading-text');
    loadingText.textContent = text;
    overlay.style.display = 'block';
  }

  function hideLoadingOverlay() {
    const overlay = document.getElementById('loading-overlay');
    overlay.style.display = 'none';
  }

  function showActionModal(title, description, confirmText, action) {
    const modal = document.getElementById('action-modal');
    document.getElementById('modal-title').textContent = title;
    document.getElementById('modal-description').textContent = description;

    const confirmBtn = document.getElementById('modal-confirm-btn');
    confirmBtn.textContent = confirmText;
    confirmBtn.onclick = action;

    modal.style.display = 'flex';
  }

  function hideActionModal() {
    const modal = document.getElementById('action-modal');
    modal.style.display = 'none';
  }

  function switchTab(tabName) {
    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
      tab.classList.remove('tab-content-active');
    });

    // Deactivate all buttons
    document.querySelectorAll('.tab-button').forEach(btn => {
      btn.classList.remove('tab-button-active');
    });

    // Show selected tab
    const tabElement = document.getElementById(tabName);
    if (tabElement) {
      tabElement.classList.add('tab-content-active');
    }

    // Activate button
    const button = document.querySelector(`[data-tab="${tabName}"]`);
    if (button) {
      button.classList.add('tab-button-active');
    }

    state.currentTab = tabName;
  }

  // Dashboard functions
  async function loadDashboard() {
    const response = await api('/admin/board');
    if (response.ok) {
      state.trendBoard = response.board;
      state.currentRun = response.board.runs[0] || null;
      updateDashboardUI();
    } else {
      setErrorBanner(response.message || '대시보드 데이터를 불러오지 못했습니다.');
    }
  }

  function updateDashboardUI() {
    if (!state.trendBoard) return;

    const board = state.trendBoard;
    const stats = document.querySelectorAll('.stat-value');
    if (stats[0]) stats[0].textContent = board.runs.filter(r => r.status === 'running').length;
    if (stats[1]) stats[1].textContent = board.runs.length;
    if (stats[2]) stats[2].textContent = formatNumber(board.metrics?.totalSnapshots || 0);
    if (stats[3]) stats[3].textContent = formatNumber(board.metrics?.failedTasks || 0);

    // Update recent profiles
    updateRecentProfilesList();
  }

  function updateRecentProfilesList() {
    const list = document.getElementById('recent-profiles-list');
    if (!state.trendBoard || !state.trendBoard.profiles.length) {
      list.innerHTML = '<p class="empty-state">아직 프로필이 없습니다.</p>';
      return;
    }

    list.innerHTML = state.trendBoard.profiles.slice(0, 5).map(profile => `
      <div class="profile-item">
        <strong>${profile.name}</strong>
        <span>${profile.categoryPath}</span>
        <span class="status-badge" style="background-color: ${getStatusColor(profile.status)};">${getStatusLabel(profile.status)}</span>
      </div>
    `).join('');
  }

  // Category loading
  async function loadCategories(parentId = '0') {
    const response = await api(`/categories/${parentId}`);
    if (response.ok && response.nodes && response.nodes.length) {
      return response.nodes;
    }
    // Fallback: return hardcoded categories
    return parentId === '0' ? getDefaultRootCategories() : [];
  }

  function getDefaultRootCategories() {
    return [
      { cid: '1', name: '디지털/가전', fullPath: '디지털/가전', level: 1 },
      { cid: '2', name: '패션/뷰티', fullPath: '패션/뷰티', level: 1 },
      { cid: '3', name: '식품/음료', fullPath: '식품/음료', level: 1 },
      { cid: '4', name: '생활/건강', fullPath: '생활/건강', level: 1 }
    ];
  }

  async function initializeCategories() {
    state.level1Categories = await loadCategories('0');
    updateCategorySelects();
  }

  function updateCategorySelects() {
    const select1 = document.getElementById('category1');
    select1.innerHTML = '<option value="">선택</option>' +
      state.level1Categories.map(cat => `<option value="${cat.cid}">${cat.name}</option>`).join('');
  }

  // Profile Management
  async function handleCategory1Change() {
    const value = document.getElementById('category1').value;
    state.form.category1 = value;
    state.form.category2 = '';
    state.form.category3 = '';

    if (!value) {
      state.level2Categories = [];
      state.level3Categories = [];
      updateCategory2Select();
      updateCategory3Select();
      return;
    }

    state.level2Categories = await loadCategories(value);
    updateCategory2Select();
  }

  function updateCategory2Select() {
    const select2 = document.getElementById('category2');
    select2.disabled = !state.form.category1;
    select2.innerHTML = '<option value="">선택</option>' +
      state.level2Categories.map(cat => `<option value="${cat.cid}">${cat.name}</option>`).join('');
  }

  async function handleCategory2Change() {
    const value = document.getElementById('category2').value;
    state.form.category2 = value;
    state.form.category3 = '';

    if (!value) {
      state.level3Categories = [];
      updateCategory3Select();
      return;
    }

    state.level3Categories = await loadCategories(value);
    updateCategory3Select();
  }

  function updateCategory3Select() {
    const select3 = document.getElementById('category3');
    select3.disabled = !state.form.category2;
    select3.innerHTML = '<option value="">선택</option>' +
      state.level3Categories.map(cat => `<option value="${cat.cid}">${cat.name}</option>`).join('');
  }

  // Filter chips
  function initializeFilterChips() {
    document.querySelectorAll('.chip-button').forEach(btn => {
      btn.addEventListener('click', (e) => {
        const filter = btn.dataset.filter;
        const value = btn.dataset.value;

        if (filter === 'device') {
          state.form.devices = toggleValue(state.form.devices, value);
        } else if (filter === 'gender') {
          state.form.genders = toggleValue(state.form.genders, value);
        } else if (filter === 'age') {
          state.form.ages = toggleValue(state.form.ages, value);
        }

        updateChipUI();
      });
    });

    // Result count segmented buttons
    document.querySelectorAll('[data-result-count]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        state.form.resultCount = parseInt(e.target.dataset.resultCount);
        document.querySelectorAll('[data-result-count]').forEach(b => {
          b.classList.remove('segmented-button-active');
        });
        e.target.classList.add('segmented-button-active');
      });
    });

    // Brand exclusion checkbox
    const excludeBrandsCheck = document.getElementById('exclude-brands');
    excludeBrandsCheck.addEventListener('change', (e) => {
      state.form.excludeBrandProducts = e.target.checked;
      const field = document.getElementById('custom-exclude-field');
      field.style.display = e.target.checked ? 'block' : 'none';
    });

    // Custom excluded terms
    const customTerms = document.getElementById('custom-excluded-terms');
    customTerms.addEventListener('change', (e) => {
      state.form.customExcludedTerms = e.target.value;
    });
  }

  function updateChipUI() {
    document.querySelectorAll('.chip-button').forEach(btn => {
      const filter = btn.dataset.filter;
      const value = btn.dataset.value;
      let isActive = false;

      if (filter === 'device' && state.form.devices.includes(value)) isActive = true;
      if (filter === 'gender' && state.form.genders.includes(value)) isActive = true;
      if (filter === 'age' && state.form.ages.includes(value)) isActive = true;

      if (isActive) {
        btn.classList.add('chip-button-active');
      } else {
        btn.classList.remove('chip-button-active');
      }
    });
  }

  // Start collection
  async function handleStartCollection() {
    const selected = state.form.category3 || state.form.category2 || state.form.category1;
    if (!selected) {
      setErrorBanner('먼저 카테고리를 선택해 주세요.');
      return;
    }

    state.submitting = true;
    setFeedbackBanner('조건을 저장하고 데이터 취합을 시작하고 있습니다.', 'info');

    const selectedCategory = state.level3Categories.find(c => c.cid === state.form.category3) ||
                           state.level2Categories.find(c => c.cid === state.form.category2) ||
                           state.level1Categories.find(c => c.cid === state.form.category1);

    const response = await api('/collect', {
      method: 'POST',
      body: JSON.stringify({
        name: buildAnalysisRequestName(selectedCategory?.fullPath || '분석'),
        categoryCid: selectedCategory?.cid,
        categoryPath: selectedCategory?.fullPath,
        categoryDepth: selectedCategory?.level || 1,
        timeUnit: 'month',
        devices: state.form.devices,
        genders: state.form.genders,
        ages: state.form.ages,
        resultCount: state.form.resultCount,
        excludeBrandProducts: state.form.excludeBrandProducts,
        customExcludedTerms: state.form.customExcludedTerms.split(',').map(s => s.trim()).filter(Boolean)
      })
    });

    state.submitting = false;

    if (!response.ok) {
      setFeedbackBanner(response.message || '분석 시작에 실패했습니다.', 'error');
      return;
    }

    state.currentRun = response.run;
    if (state.trendBoard) {
      state.trendBoard.runs = [response.run, ...state.trendBoard.runs.filter(r => r.id !== response.run.id)].slice(0, 8);
    }

    setFeedbackBanner('데이터 취합을 시작했습니다. 오른쪽 패널에서 진행 상황을 확인해 주세요.', 'success');
    setupPolling();
  }

  function buildAnalysisRequestName(categoryPath, resultCount = null) {
    const parts = [categoryPath];
    const rc = resultCount || state.form.resultCount;
    parts.push(`Top${rc}`);
    if (state.form.excludeBrandProducts) {
      parts.push('브랜드 제외');
    }
    return parts.join(' · ');
  }

  // Collection Status
  async function loadRuns() {
    const response = await api('/admin/board');
    if (response.ok) {
      state.trendBoard = response.board;
      if (response.board.runs.length) {
        state.currentRun = response.board.runs[0];
      }
      updateRunsUI();
    }
  }

  function updateRunsUI() {
    if (!state.currentRun) {
      document.getElementById('tasks-list').innerHTML = '<p class="empty-state">진행 중인 작업이 없습니다.</p>';
      return;
    }

    const run = state.currentRun;
    const progressPercent = run.totalTasks ? Math.round((run.completedTasks / run.totalTasks) * 100) : 0;

    // Update progress bar
    const progressFill = document.querySelector('.progress-fill');
    if (progressFill) progressFill.style.width = `${progressPercent}%`;

    // Update progress stats
    const stats = document.querySelectorAll('.progress-stat-value');
    if (stats[0]) stats[0].textContent = run.currentPeriod || run.latestCompletedPeriod || '대기';
    if (stats[1]) stats[1].textContent = `${run.completedTasks}/${run.totalTasks}`;
    if (stats[2]) stats[2].textContent = run.currentPage || '1/1';
    if (stats[3]) stats[3].textContent = formatETA(run.etaMinutes);
    if (stats[4]) stats[4].textContent = formatDate(run.estimatedCompletionAt);
    if (stats[5]) stats[5].textContent = run.latestCompletedPeriod || '대기';

    // Update tasks list
    const tasksList = document.getElementById('tasks-list');
    if (run.tasks && run.tasks.length) {
      tasksList.innerHTML = run.tasks.map(task => `
        <div class="task-item">
          <span>${task.period}</span>
          <span class="status-badge" style="background-color: ${getStatusColor(task.status)};">${getStatusLabel(task.status)}</span>
        </div>
      `).join('');
    }
  }

  function formatETA(minutes) {
    if (!minutes) return '계산 중';
    if (minutes < 60) return `${minutes}분`;
    const hours = Math.floor(minutes / 60);
    const remain = minutes % 60;
    return remain ? `${hours}시간 ${remain}분` : `${hours}시간`;
  }

  async function handleCancelRun() {
    if (!state.currentRun) return;

    showActionModal(
      '데이터 취합을 중지할까요?',
      '현재 진행 중인 취합을 완전히 취소합니다. 이미 끝난 월 데이터는 그대로 유지됩니다.',
      '중지 확인',
      async () => {
        hideActionModal();
        state.actionSubmitting = true;

        const response = await api(`/runs/${state.currentRun.id}/cancel`, {
          method: 'POST'
        });

        state.actionSubmitting = false;

        if (response.ok) {
          setFeedbackBanner('데이터 취합을 중지했습니다.', 'success');
          await loadRuns();
        } else {
          setFeedbackBanner(response.message || '취합 중지에 실패했습니다.', 'error');
        }
      }
    );
  }

  async function handleDeleteRun() {
    if (!state.currentRun) return;

    showActionModal(
      '현재 데이터 취합 런을 삭제할까요?',
      '현재 런과 미완료 데이터만 지우고, 이미 완성된 월 캐시는 남겨둡니다.',
      '삭제 확인',
      async () => {
        hideActionModal();
        state.actionSubmitting = true;

        const response = await api(`/runs/${state.currentRun.id}`, {
          method: 'DELETE'
        });

        state.actionSubmitting = false;

        if (response.ok) {
          state.currentRun = null;
          setFeedbackBanner('데이터 취합 런을 삭제했습니다.', 'success');
          await loadRuns();
        } else {
          setFeedbackBanner(response.message || '삭제에 실패했습니다.', 'error');
        }
      }
    );
  }

  // Polling system
  function setupPolling() {
    if (state.pollingInterval) clearInterval(state.pollingInterval);

    const isActive = state.currentRun?.status === 'running' || state.currentRun?.status === 'queued';
    const interval = isActive ? 5000 : 12000;

    state.pollingInterval = setInterval(() => {
      loadRuns();
    }, interval);
  }

  // Analysis tab
  async function loadAnalysisProfiles() {
    const response = await api('/profiles');
    if (response.ok && response.profiles) {
      state.analysisProfiles = response.profiles;
      updateAnalysisProfileSelector();
    }
  }

  function updateAnalysisProfileSelector() {
    const selector = document.getElementById('analysis-profile-selector');
    selector.innerHTML = '<option value="">프로필 선택</option>' +
      state.analysisProfiles.map(p => `<option value="${p.id}">${p.name}</option>`).join('');
  }

  async function handleTriggerAnalysis() {
    const profileId = document.getElementById('analysis-profile-selector').value;
    if (!profileId) {
      setErrorBanner('먼저 프로필을 선택해 주세요.');
      return;
    }

    showLoadingOverlay('분석을 실행 중입니다...');

    const response = await api(`/profiles/${profileId}/analysis`);

    hideLoadingOverlay();

    if (!response.ok) {
      setErrorBanner(response.message || '분석 실행에 실패했습니다.');
      return;
    }

    renderAnalysisResults(response.analysis);
  }

  function renderAnalysisResults(analysis) {
    const container = document.getElementById('analysis-results');

    if (!analysis) {
      container.innerHTML = '<p class="empty-state">분석 결과가 없습니다.</p>';
      return;
    }

    let html = '';

    // Hero metrics
    if (analysis.heroMetrics && analysis.heroMetrics.length) {
      html += '<div class="hero-metric-grid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin: 1rem 0;">';
      analysis.heroMetrics.forEach(metric => {
        html += `
          <div class="hero-metric-card" style="padding: 1rem; border: 1px solid #e5e7eb; border-radius: 0.5rem;">
            <div style="font-weight: bold; margin-bottom: 0.5rem;">${metric.label}</div>
            <div style="font-size: 1.25rem; font-weight: bold; color: #1f2937; margin-bottom: 0.5rem;">${metric.keyword}</div>
            <div style="font-size: 0.875rem; color: #6b7280;">${metric.rationale}</div>
            <div style="margin-top: 0.5rem; padding-top: 0.5rem; border-top: 1px solid #f3f4f6; font-size: 0.75rem; color: #9ca3af;">
              신뢰도 ${metric.confidence}
            </div>
          </div>
        `;
      });
      html += '</div>';
    }

    // Analysis cards
    if (analysis.analysisCards && analysis.analysisCards.length) {
      html += '<div style="margin: 1rem 0;">';
      analysis.analysisCards.forEach(card => {
        html += `
          <div style="padding: 1rem; border: 1px solid #e5e7eb; border-radius: 0.5rem; margin-bottom: 1rem;">
            <h3 style="font-weight: bold; margin-bottom: 0.5rem;">${card.title}</h3>
            <p style="font-size: 0.875rem; color: #6b7280; margin-bottom: 1rem;">${card.description}</p>
            ${card.items.length ? `
              <div style="display: grid; gap: 0.5rem;">
                ${card.items.slice(0, 4).map(item => `
                  <div style="padding: 0.5rem; background-color: #f9fafb; border-radius: 0.25rem;">
                    <strong>${item.keyword}</strong> - ${item.rationale}
                    <br/><small style="color: #9ca3af;">신뢰도 ${item.confidence}</small>
                  </div>
                `).join('')}
              </div>
            ` : '<p style="color: #9ca3af;">데이터가 충분하지 않습니다.</p>'}
          </div>
        `;
      });
      html += '</div>';
    }

    // Highlights
    if (analysis.highlights && analysis.highlights.length) {
      html += '<div style="margin: 1rem 0;">';
      html += '<h3 style="font-weight: bold; margin-bottom: 0.5rem;">주요 인사이트</h3>';
      html += '<ul style="list-style-position: inside; color: #6b7280;">';
      analysis.highlights.slice(0, 5).forEach(h => {
        html += `<li>${h}</li>`;
      });
      html += '</ul></div>';
    }

    container.innerHTML = html;
  }

  // Event listeners initialization
  function initializeEventListeners() {
    // Tab navigation
    document.querySelectorAll('.tab-button').forEach(btn => {
      btn.addEventListener('click', (e) => {
        switchTab(e.target.dataset.tab);
      });
    });

    // Category selects
    document.getElementById('category1').addEventListener('change', handleCategory1Change);
    document.getElementById('category2').addEventListener('change', handleCategory2Change);
    document.getElementById('category3').addEventListener('change', (e) => {
      state.form.category3 = e.target.value;
    });

    // Period selects
    document.getElementById('start-period').addEventListener('change', (e) => {
      state.form.startPeriod = e.target.value;
    });

    document.getElementById('end-period').addEventListener('change', (e) => {
      state.form.endPeriod = e.target.value;
    });

    // Start collection button
    document.getElementById('start-collection-btn').addEventListener('click', handleStartCollection);

    // Cancel/Delete run buttons
    document.getElementById('cancel-run-btn').addEventListener('click', handleCancelRun);
    document.getElementById('delete-run-btn').addEventListener('click', handleDeleteRun);

    // Analysis trigger button
    document.getElementById('trigger-analysis-btn').addEventListener('click', handleTriggerAnalysis);

    // Modal close
    document.getElementById('modal-cancel-btn').addEventListener('click', hideActionModal);

    // Filter chips initialization
    initializeFilterChips();
  }

  // Initialize
  async function initialize() {
    try {
      // Load initial data
      await Promise.all([
        loadDashboard(),
        initializeCategories(),
        loadAnalysisProfiles()
      ]);

      // Setup event listeners
      initializeEventListeners();

      // Setup polling
      setupPolling();

      state.loading = false;
      hideLoadingOverlay();

      setErrorBanner(null);
      showToast('애플리케이션이 시작되었습니다.', 'success');

    } catch (error) {
      console.error('Initialization error:', error);
      setErrorBanner('애플리케이션 초기화에 실패했습니다.');
      hideLoadingOverlay();
    }
  }

  // Expose the init function and flag to window
  window._datalabInitialized = false;
  window.initDatalab = function() {
    if (!window._datalabInitialized) {
      initialize();
      window._datalabInitialized = true;
    } else {
      // Just reload dashboard if already initialized
      loadDashboard();
    }
  };

  // Cleanup on unload
  window.addEventListener('beforeunload', () => {
    if (state.pollingInterval) {
      clearInterval(state.pollingInterval);
    }
  });

})();
