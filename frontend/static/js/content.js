/**
 * Content Factory — 프론트엔드 (바닐라 JS)
 *
 * app.js의 renderPage() 함수에서 case 'content': 로 호출.
 * api.js의 기존 패턴(JWT 토큰 자동 첨부)을 그대로 사용.
 *
 * 사용법: app.js에 이 파일의 함수들을 통합하거나,
 * <script src="/static/js/content.js?v=..."></script>로 분리 로드.
 */

// ============================================================
// 메인 렌더 함수 (app.js의 renderPage에서 호출)
// ============================================================

function renderContentPage() {
    const container = document.getElementById('page-content');
    container.innerHTML = `
        <div class="card" style="margin-bottom:16px">
            <h2 style="margin:0 0 12px">콘텐츠 팩토리</h2>
            <div style="display:flex;gap:8px;flex-wrap:wrap">
                <button class="btn btn-sm" onclick="contentTab('dashboard')" id="ct-btn-dashboard">대시보드</button>
                <button class="btn btn-sm" onclick="contentTab('writer')" id="ct-btn-writer">글쓰기</button>
                <button class="btn btn-sm" onclick="contentTab('cardnews')" id="ct-btn-cardnews">카드뉴스</button>
                <button class="btn btn-sm" onclick="contentTab('sources')" id="ct-btn-sources">소재함</button>
                <button class="btn btn-sm" onclick="contentTab('schedule')" id="ct-btn-schedule">발행 스케줄</button>
                <button class="btn btn-sm" onclick="contentTab('settings')" id="ct-btn-settings">SNS 연동</button>
            </div>
        </div>
        <div id="content-tab-area"></div>
    `;
    contentTab('dashboard');
}

function contentTab(tab) {
    // 탭 버튼 활성화
    document.querySelectorAll('[id^="ct-btn-"]').forEach(b => b.classList.remove('active'));
    const btn = document.getElementById(`ct-btn-${tab}`);
    if (btn) btn.classList.add('active');

    const area = document.getElementById('content-tab-area');
    switch(tab) {
        case 'dashboard': renderContentDashboard(area); break;
        case 'writer': renderContentWriter(area); break;
        case 'cardnews': renderCardnewsEditor(area); break;
        case 'sources': renderSourcesManager(area); break;
        case 'schedule': renderScheduleView(area); break;
        case 'settings': renderSNSSettings(area); break;
    }
}


// ============================================================
// 대시보드 — 오늘의 콘텐츠 + 요약
// ============================================================

async function renderContentDashboard(area) {
    area.innerHTML = '<div class="card"><p>로딩 중...</p></div>';

    try {
        const [items, sources, sns] = await Promise.all([
            api.get('/api/content/items?status=draft&limit=10'),
            api.get('/api/content/sources?status=pending&limit=5'),
            api.get('/api/content/publish/status')
        ]);

        const pillarLabels = {
            inertia_break: '관성 깨기', news_20people: 'AI 뉴스', vp_coding: '코딩일지',
            employee_reaction: '직원 반응', weekly_ax: '주간 AX'
        };

        area.innerHTML = `
            <div class="card" style="margin-bottom:12px">
                <h3>SNS 연동 상태</h3>
                <div style="display:flex;gap:16px;margin-top:8px">
                    <span style="color:${sns.threads?.connected ? '#10B981' : '#EF4444'}">
                        ● Threads ${sns.threads?.connected ? '연결됨 (@'+sns.threads.username+')' : '미연결'}
                    </span>
                    <span style="color:${sns.instagram?.connected ? '#10B981' : '#EF4444'}">
                        ● Instagram ${sns.instagram?.connected ? '연결됨 (@'+sns.instagram.username+')' : '미연결'}
                    </span>
                </div>
            </div>

            <div class="card" style="margin-bottom:12px">
                <h3>대기 중인 소재 (${sources.total || 0}건)</h3>
                <div id="dash-sources" style="margin-top:8px"></div>
            </div>

            <div class="card">
                <h3>미발행 콘텐츠 (${items.total || 0}건)</h3>
                <div id="dash-items" style="margin-top:8px"></div>
            </div>
        `;

        // 소재 목록
        const srcDiv = document.getElementById('dash-sources');
        if (sources.sources?.length) {
            srcDiv.innerHTML = sources.sources.map(s => `
                <div style="padding:8px 0;border-bottom:1px solid var(--border-color,#eee);display:flex;justify-content:space-between;align-items:center">
                    <div>
                        <span style="font-size:12px;color:#888">[${s.source_type}]</span>
                        <span>${escapeHtml(s.title?.substring(0, 60) || '')}</span>
                    </div>
                    <button class="btn btn-xs" onclick="evaluateSource(${s.id})">AI 평가</button>
                </div>
            `).join('');
        } else {
            srcDiv.innerHTML = '<p style="color:#888">대기 중인 소재 없음</p>';
        }

        // 콘텐츠 목록
        const itemDiv = document.getElementById('dash-items');
        if (items.items?.length) {
            itemDiv.innerHTML = items.items.map(i => `
                <div style="padding:12px 0;border-bottom:1px solid var(--border-color,#eee)">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                        <span class="badge">${pillarLabels[i.content_type] || i.content_type}</span>
                        <span style="font-size:12px;color:#888">${i.platform}</span>
                    </div>
                    <div style="font-size:13px;white-space:pre-wrap;max-height:120px;overflow:hidden;color:#555">${escapeHtml(i.body?.substring(0, 200) || '')}</div>
                    <div style="margin-top:8px;display:flex;gap:6px">
                        <button class="btn btn-xs btn-primary" onclick="approveItem(${i.id})">승인</button>
                        <button class="btn btn-xs" onclick="editItemInWriter(${i.id})">수정</button>
                        <button class="btn btn-xs" onclick="regenerateItem(${i.id})">재생성</button>
                        <button class="btn btn-xs btn-danger" onclick="rejectItem(${i.id})">폐기</button>
                    </div>
                </div>
            `).join('');
        } else {
            itemDiv.innerHTML = '<p style="color:#888">생성된 콘텐츠 없음. "글쓰기" 탭에서 새로 만들어보세요.</p>';
        }

    } catch (e) {
        area.innerHTML = `<div class="card"><p style="color:red">로딩 실패: ${e.message}</p></div>`;
    }
}


// ============================================================
// 글쓰기 도구 (쓰레드) — API 없이 편집, "생성" 시만 API 호출
// ============================================================

function renderContentWriter(area) {
    area.innerHTML = `
        <div class="card" style="margin-bottom:12px">
            <h3>쓰레드 글쓰기</h3>
            <div style="display:flex;gap:8px;margin:12px 0">
                <select id="writer-pillar" style="padding:6px 10px;border-radius:6px;border:1px solid #ddd">
                    <option value="inertia_break">관성 깨기</option>
                    <option value="news_20people">AI 뉴스 해석</option>
                    <option value="vp_coding">부사장 코딩일지</option>
                    <option value="employee_reaction">직원 반응</option>
                    <option value="weekly_ax">주간 AX 리포트</option>
                </select>
                <button class="btn btn-primary" onclick="generateThreadsPost()">AI 생성</button>
            </div>
            <div style="margin-bottom:8px">
                <label style="font-size:12px;color:#888">소재 (직접 입력하거나 소재함에서 선택)</label>
                <textarea id="writer-source" rows="3" style="width:100%;padding:8px;border-radius:6px;border:1px solid #ddd;resize:vertical;font-size:13px"
                    placeholder="예: 발주서 자동화 시스템을 만들었다. PDF 넣으면 AI가 품목 추출..."></textarea>
            </div>
        </div>

        <div class="card">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <h3>편집기 <span id="writer-charcount" style="font-size:12px;color:#888;font-weight:normal">(0자)</span></h3>
                <div style="display:flex;gap:6px">
                    <button class="btn btn-sm" onclick="saveWriterContent()">DB 저장</button>
                    <button class="btn btn-sm" onclick="copyWriterContent()">복사</button>
                    <button class="btn btn-sm btn-primary" onclick="publishWriterContent()">발행</button>
                </div>
            </div>
            <textarea id="writer-editor" rows="12"
                style="width:100%;padding:12px;border-radius:8px;border:1px solid #ddd;resize:vertical;font-size:14px;line-height:1.7;font-family:inherit"
                oninput="updateCharCount()"
                placeholder="AI가 생성하거나, 직접 작성하세요..."></textarea>
            <div style="margin-top:8px;display:flex;justify-content:space-between;align-items:center">
                <span style="font-size:12px;color:#888">쓰레드 권장: 200~400자</span>
                <span id="writer-status" style="font-size:12px"></span>
            </div>
        </div>
    `;
}

function updateCharCount() {
    const editor = document.getElementById('writer-editor');
    const count = document.getElementById('writer-charcount');
    const len = editor.value.length;
    const color = len > 400 ? '#EF4444' : len > 200 ? '#10B981' : '#888';
    count.textContent = `(${len}자)`;
    count.style.color = color;
}

async function generateThreadsPost() {
    const pillar = document.getElementById('writer-pillar').value;
    const source = document.getElementById('writer-source').value;
    const editor = document.getElementById('writer-editor');
    const status = document.getElementById('writer-status');

    if (!source.trim()) {
        alert('소재를 입력해주세요.');
        return;
    }

    status.textContent = 'AI 생성 중...';
    status.style.color = '#3B82F6';

    try {
        const result = await api.post('/api/content/items/generate', {
            platform: 'threads',
            content_type: pillar,
            manual_text: source
        });

        editor.value = result.body || '';
        updateCharCount();
        status.textContent = '생성 완료';
        status.style.color = '#10B981';

        // 생성된 item_id 저장 (저장/발행 시 사용)
        editor.dataset.itemId = result.item_id;
    } catch (e) {
        status.textContent = `생성 실패: ${e.message}`;
        status.style.color = '#EF4444';
    }
}

async function saveWriterContent() {
    const editor = document.getElementById('writer-editor');
    const itemId = editor.dataset.itemId;

    if (!itemId) {
        // 새로 저장 (AI 생성 없이 수동 작성한 경우)
        try {
            const result = await api.post('/api/content/items/generate', {
                platform: 'threads',
                content_type: document.getElementById('writer-pillar').value,
                manual_text: editor.value
            });
            editor.dataset.itemId = result.item_id;
            showToast('저장 완료');
        } catch (e) {
            showToast('저장 실패: ' + e.message, 'error');
        }
    } else {
        // 기존 콘텐츠 수정
        try {
            await api.put(`/api/content/items/${itemId}`, { body: editor.value });
            showToast('수정 저장 완료');
        } catch (e) {
            showToast('수정 실패: ' + e.message, 'error');
        }
    }
}

function copyWriterContent() {
    const editor = document.getElementById('writer-editor');
    navigator.clipboard.writeText(editor.value);
    showToast('클립보드에 복사됨');
}

async function publishWriterContent() {
    const editor = document.getElementById('writer-editor');
    const itemId = editor.dataset.itemId;

    if (!itemId) {
        alert('먼저 저장해주세요.');
        return;
    }

    if (!confirm('Threads에 발행하시겠습니까?')) return;

    try {
        const result = await api.post(`/api/content/items/${itemId}/publish`, {
            platform: 'threads'
        });
        showToast(result.message || '발행 완료');
    } catch (e) {
        showToast('발행 실패: ' + e.message, 'error');
    }
}


// ============================================================
// 카드뉴스 편집기 — 프론트엔드에서 실시간 미리보기
// ============================================================

function renderCardnewsEditor(area) {
    area.innerHTML = `
        <div class="card" style="margin-bottom:12px">
            <h3>카드뉴스 편집기</h3>
            <p style="font-size:13px;color:#888;margin:8px 0">
                JSON을 수정하면 미리보기가 실시간 반영됩니다. "AI 생성"으로 자동 생성도 가능.
            </p>
            <div style="display:flex;gap:8px;margin-bottom:12px">
                <textarea id="cardnews-source" rows="2" style="flex:1;padding:8px;border-radius:6px;border:1px solid #ddd;font-size:13px"
                    placeholder="소재 입력 (예: 발주서 자동화 2시간→10분)"></textarea>
                <button class="btn btn-primary" onclick="generateCardnews()" style="white-space:nowrap">AI 생성</button>
            </div>
        </div>

        <div style="display:flex;gap:12px">
            <div class="card" style="flex:1;min-width:0">
                <h4>JSON 편집</h4>
                <textarea id="cardnews-json" rows="20"
                    style="width:100%;padding:10px;border-radius:6px;border:1px solid #ddd;font-family:monospace;font-size:12px;resize:vertical"
                    oninput="updateCardnewsPreview()">${getDefaultCardnewsJSON()}</textarea>
            </div>
            <div class="card" style="width:280px;flex-shrink:0">
                <h4>미리보기</h4>
                <div id="cardnews-preview" style="margin-top:8px"></div>
                <div style="margin-top:8px;display:flex;gap:6px">
                    <button class="btn btn-xs" onclick="prevCardSlide()">이전</button>
                    <span id="cardnews-slide-num" style="font-size:12px;line-height:28px;flex:1;text-align:center">1/5</span>
                    <button class="btn btn-xs" onclick="nextCardSlide()">다음</button>
                </div>
                <button class="btn btn-sm btn-primary" style="width:100%;margin-top:8px" onclick="exportCardnewsPNG()">
                    PNG 내보내기
                </button>
            </div>
        </div>
    `;

    window._cardSlideIndex = 0;
    updateCardnewsPreview();
}

function getDefaultCardnewsJSON() {
    return JSON.stringify({
        slides: [
            {type:"cover", text:"발주서 처리에\n매일 2시간?", subtext:"관성 리포트 EP.01"},
            {type:"inertia", text:"PDF → 수동 확인 → ERP 입력 → 송장 등록", subtext:"10년간 같은 방식"},
            {type:"transformation", text:"PDF 업로드 → AI 분석 → 자동 처리", subtext:"전체 10분"},
            {type:"stats", text:"2시간 → 10분", subtext:"월 40시간 절약"},
            {type:"cta", text:"다음: 직원의 첫 반응은?", subtext:"팔로우하고 다음 편 받기"}
        ],
        caption:"관성을 깨는 부사장의 AI 전환 실전기",
        hashtags:["관성깨기","AI자동화","중소기업AI"]
    }, null, 2);
}

function updateCardnewsPreview() {
    const preview = document.getElementById('cardnews-preview');
    try {
        const data = JSON.parse(document.getElementById('cardnews-json').value);
        const slides = data.slides || [];
        const idx = window._cardSlideIndex || 0;
        const slide = slides[idx];

        if (!slide) {
            preview.innerHTML = '<p style="color:#888">슬라이드 없음</p>';
            return;
        }

        const colors = {
            cover: {bg:'#0C0F1D', accent:'#97C459'},
            inertia: {bg:'#0C0F1D', accent:'#D85A30'},
            transformation: {bg:'#0C0F1D', accent:'#1D9E75'},
            stats: {bg:'#0C0F1D', accent:'#06B6D4'},
            insight: {bg:'#0C0F1D', accent:'#94A3B8'},
            cta: {bg:'#0C0F1D', accent:'#97C459'}
        };
        const c = colors[slide.type] || colors.cover;

        preview.innerHTML = `
            <div style="width:252px;height:315px;background:${c.bg};border-radius:12px;padding:24px;display:flex;flex-direction:column;justify-content:center;color:white;position:relative;overflow:hidden">
                <div style="position:absolute;inset:0;background-image:linear-gradient(90deg,rgba(99,153,34,0.06)1px,transparent 1px),linear-gradient(0deg,rgba(99,153,34,0.06)1px,transparent 1px);background-size:24px 24px;opacity:0.4"></div>
                <div style="position:relative;z-index:1">
                    <div style="font-size:10px;color:${c.accent};text-transform:uppercase;letter-spacing:0.1em;margin-bottom:8px">${slide.type}</div>
                    <div style="font-size:18px;font-weight:700;line-height:1.4;white-space:pre-line;margin-bottom:8px">${escapeHtml(slide.text || '')}</div>
                    <div style="font-size:11px;color:#94A3B8">${escapeHtml(slide.subtext || '')}</div>
                </div>
            </div>
        `;

        document.getElementById('cardnews-slide-num').textContent = `${idx+1}/${slides.length}`;
    } catch (e) {
        preview.innerHTML = '<p style="color:#EF4444;font-size:12px">JSON 파싱 오류</p>';
    }
}

function prevCardSlide() {
    window._cardSlideIndex = Math.max(0, (window._cardSlideIndex || 0) - 1);
    updateCardnewsPreview();
}
function nextCardSlide() {
    try {
        const data = JSON.parse(document.getElementById('cardnews-json').value);
        window._cardSlideIndex = Math.min(data.slides.length - 1, (window._cardSlideIndex || 0) + 1);
    } catch(e) {}
    updateCardnewsPreview();
}

async function generateCardnews() {
    const source = document.getElementById('cardnews-source').value;
    if (!source.trim()) { alert('소재를 입력해주세요'); return; }

    try {
        const result = await api.post('/api/content/items/generate', {
            platform: 'instagram',
            content_type: 'inertia_break',
            manual_text: source
        });
        // AI가 JSON으로 응답하면 에디터에 삽입
        document.getElementById('cardnews-json').value =
            typeof result.body === 'string' ? result.body : JSON.stringify(result.body, null, 2);
        window._cardSlideIndex = 0;
        updateCardnewsPreview();
    } catch (e) {
        alert('생성 실패: ' + e.message);
    }
}

function exportCardnewsPNG() {
    // html2canvas를 사용한 클라이언트 사이드 PNG 내보내기
    // CDN: <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    alert('html2canvas 라이브러리 로드 후 사용 가능.\nindex.html에 CDN 스크립트 추가 필요.');
}


// ============================================================
// 소재함
// ============================================================

async function renderSourcesManager(area) {
    area.innerHTML = '<div class="card"><p>로딩 중...</p></div>';

    try {
        const data = await api.get('/api/content/sources?limit=30');
        area.innerHTML = `
            <div class="card">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
                    <h3>소재함 (${data.total || 0}건)</h3>
                    <div style="display:flex;gap:6px">
                        <button class="btn btn-sm" onclick="addManualSource()">수동 추가</button>
                        <button class="btn btn-sm btn-primary" onclick="triggerCollection()">자동 수집</button>
                    </div>
                </div>
                <table class="table" style="font-size:13px">
                    <thead><tr>
                        <th>유형</th><th>제목</th><th>점수</th><th>상태</th><th>액션</th>
                    </tr></thead>
                    <tbody id="sources-tbody"></tbody>
                </table>
            </div>
        `;

        const tbody = document.getElementById('sources-tbody');
        tbody.innerHTML = (data.sources || []).map(s => `
            <tr>
                <td><span class="badge">${s.source_type}</span></td>
                <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis">${escapeHtml(s.title || '')}</td>
                <td>${s.relevance_score ? s.relevance_score.toFixed(1) : '-'}</td>
                <td>${s.status}</td>
                <td>
                    <button class="btn btn-xs" onclick="evaluateSource(${s.id})">평가</button>
                    <button class="btn btn-xs" onclick="useSourceForContent(${s.id})">글쓰기</button>
                </td>
            </tr>
        `).join('');
    } catch (e) {
        area.innerHTML = `<div class="card"><p style="color:red">로딩 실패</p></div>`;
    }
}

async function triggerCollection() {
    try {
        const result = await api.post('/api/content/sources/collect', {});
        showToast(`수집 완료: RSS ${result.collected?.rss || 0}건, GitHub ${result.collected?.github || 0}건`);
        renderSourcesManager(document.getElementById('content-tab-area'));
    } catch (e) {
        showToast('수집 실패: ' + e.message, 'error');
    }
}

async function evaluateSource(sourceId) {
    try {
        const result = await api.post(`/api/content/sources/${sourceId}/evaluate`, {});
        showToast(`평가 완료: ${result.score}점 - ${result.reason || ''}`);
    } catch (e) {
        showToast('평가 실패: ' + e.message, 'error');
    }
}


// ============================================================
// SNS 연동 설정
// ============================================================

async function renderSNSSettings(area) {
    area.innerHTML = '<div class="card"><p>연동 상태 확인 중...</p></div>';

    try {
        const status = await api.get('/api/content/publish/status');

        area.innerHTML = `
            <div class="card" style="margin-bottom:12px">
                <h3>Threads 연동</h3>
                <div style="margin-top:12px">
                    <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
                        <span style="width:12px;height:12px;border-radius:50%;background:${status.threads?.connected ? '#10B981' : '#EF4444'};display:inline-block"></span>
                        <strong>${status.threads?.connected ? '연결됨' : '미연결'}</strong>
                        ${status.threads?.username ? `<span style="color:#888">@${status.threads.username}</span>` : ''}
                    </div>
                    <p style="font-size:13px;color:#888">
                        Threads API 토큰은 Render 환경변수(THREADS_USER_ID, THREADS_ACCESS_TOKEN)로 관리됩니다.
                    </p>
                    ${status.threads?.connected ? '<button class="btn btn-sm" onclick="testThreadsPost()">테스트 발행</button>' : ''}
                </div>
            </div>

            <div class="card" style="margin-bottom:12px">
                <h3>Instagram 연동</h3>
                <div style="margin-top:12px">
                    <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
                        <span style="width:12px;height:12px;border-radius:50%;background:${status.instagram?.connected ? '#10B981' : '#EF4444'};display:inline-block"></span>
                        <strong>${status.instagram?.connected ? '연결됨' : '미연결'}</strong>
                        ${status.instagram?.username ? `<span style="color:#888">@${status.instagram.username}</span>` : ''}
                    </div>
                    <p style="font-size:13px;color:#888">
                        Instagram Graph API 토큰은 Render 환경변수(IG_USER_ID, IG_ACCESS_TOKEN)로 관리됩니다.
                    </p>
                </div>
            </div>

            <div class="card">
                <h3>연동 가이드</h3>
                <div style="font-size:13px;color:#666;line-height:1.8;margin-top:8px">
                    <p><strong>1단계:</strong> <a href="https://developers.facebook.com" target="_blank">Meta Developer</a>에서 앱 생성</p>
                    <p><strong>2단계:</strong> Threads Publishing API / Instagram Graph API 권한 추가</p>
                    <p><strong>3단계:</strong> Long-lived User Access Token 발급</p>
                    <p><strong>4단계:</strong> Render 환경변수에 토큰 등록 후 재배포</p>
                </div>
            </div>
        `;
    } catch (e) {
        area.innerHTML = `<div class="card"><p style="color:red">상태 확인 실패: ${e.message}</p></div>`;
    }
}


// ============================================================
// 발행 스케줄 (캘린더)
// ============================================================

async function renderScheduleView(area) {
    area.innerHTML = '<div class="card"><p>로딩 중...</p></div>';
    try {
        const data = await api.get('/api/content/schedule');
        area.innerHTML = `
            <div class="card">
                <h3>발행 스케줄</h3>
                <p style="font-size:13px;color:#888;margin:8px 0">승인된 콘텐츠가 예약 순서대로 표시됩니다.</p>
                <div id="schedule-list" style="margin-top:12px"></div>
            </div>
        `;
        const list = document.getElementById('schedule-list');
        if (data.schedule?.length) {
            list.innerHTML = data.schedule.map(s => `
                <div style="padding:8px 0;border-bottom:1px solid #eee;display:flex;justify-content:space-between">
                    <div>
                        <span class="badge">${s.platform}</span>
                        <span style="font-size:13px">${escapeHtml(s.body?.substring(0, 60) || '').replace(/\n/g,' ')}</span>
                    </div>
                    <span style="font-size:12px;color:#888">${s.status}</span>
                </div>
            `).join('');
        } else {
            list.innerHTML = '<p style="color:#888">예약된 콘텐츠 없음</p>';
        }
    } catch (e) {
        area.innerHTML = `<div class="card"><p style="color:red">로딩 실패</p></div>`;
    }
}


// ============================================================
// 유틸리티
// ============================================================

async function approveItem(id) {
    await api.put(`/api/content/items/${id}/approve`, {});
    showToast('승인 완료');
    renderContentDashboard(document.getElementById('content-tab-area'));
}

async function rejectItem(id) {
    if (!confirm('정말 폐기하시겠습니까?')) return;
    await api.put(`/api/content/items/${id}/reject`, {});
    showToast('폐기됨');
    renderContentDashboard(document.getElementById('content-tab-area'));
}

async function regenerateItem(id) {
    const result = await api.post(`/api/content/items/${id}/regenerate`, {});
    showToast('새 버전 생성 완료');
    renderContentDashboard(document.getElementById('content-tab-area'));
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function showToast(msg, type) {
    // 기존 order-agent의 toast 함수가 있으면 사용, 없으면 alert
    if (typeof window.showNotification === 'function') {
        window.showNotification(msg, type);
    } else {
        alert(msg);
    }
}
