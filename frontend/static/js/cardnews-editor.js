/**
 * Content Factory — 카드뉴스 비주얼 에디터
 * content.js에서 renderCardnewsEditor()를 이 코드로 교체
 *
 * 의존성: html2canvas (CDN)
 * index.html에 추가:
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
 */

// ============================================================
// 디자인 템플릿 5종
// ============================================================

const CARD_TEMPLATES = {
    'circuit-dark': {
        name: '회로 다크',
        bg: '#0C0F1D',
        bgPattern: `linear-gradient(90deg, rgba(99,153,34,0.07) 1px, transparent 1px),
                     linear-gradient(0deg, rgba(99,153,34,0.07) 1px, transparent 1px)`,
        patternSize: '60px 60px',
        accent: '#97C459',
        accentRgb: '99,153,34',
        textPrimary: '#F1F5F9',
        textSecondary: '#94A3B8',
        textMuted: '#64748B',
        badgeBg: 'rgba(99,153,34,0.12)',
        badgeBorder: 'rgba(99,153,34,0.25)',
    },
    'gradient-tech': {
        name: '테크 그라디언트',
        bg: 'linear-gradient(160deg, #0A0E1A 0%, #1A1040 50%, #0A0E1A 100%)',
        bgPattern: `linear-gradient(90deg, rgba(127,119,221,0.06) 1px, transparent 1px),
                     linear-gradient(0deg, rgba(127,119,221,0.06) 1px, transparent 1px)`,
        patternSize: '60px 60px',
        accent: '#AFA9EC',
        accentRgb: '127,119,221',
        textPrimary: '#F1F5F9',
        textSecondary: '#94A3B8',
        textMuted: '#64748B',
        badgeBg: 'rgba(127,119,221,0.12)',
        badgeBorder: 'rgba(127,119,221,0.25)',
    },
    'stat-highlight': {
        name: '수치 강조',
        bg: 'linear-gradient(160deg, #0C0F1D 0%, #0F172A 100%)',
        bgPattern: `linear-gradient(90deg, rgba(6,182,212,0.06) 1px, transparent 1px),
                     linear-gradient(0deg, rgba(6,182,212,0.06) 1px, transparent 1px)`,
        patternSize: '60px 60px',
        accent: '#06B6D4',
        accentRgb: '6,182,212',
        textPrimary: '#F1F5F9',
        textSecondary: '#94A3B8',
        textMuted: '#64748B',
        badgeBg: 'rgba(6,182,212,0.12)',
        badgeBorder: 'rgba(6,182,212,0.25)',
    },
    'split-compare': {
        name: '비교 분할',
        bg: '#0C0F1D',
        bgPattern: 'none',
        patternSize: '60px 60px',
        accent: '#10B981',
        accentRgb: '16,185,129',
        accentRed: '#EF4444',
        textPrimary: '#F1F5F9',
        textSecondary: '#94A3B8',
        textMuted: '#64748B',
        badgeBg: 'rgba(16,185,129,0.12)',
        badgeBorder: 'rgba(16,185,129,0.25)',
    },
    'terminal-log': {
        name: '터미널',
        bg: '#0D1117',
        bgPattern: 'none',
        patternSize: '60px 60px',
        accent: '#58A6FF',
        accentRgb: '88,166,255',
        textPrimary: '#C9D1D9',
        textSecondary: '#8B949E',
        textMuted: '#484F58',
        badgeBg: 'rgba(88,166,255,0.1)',
        badgeBorder: 'rgba(88,166,255,0.2)',
        fontFamily: "'JetBrains Mono', 'Courier New', monospace",
    },
};

// ============================================================
// 에디터 상태
// ============================================================

let editorState = {
    template: 'circuit-dark',
    currentSlide: 0,
    slides: [
        { type: 'cover', text: '발주서 처리에\n매일 2시간?', subtext: '관성 리포트 EP.01', badge: '관성 리포트' },
        { type: 'inertia', text: 'PDF 수동 확인\nERP 수기 입력\n송장 개별 등록', subtext: '10년간 같은 방식', label: '관성' },
        { type: 'transformation', text: 'PDF 업로드 → AI 분석\nERP 자동 전송\n송장 자동 생성', subtext: '전체 10분', label: '전환' },
        { type: 'stats', text: '2시간 → 10분', subtext: '월 40시간 절약 = 정규직 0.3명분' },
        { type: 'insight', text: '관성은\n"원래 이렇게 하는 거야"\n라는 한 마디에\n숨어있다', subtext: '관성을 깨는 부사장' },
        { type: 'cta', text: '다음: 직원의 첫 반응은?', subtext: '팔로우하고 다음 편 받기' },
    ],
    caption: '',
    hashtags: ['관성깨기', 'AI자동화', '중소기업AI'],
};

// ============================================================
// 메인 렌더 (content.js의 renderCardnewsEditor를 교체)
// ============================================================

function renderCardnewsEditor(area) {
    area.innerHTML = `
        <div class="card" style="margin-bottom:12px">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
                <h3>카드뉴스 에디터</h3>
                <div style="display:flex;gap:6px;flex-wrap:wrap">
                    <select id="ce-template" onchange="changeTemplate(this.value)" style="padding:5px 8px;border-radius:6px;border:1px solid #ddd;font-size:12px">
                        ${Object.entries(CARD_TEMPLATES).map(([k,v]) =>
                            `<option value="${k}" ${k===editorState.template?'selected':''}>${v.name}</option>`
                        ).join('')}
                    </select>
                    <button class="btn btn-sm" onclick="addSlide()">슬라이드 추가</button>
                    <button class="btn btn-sm" onclick="aiGenerateSlides()">AI 생성</button>
                    <button class="btn btn-sm btn-primary" onclick="exportAllSlidesPNG()">전체 PNG 내보내기</button>
                </div>
            </div>
        </div>

        <div style="display:flex;gap:12px;align-items:flex-start">
            <!-- 슬라이드 목록 (왼쪽) -->
            <div id="ce-slide-list" style="width:80px;flex-shrink:0;display:flex;flex-direction:column;gap:6px"></div>

            <!-- 미리보기 (중앙) -->
            <div style="flex-shrink:0">
                <div id="ce-preview-wrap" style="width:324px;height:405px;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.2);position:relative">
                    <div id="ce-preview"></div>
                </div>
                <div style="display:flex;justify-content:center;gap:8px;margin-top:8px">
                    <button class="btn btn-xs" onclick="navSlide(-1)">이전</button>
                    <span id="ce-slide-num" style="font-size:12px;line-height:28px">1/6</span>
                    <button class="btn btn-xs" onclick="navSlide(1)">다음</button>
                    <button class="btn btn-xs" onclick="exportCurrentSlidePNG()">이 슬라이드 PNG</button>
                </div>
            </div>

            <!-- 편집 패널 (오른쪽) -->
            <div class="card" style="flex:1;min-width:0" id="ce-edit-panel"></div>
        </div>

        <!-- 숨겨진 풀사이즈 렌더링 영역 (PNG 내보내기용) -->
        <div id="ce-render-area" style="position:absolute;left:-9999px;top:-9999px"></div>
    `;

    renderSlideList();
    renderPreview();
    renderEditPanel();
}

// ============================================================
// 슬라이드 목록 (썸네일)
// ============================================================

function renderSlideList() {
    const list = document.getElementById('ce-slide-list');
    if (!list) return;

    list.innerHTML = editorState.slides.map((s, i) => `
        <div onclick="selectSlide(${i})" style="
            width:72px;height:90px;border-radius:6px;overflow:hidden;cursor:pointer;
            border:2px solid ${i === editorState.currentSlide ? '#3B82F6' : 'transparent'};
            opacity:${i === editorState.currentSlide ? 1 : 0.6};
            position:relative;background:#0C0F1D;
            display:flex;align-items:center;justify-content:center;
            font-size:9px;color:#94A3B8;text-align:center;padding:4px;line-height:1.3;
        ">
            <div>
                <div style="font-size:8px;color:#64748B;margin-bottom:2px">${i+1}. ${s.type}</div>
                <div style="white-space:pre-line">${(s.text||'').substring(0,30)}</div>
            </div>
            ${editorState.slides.length > 1 ? `
                <div onclick="event.stopPropagation();removeSlide(${i})"
                     style="position:absolute;top:2px;right:2px;width:14px;height:14px;border-radius:50%;
                            background:rgba(239,68,68,0.8);color:white;font-size:9px;
                            display:flex;align-items:center;justify-content:center;cursor:pointer">x</div>
            ` : ''}
            ${i > 0 ? `
                <div onclick="event.stopPropagation();moveSlide(${i},-1)"
                     style="position:absolute;bottom:2px;left:2px;font-size:10px;cursor:pointer;color:#64748B">▲</div>
            ` : ''}
            ${i < editorState.slides.length-1 ? `
                <div onclick="event.stopPropagation();moveSlide(${i},1)"
                     style="position:absolute;bottom:2px;right:2px;font-size:10px;cursor:pointer;color:#64748B">▼</div>
            ` : ''}
        </div>
    `).join('');
}

// ============================================================
// 슬라이드 미리보기 렌더링 (실제 디자인 템플릿 적용)
// ============================================================

function renderPreview() {
    const preview = document.getElementById('ce-preview');
    if (!preview) return;

    const slide = editorState.slides[editorState.currentSlide];
    const t = CARD_TEMPLATES[editorState.template];
    if (!slide || !t) return;

    // 스케일: 실제 1080x1350 → 324x405 (0.3배)
    const scale = 0.3;
    const html = buildSlideHTML(slide, t, false);

    preview.innerHTML = `
        <div style="width:1080px;height:1350px;transform:scale(${scale});transform-origin:top left;position:absolute;top:0;left:0">
            ${html}
        </div>
    `;

    const numEl = document.getElementById('ce-slide-num');
    if (numEl) numEl.textContent = `${editorState.currentSlide + 1}/${editorState.slides.length}`;
}

function buildSlideHTML(slide, t, fullsize) {
    const ff = t.fontFamily || "'Pretendard', -apple-system, sans-serif";
    const patternCSS = t.bgPattern !== 'none' ?
        `background-image:${t.bgPattern};background-size:${t.patternSize};` : '';

    const textLines = (slide.text || '').split('\n');

    // 슬라이드 유형별 레이아웃
    let content = '';

    if (slide.type === 'cover') {
        content = `
            <div style="flex:1;display:flex;flex-direction:column;justify-content:center;padding:80px 72px">
                ${slide.badge ? `
                    <div style="display:inline-flex;align-items:center;gap:8px;background:${t.badgeBg};
                         border:1px solid ${t.badgeBorder};border-radius:24px;padding:10px 24px;
                         font-size:18px;color:${t.accent};font-weight:500;margin-bottom:48px;width:fit-content">
                        ${slide.badge}
                    </div>
                ` : ''}
                <div style="font-size:56px;font-weight:700;line-height:1.3;letter-spacing:-0.02em;
                     color:${t.textPrimary};margin-bottom:32px;white-space:pre-line">${escHtml(slide.text || '')}</div>
                <div style="font-size:26px;color:${t.textSecondary};line-height:1.6">${escHtml(slide.subtext || '')}</div>
            </div>
            <div style="padding:0 72px 60px;display:flex;justify-content:space-between;align-items:center">
                <span style="font-size:18px;color:${t.textMuted};font-family:monospace">관성을 깨는 부사장</span>
                <span style="font-size:22px;font-weight:700;color:${t.textSecondary};letter-spacing:0.08em">AX REPORT</span>
            </div>
        `;
    } else if (slide.type === 'inertia') {
        const items = textLines.filter(l => l.trim());
        content = `
            <div style="flex:1;display:flex;flex-direction:column;padding:72px">
                <div style="font-size:20px;text-transform:uppercase;font-weight:600;letter-spacing:0.12em;
                     color:#EF4444;margin-bottom:12px">${escHtml(slide.label || '관성')}</div>
                <div style="font-size:36px;font-weight:700;color:${t.textPrimary};margin-bottom:8px">
                    "원래 이렇게 해왔는데"</div>
                <div style="font-size:20px;color:${t.textMuted};margin-bottom:40px">${escHtml(slide.subtext || '')}</div>
                <div style="flex:1;display:flex;flex-direction:column;justify-content:center;gap:24px">
                    ${items.map((item, i) => `
                        <div style="display:flex;align-items:center;gap:16px;background:rgba(239,68,68,0.06);
                             border:1px solid rgba(239,68,68,0.12);border-radius:16px;padding:24px 28px">
                            <span style="color:#EF4444;font-family:monospace;font-size:20px;font-weight:600;width:36px">
                                ${String(i+1).padStart(2,'0')}
                            </span>
                            <span style="font-size:22px;color:${t.textSecondary}">${escHtml(item)}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    } else if (slide.type === 'transformation') {
        const items = textLines.filter(l => l.trim());
        content = `
            <div style="flex:1;display:flex;flex-direction:column;padding:72px">
                <div style="font-size:20px;text-transform:uppercase;font-weight:600;letter-spacing:0.12em;
                     color:#10B981;margin-bottom:12px">${escHtml(slide.label || '전환')}</div>
                <div style="font-size:36px;font-weight:700;color:${t.textPrimary};margin-bottom:8px">
                    부사장이 직접 만들었다</div>
                <div style="font-size:20px;color:${t.textMuted};margin-bottom:40px">${escHtml(slide.subtext || '')}</div>
                <div style="flex:1;display:flex;flex-direction:column;justify-content:center;gap:24px">
                    ${items.map((item, i) => `
                        <div style="display:flex;align-items:center;gap:16px;background:rgba(16,185,129,0.06);
                             border:1px solid rgba(16,185,129,0.15);border-radius:16px;padding:24px 28px">
                            <span style="color:#5DCAA5;font-family:monospace;font-size:20px;font-weight:600;width:36px">
                                ${String(i+1).padStart(2,'0')}
                            </span>
                            <span style="font-size:22px;color:${t.textSecondary}">${escHtml(item)}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    } else if (slide.type === 'stats') {
        content = `
            <div style="flex:1;display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center;padding:80px 72px">
                <div style="font-size:12px;text-transform:uppercase;color:${t.textMuted};letter-spacing:0.15em;margin-bottom:32px">
                    관성이 깨진 순간</div>
                <div style="font-size:96px;font-weight:800;color:${t.textPrimary};letter-spacing:-0.03em;
                     margin-bottom:24px;line-height:1.1;white-space:pre-line">${escHtml(slide.text || '')}</div>
                <div style="font-size:24px;color:${t.textSecondary};margin-top:16px">${escHtml(slide.subtext || '')}</div>
            </div>
        `;
    } else if (slide.type === 'insight') {
        content = `
            <div style="flex:1;display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center;padding:80px 100px">
                <div style="font-size:72px;color:${t.textMuted};margin-bottom:24px;line-height:1">"</div>
                <div style="font-size:40px;font-weight:700;line-height:1.5;color:${t.textPrimary};
                     white-space:pre-line;margin-bottom:32px">${escHtml(slide.text || '')}</div>
                <div style="width:40px;height:2px;background:${t.textMuted};margin-bottom:24px"></div>
                <div style="font-size:18px;color:${t.textMuted}">${escHtml(slide.subtext || '')}</div>
            </div>
        `;
    } else if (slide.type === 'cta') {
        content = `
            <div style="flex:1;display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center;padding:80px 72px">
                <div style="font-size:18px;color:${t.textMuted};text-transform:uppercase;letter-spacing:0.15em;margin-bottom:24px">
                    다음 관성 리포트</div>
                <div style="font-size:44px;font-weight:700;line-height:1.4;color:${t.textPrimary};
                     margin-bottom:48px;white-space:pre-line">${escHtml(slide.text || '')}</div>
                <div style="display:inline-flex;align-items:center;gap:12px;background:${t.accent};
                     color:white;padding:20px 48px;border-radius:40px;font-size:22px;font-weight:600">
                    ${escHtml(slide.subtext || '팔로우하고 다음 편 받기')}</div>
                <div style="margin-top:32px;font-size:20px;color:${t.textMuted};font-family:monospace">
                    관성을 깨는 부사장</div>
            </div>
        `;
    } else {
        content = `
            <div style="flex:1;display:flex;flex-direction:column;justify-content:center;padding:72px">
                <div style="font-size:36px;font-weight:700;color:${t.textPrimary};white-space:pre-line;margin-bottom:16px">
                    ${escHtml(slide.text || '')}</div>
                <div style="font-size:22px;color:${t.textSecondary}">${escHtml(slide.subtext || '')}</div>
            </div>
        `;
    }

    return `
        <div style="width:1080px;height:1350px;position:relative;overflow:hidden;
                    background:${t.bg};font-family:${ff};display:flex;flex-direction:column">
            <div style="position:absolute;inset:0;${patternCSS}opacity:0.5"></div>
            <div style="position:relative;z-index:1;flex:1;display:flex;flex-direction:column">
                ${content}
            </div>
        </div>
    `;
}

// ============================================================
// 편집 패널 (오른쪽)
// ============================================================

function renderEditPanel() {
    const panel = document.getElementById('ce-edit-panel');
    if (!panel) return;

    const slide = editorState.slides[editorState.currentSlide];
    if (!slide) return;

    const typeOptions = ['cover','inertia','transformation','stats','insight','cta','custom'];
    const typeLabels = {cover:'표지',inertia:'관성',transformation:'전환',stats:'수치',insight:'인사이트',cta:'CTA',custom:'커스텀'};

    panel.innerHTML = `
        <h4>슬라이드 ${editorState.currentSlide + 1} 편집</h4>

        <div style="margin:12px 0 8px">
            <label style="font-size:12px;color:#888">슬라이드 유형</label>
            <select id="ce-type" onchange="updateSlideField('type', this.value)"
                style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid #ddd;font-size:13px;margin-top:4px">
                ${typeOptions.map(o => `<option value="${o}" ${slide.type===o?'selected':''}>${typeLabels[o]||o}</option>`).join('')}
            </select>
        </div>

        ${slide.type === 'cover' ? `
        <div style="margin-bottom:8px">
            <label style="font-size:12px;color:#888">배지 텍스트</label>
            <input id="ce-badge" value="${escHtml(slide.badge||'')}" oninput="updateSlideField('badge',this.value)"
                style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid #ddd;font-size:13px;margin-top:4px"/>
        </div>
        ` : ''}

        ${slide.type === 'inertia' || slide.type === 'transformation' ? `
        <div style="margin-bottom:8px">
            <label style="font-size:12px;color:#888">라벨</label>
            <input id="ce-label" value="${escHtml(slide.label||'')}" oninput="updateSlideField('label',this.value)"
                style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid #ddd;font-size:13px;margin-top:4px"/>
        </div>
        ` : ''}

        <div style="margin-bottom:8px">
            <label style="font-size:12px;color:#888">메인 텍스트 (줄바꿈: Enter)</label>
            <textarea id="ce-text" rows="5" oninput="updateSlideField('text',this.value)"
                style="width:100%;padding:8px;border-radius:6px;border:1px solid #ddd;font-size:13px;line-height:1.6;resize:vertical;margin-top:4px"
            >${slide.text||''}</textarea>
        </div>

        <div style="margin-bottom:12px">
            <label style="font-size:12px;color:#888">서브 텍스트</label>
            <input id="ce-subtext" value="${escHtml(slide.subtext||'')}" oninput="updateSlideField('subtext',this.value)"
                style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid #ddd;font-size:13px;margin-top:4px"/>
        </div>

        <div style="border-top:1px solid #eee;padding-top:12px;margin-top:8px">
            <label style="font-size:12px;color:#888">인스타 캡션</label>
            <textarea id="ce-caption" rows="3" oninput="editorState.caption=this.value"
                style="width:100%;padding:8px;border-radius:6px;border:1px solid #ddd;font-size:12px;margin-top:4px"
            >${editorState.caption||''}</textarea>
        </div>

        <div style="margin-top:8px">
            <label style="font-size:12px;color:#888">해시태그 (쉼표 구분)</label>
            <input id="ce-hashtags" value="${(editorState.hashtags||[]).join(', ')}"
                oninput="editorState.hashtags=this.value.split(',').map(s=>s.trim()).filter(Boolean)"
                style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid #ddd;font-size:12px;margin-top:4px"/>
        </div>
    `;
}

// ============================================================
// 상태 업데이트 함수들
// ============================================================

function updateSlideField(field, value) {
    editorState.slides[editorState.currentSlide][field] = value;
    renderPreview();
    renderSlideList();
    if (field === 'type') renderEditPanel();
}

function selectSlide(idx) {
    editorState.currentSlide = idx;
    renderSlideList();
    renderPreview();
    renderEditPanel();
}

function navSlide(dir) {
    const next = editorState.currentSlide + dir;
    if (next >= 0 && next < editorState.slides.length) selectSlide(next);
}

function changeTemplate(templateKey) {
    editorState.template = templateKey;
    renderPreview();
}

function addSlide() {
    editorState.slides.push({ type: 'custom', text: '새 슬라이드', subtext: '' });
    editorState.currentSlide = editorState.slides.length - 1;
    renderSlideList();
    renderPreview();
    renderEditPanel();
}

function removeSlide(idx) {
    if (editorState.slides.length <= 1) return;
    editorState.slides.splice(idx, 1);
    if (editorState.currentSlide >= editorState.slides.length) {
        editorState.currentSlide = editorState.slides.length - 1;
    }
    renderSlideList();
    renderPreview();
    renderEditPanel();
}

function moveSlide(idx, dir) {
    const newIdx = idx + dir;
    if (newIdx < 0 || newIdx >= editorState.slides.length) return;
    const temp = editorState.slides[idx];
    editorState.slides[idx] = editorState.slides[newIdx];
    editorState.slides[newIdx] = temp;
    if (editorState.currentSlide === idx) editorState.currentSlide = newIdx;
    else if (editorState.currentSlide === newIdx) editorState.currentSlide = idx;
    renderSlideList();
    renderPreview();
}

// ============================================================
// PNG 내보내기 (html2canvas)
// ============================================================

async function exportCurrentSlidePNG() {
    if (typeof html2canvas === 'undefined') {
        alert('html2canvas가 로드되지 않았습니다.\nindex.html에 CDN 스크립트를 추가해주세요.');
        return;
    }

    const renderArea = document.getElementById('ce-render-area');
    const slide = editorState.slides[editorState.currentSlide];
    const t = CARD_TEMPLATES[editorState.template];

    renderArea.style.left = '0';
    renderArea.style.top = '0';
    renderArea.style.position = 'fixed';
    renderArea.style.zIndex = '-1';
    renderArea.style.opacity = '0';
    renderArea.innerHTML = buildSlideHTML(slide, t, true);

    try {
        const canvas = await html2canvas(renderArea.firstElementChild, {
            width: 1080, height: 1350, scale: 1,
            backgroundColor: null, useCORS: true
        });

        const link = document.createElement('a');
        link.download = `slide_${editorState.currentSlide + 1}_${slide.type}.png`;
        link.href = canvas.toDataURL('image/png');
        link.click();
        showToast('PNG 저장 완료');
    } catch (e) {
        showToast('PNG 내보내기 실패: ' + e.message, 'error');
    } finally {
        renderArea.style.left = '-9999px';
        renderArea.style.position = 'absolute';
        renderArea.style.opacity = '1';
        renderArea.innerHTML = '';
    }
}

async function exportAllSlidesPNG() {
    if (typeof html2canvas === 'undefined') {
        alert('html2canvas가 로드되지 않았습니다.');
        return;
    }

    const renderArea = document.getElementById('ce-render-area');
    const t = CARD_TEMPLATES[editorState.template];

    renderArea.style.left = '0';
    renderArea.style.top = '0';
    renderArea.style.position = 'fixed';
    renderArea.style.zIndex = '-1';
    renderArea.style.opacity = '0';

    for (let i = 0; i < editorState.slides.length; i++) {
        const slide = editorState.slides[i];
        renderArea.innerHTML = buildSlideHTML(slide, t, true);

        try {
            const canvas = await html2canvas(renderArea.firstElementChild, {
                width: 1080, height: 1350, scale: 1,
                backgroundColor: null, useCORS: true
            });

            const link = document.createElement('a');
            link.download = `card_${String(i+1).padStart(2,'0')}_${slide.type}.png`;
            link.href = canvas.toDataURL('image/png');
            link.click();

            await new Promise(r => setTimeout(r, 300));
        } catch (e) {
            console.error(`슬라이드 ${i+1} 내보내기 실패:`, e);
        }
    }

    renderArea.style.left = '-9999px';
    renderArea.style.position = 'absolute';
    renderArea.style.opacity = '1';
    renderArea.innerHTML = '';
    showToast(`${editorState.slides.length}장 PNG 저장 완료`);
}

// ============================================================
// AI 생성 연동
// ============================================================

async function aiGenerateSlides() {
    const source = prompt('카드뉴스 소재를 입력하세요:\n(예: 발주서 자동화로 2시간이 10분이 됐다)');
    if (!source) return;

    showToast('AI 생성 중...');

    try {
        const result = await api.post('/api/content/items/generate', {
            platform: 'instagram',
            content_type: 'inertia_break',
            manual_text: source
        });

        let parsed;
        try {
            const body = result.body || '';
            parsed = JSON.parse(body.replace(/```json|```/g, '').trim());
        } catch (e) {
            showToast('AI 응답 파싱 실패. JSON 편집기로 전환합니다.', 'error');
            return;
        }

        if (parsed.slides) {
            editorState.slides = parsed.slides;
            editorState.caption = parsed.caption || '';
            editorState.hashtags = parsed.hashtags || [];
            editorState.currentSlide = 0;
            renderSlideList();
            renderPreview();
            renderEditPanel();
            showToast('AI 생성 완료');
        }
    } catch (e) {
        showToast('생성 실패: ' + e.message, 'error');
    }
}

function escHtml(str) {
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}
