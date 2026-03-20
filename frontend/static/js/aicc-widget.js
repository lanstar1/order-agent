/**
 * 랜스타 AI 상담 챗봇 위젯
 * 고도몰 쇼핑몰에 직접 삽입 (iframe/별도 URL 없음)
 *
 * 사용법: 고도몰 스킨 HTML 하단에 아래 추가
 * <script src="https://order-agent-ffr7.onrender.com/static/js/aicc-widget.js"></script>
 * <script>LanstarChat.init({ memberName: '{$member.mem_name}', memberLevel: '{$member.groupSno}' });</script>
 */

(function () {
  'use strict';

  const BACKEND = 'https://order-agent-ffr7.onrender.com';

  // ── 상태 변수 ─────────────────────────────────────────────
  let _memberName = '';
  let _memberPhone = '';
  let _memberNo = '';      // 고도몰 회원번호 (memNo)
  let _memberLevel = 0;   // 고도몰 회원등급 (0=일반, 1=LV1, 2=LV2사업자, 3=LV3업체)
  let _selectedMenu = '';
  let _selectedModel = null;   // {model_name, erp_code, product_name}
  let _sessionId = null;
  let _ws = null;
  let _allModels = [];
  let _isSending = false;      // 중복 전송 방지 플래그
  let _placeholderExamples = []; // 리뷰 기반 랜덤 예시 질문
  let _techPlaceholders = [];    // 기술문의: 모델별 QnA 기반 예시 질문

  // ── 세션 유지 (sessionStorage) ─────────────────────────────
  var STORAGE_KEY = 'ls_chat_state';

  function _saveState() {
    try {
      var mc = document.getElementById('ls-chat-messages');
      var popup = document.getElementById('ls-chat-popup');
      var state = {
        sessionId: _sessionId,
        selectedMenu: _selectedMenu,
        selectedModel: _selectedModel,
        messagesHtml: mc ? mc.innerHTML : '',
        popupOpen: popup ? popup.classList.contains('open') : false,
        screen: _getCurrentScreen(),
      };
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (e) { /* quota 초과 등 무시 */ }
  }

  function _clearState() {
    try { sessionStorage.removeItem(STORAGE_KEY); } catch (e) {}
  }

  function _loadState() {
    try {
      var raw = sessionStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }

  function _getCurrentScreen() {
    var screens = ['menu', 'model', 'chat', 'inventory', 'quote', 'orders'];
    for (var i = 0; i < screens.length; i++) {
      var el = document.getElementById('ls-screen-' + screens[i]);
      if (el && el.style.display !== 'none') return screens[i];
    }
    return 'menu';
  }

  function _restoreSession() {
    var state = _loadState();
    console.log('[AICC] restore check:', state ? {sid: state.sessionId, screen: state.screen, open: state.popupOpen, hasHtml: !!state.messagesHtml} : 'no state');
    if (!state) return;

    // 팝업 상태 먼저 복원 (세션 없어도 열린 상태는 유지)
    if (state.popupOpen) {
      document.getElementById('ls-chat-popup').classList.add('open');
    }

    if (!state.sessionId) return;

    // 상태 복원
    _sessionId = state.sessionId;
    _selectedMenu = state.selectedMenu || '';
    _selectedModel = state.selectedModel || null;

    // 채팅 화면이었으면 복원
    if (state.screen === 'chat') {
      // 상단 정보 표시
      var modelInfo = _selectedModel ? '<strong>' + escHtml(_selectedModel.model_name) + '</strong> | ' : '';
      document.getElementById('ls-chat-info-text').innerHTML =
        modelInfo + '<strong>' + escHtml(_selectedMenu) + '</strong>';

      // 채팅 메시지 복원
      if (state.messagesHtml) {
        document.getElementById('ls-chat-messages').innerHTML = state.messagesHtml;
      }

      // 입력 활성화
      var chatInput = document.getElementById('ls-chat-input');
      chatInput.disabled = false;
      if (_selectedMenu === '제품문의') {
        chatInput.placeholder = _getRandomPlaceholder();
      } else if (_selectedMenu === '기술문의' && _selectedModel) {
        chatInput.placeholder = _getTechPlaceholder(_selectedModel.model_name);
        _fetchTechPlaceholders(_selectedModel.model_name);
      } else {
        chatInput.placeholder = '메시지를 입력하세요...';
      }
      document.getElementById('ls-chat-send').disabled = false;

      showScreen('chat');

      // WebSocket 재연결 (복원 모드 — greeting 스킵)
      _isResuming = true;
      connectWS();
      _isResuming = false;
      console.log('[AICC] session restored:', _sessionId, _selectedMenu);
    }
  }

  // ── CSS 주입 ────────────────────────────────────────────────
  const CSS = `
  #ls-chat-btn{position:fixed;bottom:24px;right:150px;z-index:99998;width:140px;height:210px;border-radius:16px;background:linear-gradient(135deg,#1a1a2e 0%,#2d2d5e 100%);color:#fff;border:none;cursor:pointer;box-shadow:0 6px 24px rgba(26,26,46,.45);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;transition:transform .15s,box-shadow .15s;padding:12px 8px}
  #ls-chat-btn:hover{transform:scale(1.03);box-shadow:0 8px 28px rgba(26,26,46,.55)}
  #ls-chat-btn svg{width:52px;height:52px;flex-shrink:0}
  #ls-chat-btn .btn-title{font-size:15px;font-weight:700;letter-spacing:-.3px;line-height:1.2}
  #ls-chat-btn .btn-title em{font-style:normal;color:#e63946}
  #ls-chat-btn .btn-desc{font-size:11px;opacity:.85;line-height:1.4;text-align:center;word-break:keep-all}
  #ls-chat-btn .btn-cta{font-size:12px;background:rgba(255,255,255,.15);border-radius:20px;padding:4px 14px;margin-top:2px}
  #ls-chat-badge{position:absolute;top:-4px;right:-4px;background:#e63946;color:#fff;border-radius:50%;width:20px;height:20px;font-size:11px;display:none;align-items:center;justify-content:center;font-weight:700}
  #ls-chat-popup{position:fixed;bottom:90px;right:150px;z-index:99999;width:380px;height:600px;border-radius:16px;background:#fff;box-shadow:0 8px 40px rgba(0,0,0,.22);display:none;flex-direction:column;overflow:hidden;font-family:'Noto Sans KR',sans-serif}
  #ls-chat-popup.open{display:flex}
  #ls-chat-header{background:#1a1a2e;color:#fff;padding:14px 16px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
  #ls-chat-header .title{font-size:15px;font-weight:700}
  #ls-chat-header .title span{color:#e63946}
  #ls-chat-close{background:none;border:none;color:#fff;font-size:20px;cursor:pointer;line-height:1;padding:0}
  #ls-chat-body{flex:1;overflow:hidden;display:flex;flex-direction:column}

  /* 메뉴 화면 */
  #ls-screen-menu{padding:20px 16px;flex:1;overflow-y:auto}
  #ls-screen-menu .greeting{font-size:14px;color:#333;margin-bottom:16px;line-height:1.6}
  .ls-menu-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px}
  .ls-menu-btn{padding:14px 8px;border:1.5px solid #e0e0e0;border-radius:10px;background:#fff;cursor:pointer;font-size:13px;font-family:inherit;text-align:center;transition:all .15s;color:#333}
  .ls-menu-btn:hover{border-color:#1a1a2e;background:#f5f7ff;color:#1a1a2e}
  .ls-menu-wide{grid-column:1/-1;display:flex;align-items:center;gap:8px;padding:14px 16px;background:linear-gradient(135deg,#f5f7ff 0%,#e8ecff 100%);border-color:#c5cdf5;margin-bottom:4px}
  .ls-menu-wide .ls-menu-icon{margin-bottom:0;font-size:22px}
  .ls-menu-wide .ls-menu-sub{font-size:11px;color:#888;margin-left:4px}
  .ls-menu-icon{display:block;font-size:20px;margin-bottom:4px}
  .ls-excel-btn{display:flex;align-items:center;gap:8px;width:100%;padding:10px 16px;border:1.5px solid #16a34a;border-radius:10px;background:linear-gradient(135deg,#f0fdf4 0%,#dcfce7 100%);cursor:pointer;font-size:13px;font-family:inherit;color:#166534;transition:all .15s;margin-top:4px}
  .ls-excel-btn:hover{background:linear-gradient(135deg,#dcfce7 0%,#bbf7d0 100%);border-color:#15803d}
  .ls-excel-btn svg{width:18px;height:18px;flex-shrink:0}
  .ls-excel-btn .excel-label{font-weight:600}
  .ls-excel-btn .excel-sub{font-size:11px;color:#6b7280;margin-left:auto}
  .ls-excel-downloading{opacity:0.6;pointer-events:none}

  /* 제품 선택 화면 */
  #ls-screen-model{padding:16px;flex:1;overflow-y:auto}
  #ls-screen-model .back-btn{background:none;border:none;color:#666;font-size:13px;cursor:pointer;padding:0 0 12px;display:flex;align-items:center;gap:4px}
  #ls-screen-model h3{font-size:14px;color:#333;margin-bottom:12px}
  #ls-model-input-wrap{position:relative;margin-bottom:8px}
  #ls-model-input{width:100%;padding:10px 12px;border:1.5px solid #ddd;border-radius:8px;font-size:14px;font-family:inherit;box-sizing:border-box;outline:none}
  #ls-model-input:focus{border-color:#1a1a2e}
  #ls-model-dropdown{position:absolute;top:100%;left:0;right:0;background:#fff;border:1.5px solid #ddd;border-radius:8px;max-height:220px;overflow-y:auto;z-index:10;box-shadow:0 4px 16px rgba(0,0,0,.1);display:none}
  #ls-model-dropdown li{padding:10px 12px;font-size:13px;cursor:pointer;border-bottom:1px solid #f0f0f0;list-style:none}
  #ls-model-dropdown li:hover{background:#f5f7ff}
  #ls-model-dropdown li .model-code{font-weight:700;color:#1a1a2e}
  #ls-model-dropdown li .model-name{color:#666;font-size:11px;display:block;margin-top:1px}
  #ls-model-next-btn{width:100%;padding:12px;background:#1a1a2e;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer;margin-top:8px;font-family:inherit}
  #ls-model-next-btn:disabled{background:#ccc;cursor:not-allowed}

  /* 채팅 화면 */
  #ls-screen-chat{flex:1;display:flex;flex-direction:column;overflow:hidden}
  #ls-chat-info{background:#f5f7ff;padding:8px 14px;font-size:12px;color:#555;border-bottom:1px solid #e8e8e8;flex-shrink:0}
  #ls-chat-info strong{color:#1a1a2e}
  #ls-chat-messages{flex:1;overflow-y:auto;padding:12px}
  .ls-msg{margin-bottom:10px;display:flex;flex-direction:column}
  .ls-msg.user{align-items:flex-end}
  .ls-msg.ai,.ls-msg.system{align-items:flex-start}
  .ls-msg-label{font-size:10px;color:#999;margin-bottom:3px}
  .ls-msg-bubble{max-width:82%;padding:9px 13px;border-radius:14px;font-size:13px;line-height:1.6;white-space:pre-wrap;word-break:break-word}
  .ls-msg.user .ls-msg-bubble{background:#1a1a2e;color:#fff;border-bottom-right-radius:4px}
  .ls-msg.ai .ls-msg-bubble{background:#f1f3f8;color:#333;border-bottom-left-radius:4px}
  .ls-msg.system .ls-msg-bubble{background:#e8f5e9;color:#2e7d32;border-radius:8px;font-size:12px;width:100%;text-align:center}
  #ls-typing{padding:10px 14px;display:none;flex-direction:column;gap:6px;font-size:12px}
  #ls-status-main{display:flex;align-items:center;gap:8px;color:#1a1a2e;font-weight:600;font-size:13px}
  #ls-status-main .dot{width:6px;height:6px;border-radius:50%;background:#1a1a2e;animation:ls-bounce .8s infinite}
  #ls-status-main .dot:nth-child(2){animation-delay:.15s}
  #ls-status-main .dot:nth-child(3){animation-delay:.3s}
  @keyframes ls-bounce{0%,80%,100%{transform:translateY(0)}40%{transform:translateY(-6px)}}
  #ls-status-details{display:flex;flex-direction:column;gap:2px;padding-left:4px;overflow:hidden}
  .ls-status-item{display:flex;align-items:center;gap:6px;font-size:11px;color:#1a1a2e;font-weight:500;animation:ls-fadeSlide .35s ease-out;max-height:28px;overflow:hidden;transition:opacity .6s ease,max-height .6s ease,margin .6s ease}
  .ls-status-item.fading{opacity:0;max-height:0;margin:0;padding:0}
  .ls-status-item .ls-status-icon{font-size:10px;width:14px;text-align:center;flex-shrink:0}
  .ls-status-item .ls-status-text{animation:ls-wave 1.5s ease-in-out infinite}
  @keyframes ls-fadeSlide{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:translateY(0)}}
  @keyframes ls-wave{0%,100%{opacity:1}50%{opacity:.55}}

  /* 추천 질문 버튼 */
  .ls-suggestions{display:flex;flex-direction:column;gap:6px;margin-top:8px;padding:0 4px}
  .ls-suggest-btn{background:#fff;border:1.5px solid #d0d5e0;border-radius:20px;padding:8px 14px;font-size:12px;color:#1a1a2e;cursor:pointer;text-align:left;transition:all .2s;font-family:inherit;line-height:1.4}
  .ls-suggest-btn:hover{background:#f0f2ff;border-color:#1a1a2e;transform:translateX(4px)}
  .ls-suggest-btn:active{transform:scale(.97)}
  #ls-img-preview{display:none;padding:6px 12px;border-top:1px solid #eee;background:#fafafa;flex-shrink:0}
  #ls-img-preview .preview-wrap{display:inline-flex;align-items:center;gap:6px;background:#fff;border:1px solid #ddd;border-radius:8px;padding:4px 8px}
  #ls-img-preview img{max-height:48px;max-width:80px;border-radius:4px;object-fit:cover}
  #ls-img-preview .remove-btn{background:none;border:none;color:#999;cursor:pointer;font-size:16px;padding:0;line-height:1}
  #ls-chat-bottom{padding:10px 12px;border-top:1px solid #eee;display:flex;gap:8px;flex-shrink:0;background:#fff;align-items:flex-end}
  #ls-chat-img-btn{width:38px;height:38px;border-radius:50%;background:none;border:1.5px solid #ddd;cursor:pointer;font-size:18px;flex-shrink:0;display:flex;align-items:center;justify-content:center;color:#666;transition:all .15s}
  #ls-chat-img-btn:hover{border-color:#1a1a2e;color:#1a1a2e}
  #ls-chat-input{flex:1;padding:9px 12px;border:1.5px solid #ddd;border-radius:20px;font-size:13px;font-family:inherit;outline:none;resize:none;max-height:80px;line-height:1.4}
  #ls-chat-input:focus{border-color:#1a1a2e}
  #ls-chat-send{width:38px;height:38px;border-radius:50%;background:#1a1a2e;color:#fff;border:none;cursor:pointer;font-size:16px;flex-shrink:0;display:flex;align-items:center;justify-content:center}
  #ls-chat-send:disabled{background:#ccc;cursor:not-allowed}
  .ls-msg-img{max-width:200px;border-radius:8px;margin-bottom:6px;cursor:pointer}
  .ls-msg-img:hover{opacity:.9}
  .ls-yt-card{display:block;text-decoration:none!important;background:#fff;border:1px solid #e0e0e0;border-radius:10px;overflow:hidden;margin:8px 0 4px;transition:box-shadow .2s}
  .ls-yt-card:hover{box-shadow:0 2px 8px rgba(0,0,0,.15)}
  .ls-yt-thumb-wrap{position:relative;width:100%;aspect-ratio:16/9;background:#000}
  .ls-yt-thumb{width:100%;height:100%;object-fit:cover;display:block}
  .ls-yt-play{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);opacity:.85;transition:opacity .2s}
  .ls-yt-card:hover .ls-yt-play{opacity:1}
  .ls-yt-title{padding:8px 10px;font-size:12px;font-weight:600;color:#1a1a2e;line-height:1.4;white-space:normal}
  #ls-chat-file-input{display:none}
  /* 재고 결과 화면 */
  #ls-screen-inventory{padding:16px;flex:1;overflow-y:auto}
  .ls-inv-card{border:1.5px solid #e0e0e0;border-radius:10px;padding:16px;margin-bottom:12px}
  .ls-inv-model{font-size:15px;font-weight:700;color:#1a1a2e;margin-bottom:12px}
  .ls-inv-row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f0f0f0;font-size:13px}
  .ls-inv-row:last-child{border-bottom:none;font-weight:700;color:#e63946}
  .ls-inv-empty{text-align:center;padding:24px;color:#999;font-size:13px}

  /* 견적 안내 화면 */
  #ls-screen-quote{padding:20px 16px;flex:1;overflow-y:auto}
  .ls-quote-info{background:#f5f7ff;border-radius:10px;padding:16px;font-size:13px;line-height:1.8}
  .ls-quote-info h3{font-size:14px;font-weight:700;color:#1a1a2e;margin-bottom:10px}
  .ls-quote-info ol{padding-left:16px;color:#555}
  .ls-quote-link{display:block;text-align:center;margin-top:14px;padding:12px;background:#1a1a2e;color:#fff;border-radius:8px;text-decoration:none;font-size:14px}

  /* 주문조회 화면 */
  #ls-screen-orders{padding:16px;flex:1;overflow-y:auto;display:flex;flex-direction:column}
  .ls-login-prompt{text-align:center;padding:40px 16px}
  .ls-login-prompt p{font-size:14px;color:#333;margin-bottom:16px;line-height:1.6}
  .ls-login-btn{display:inline-block;padding:12px 24px;background:#1a1a2e;color:#fff;border-radius:8px;text-decoration:none;font-size:14px}
  .ls-phone-form{padding:16px 0}
  .ls-phone-form p{font-size:14px;color:#333;margin-bottom:12px;line-height:1.6}
  .ls-phone-form input{width:100%;padding:10px 12px;border:1.5px solid #ddd;border-radius:8px;font-size:14px;box-sizing:border-box;font-family:inherit}
  .ls-phone-form input:focus{border-color:#1a1a2e;outline:none}
  .ls-phone-form button{width:100%;padding:12px;background:#1a1a2e;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer;margin-top:8px;font-family:inherit}
  .ls-orders-label{font-size:13px;color:#666;margin-bottom:8px}
  .ls-orders-scroll{display:flex;overflow-x:auto;scroll-snap-type:x mandatory;gap:12px;padding:4px 0 8px;-webkit-overflow-scrolling:touch;scrollbar-width:none}
  .ls-orders-scroll::-webkit-scrollbar{display:none}
  .ls-order-card{min-width:calc(100% - 8px);max-width:calc(100% - 8px);scroll-snap-align:start;border:1.5px solid #e0e0e0;border-radius:10px;padding:16px;flex-shrink:0;box-sizing:border-box;background:#fff}
  .ls-order-row{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid #f0f0f0;font-size:13px}
  .ls-order-row:last-child{border-bottom:none}
  .ls-order-row .label{color:#666;white-space:nowrap;margin-right:12px}
  .ls-order-row .value{color:#333;font-weight:500;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:65%}
  .ls-order-status{display:inline-block;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600}
  .ls-status-shipping{background:#e3f2fd;color:#1565c0}
  .ls-status-complete{background:#e8f5e9;color:#2e7d32}
  .ls-status-pending{background:#fff3e0;color:#e65100}
  .ls-status-cancel{background:#fce4ec;color:#c62828}
  .ls-status-default{background:#f5f5f5;color:#666}
  .ls-order-btns{display:flex;flex-direction:column;gap:6px;margin-top:12px}
  .ls-order-btns a,.ls-order-btns button{display:block;width:100%;padding:10px;text-align:center;border:1.5px solid #e0e0e0;border-radius:8px;font-size:13px;text-decoration:none;color:#333;background:#fff;cursor:pointer;box-sizing:border-box;font-family:inherit}
  .ls-order-btns a:hover,.ls-order-btns button:hover{background:#f5f7ff;border-color:#1a1a2e}
  .ls-order-dots{display:flex;justify-content:center;gap:6px;padding:8px 0}
  .ls-order-dot{width:8px;height:8px;border-radius:50%;background:#ddd;transition:background .2s}
  .ls-order-dot.active{background:#1a1a2e}
  .ls-order-bottom{display:flex;gap:8px;margin-top:auto;padding-top:12px}
  .ls-order-bottom a,.ls-order-bottom button{flex:1;padding:10px;border-radius:20px;font-size:13px;text-align:center;cursor:pointer;text-decoration:none;border:1.5px solid #ddd;background:#fff;color:#333;font-family:inherit}
  .ls-order-bottom .primary{border-color:#e63946;color:#e63946}
  .ls-order-empty{text-align:center;padding:32px 16px;color:#999;font-size:13px;line-height:1.6}

  @media(max-width:768px){
    #ls-chat-btn{right:12px;bottom:12px;width:60px;height:60px;border-radius:50%;padding:0;gap:0}
    #ls-chat-btn svg{width:28px;height:28px}
    #ls-chat-btn .btn-title,#ls-chat-btn .btn-desc,#ls-chat-btn .btn-cta{display:none}
    #ls-chat-badge{top:-2px;right:-2px}
    #ls-chat-popup{width:100vw;height:100vh;height:100dvh;bottom:0;right:0;border-radius:0;top:0;left:0}
    #ls-chat-popup.open{display:flex}
    #ls-chat-messages{padding:10px 8px}
    .ls-msg-bubble{max-width:88%;font-size:13px}
    #ls-chat-bottom{padding:8px 10px}
    #ls-chat-input{font-size:14px}
  }
  `;

  // ── 리뷰 기반 랜덤 예시 질문 로드 ──────────────────────
  function _fetchPlaceholderExamples() {
    if (_placeholderExamples.length > 0) return;
    fetch(BACKEND + '/api/aicc/placeholder-examples?count=10')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.examples && data.examples.length) {
          _placeholderExamples = data.examples;
          console.log('[AICC] 예시 질문 로드:', _placeholderExamples.length + '개');
        }
      })
      .catch(function(e) { console.log('[AICC] 예시 질문 로드 실패:', e); });
  }

  function _getRandomPlaceholder() {
    if (_placeholderExamples.length === 0) return '예: HDMI 케이블 추천해주세요';
    return '예: ' + _placeholderExamples[Math.floor(Math.random() * _placeholderExamples.length)];
  }

  function _fetchTechPlaceholders(modelName) {
    _techPlaceholders = [];
    if (!modelName) return;
    fetch(BACKEND + '/api/aicc/placeholder-examples/tech?model=' + encodeURIComponent(modelName) + '&count=8')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.examples && data.examples.length) {
          _techPlaceholders = data.examples;
          console.log('[AICC] 기술문의 예시 질문 로드:', _techPlaceholders.length + '개 (' + modelName + ')');
          // 이미 채팅 화면이면 placeholder 즉시 갱신
          var chatInput = document.getElementById('ls-chat-input');
          if (chatInput && _selectedMenu === '기술문의') {
            chatInput.placeholder = _getTechPlaceholder(modelName);
          }
        }
      })
      .catch(function(e) { console.log('[AICC] 기술문의 예시 로드 실패:', e); });
  }

  function _getTechPlaceholder(modelName) {
    if (_techPlaceholders.length === 0) {
      return '예: ' + (modelName || '') + ' 연결 방법은?';
    }
    return '예: ' + _techPlaceholders[Math.floor(Math.random() * _techPlaceholders.length)];
  }

  function injectCSS() {
    if (document.getElementById('ls-chat-css')) return;
    const style = document.createElement('style');
    style.id = 'ls-chat-css';
    style.textContent = CSS;
    document.head.appendChild(style);
  }

  // ── HTML 구조 생성 ─────────────────────────────────────────
  function createHTML() {
    if (document.getElementById('ls-chat-btn')) return;

    document.body.insertAdjacentHTML('beforeend', `
      <!-- 팝업 버튼 -->
      <button id="ls-chat-btn" title="AI 상담">
        <svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
          <rect x="12" y="18" width="40" height="28" rx="6" fill="#fff" opacity=".95"/>
          <rect x="20" y="26" width="4" height="5" rx="2" fill="#1a1a2e"/>
          <rect x="40" y="26" width="4" height="5" rx="2" fill="#1a1a2e"/>
          <path d="M26 36c0 0 3 4 6 4s6-4 6-4" stroke="#1a1a2e" stroke-width="2" stroke-linecap="round"/>
          <circle cx="32" cy="12" r="3" fill="#e63946"/>
          <line x1="32" y1="15" x2="32" y2="18" stroke="#e63946" stroke-width="2"/>
          <rect x="8" y="30" width="4" height="6" rx="2" fill="#fff" opacity=".7"/>
          <rect x="52" y="30" width="4" height="6" rx="2" fill="#fff" opacity=".7"/>
          <path d="M24 46l-4 6" stroke="#fff" stroke-width="2" stroke-linecap="round" opacity=".7"/>
          <path d="M40 46l4 6" stroke="#fff" stroke-width="2" stroke-linecap="round" opacity=".7"/>
        </svg>
        <div class="btn-title"><em>LAN</em>star AI</div>
        <div class="btn-desc">제품사용법, 배송/재고 조회<br>견적문의 무엇이든 물어보세요</div>
        <div class="btn-cta">💬 상담 시작</div>
        <span id="ls-chat-badge"></span>
      </button>

      <!-- 팝업 본체 -->
      <div id="ls-chat-popup">
        <div id="ls-chat-header">
          <div class="title"><span>LAN</span>star AI 상담</div>
          <button id="ls-chat-close">\u00D7</button>
        </div>
        <div id="ls-chat-body">

          <!-- 화면1: 메뉴 선택 -->
          <div id="ls-screen-menu">
            <p class="greeting">안녕하세요! 랜스타 AI 상담사입니다.<br>무엇을 도와드릴까요?</p>
            <button class="ls-menu-btn ls-menu-wide" data-menu="\uC81C\uD488\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDD0D</span>\uC81C\uD488 \uBB38\uC758<span class="ls-menu-sub">\uC6A9\uB3C4\u00B7\uADDC\uACA9\uC5D0 \uB9DE\uB294 \uC81C\uD488 \uCD94\uCC9C</span></button>
            <div class="ls-menu-grid">
              <button class="ls-menu-btn" data-menu="\uAE30\uC220\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDCCB</span>\uAE30\uC220 \uBB38\uC758</button>
              <button class="ls-menu-btn" data-menu="\uBC30\uC1A1\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDE9A</span>\uBC30\uC1A1 \uBB38\uC758</button>
              <button class="ls-menu-btn" data-menu="\uC7AC\uACE0\uC870\uD68C"><span class="ls-menu-icon">\uD83D\uDCE6</span>\uC7AC\uACE0 \uC870\uD68C</button>
              <button class="ls-menu-btn" data-menu="\uACAC\uC801\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDCC4</span>\uACAC\uC801 \uBB38\uC758</button>
            </div>
            <div id="ls-excel-download-wrap"></div>
          </div>

          <!-- 화면2: 제품 선택 (제품/기술/재고문의 시) -->
          <div id="ls-screen-model" style="display:none">
            <button class="back-btn" onclick="LanstarChat._goBack()">\u2190 \uB4A4\uB85C</button>
            <h3 id="ls-model-screen-title">\uBB38\uC758\uD558\uC2E4 \uC81C\uD488 \uBAA8\uB378\uBA85\uC744 \uC785\uB825\uD574 \uC8FC\uC138\uC694</h3>
            <div id="ls-model-input-wrap">
              <input id="ls-model-input" type="text" placeholder="\uC608: LS-HDMI-5M, LS-1000H" autocomplete="off">
              <ul id="ls-model-dropdown"></ul>
            </div>
            <p id="ls-selected-model-info" style="font-size:12px;color:#1a1a2e;margin:6px 0;display:none"></p>
            <button id="ls-model-next-btn" disabled onclick="LanstarChat._confirmModel()">\uB2E4\uC74C \u2192</button>
          </div>

          <!-- 화면3: 채팅 -->
          <div id="ls-screen-chat" style="display:none">
            <div id="ls-chat-info">
              <button id="ls-chat-back" onclick="LanstarChat._chatBack()" style="background:none;border:none;cursor:pointer;font-size:12px;color:#666;padding:0;margin-right:8px">← 뒤로</button>
              <span id="ls-chat-info-text"></span>
            </div>
            <div id="ls-chat-messages"></div>
            <div id="ls-typing">
              <div id="ls-status-main"><span class="dot"></span><span class="dot"></span><span class="dot"></span><span id="ls-status-step">AI\uAC00 \uB2F5\uBCC0 \uC900\uBE44 \uC911...</span></div>
              <div id="ls-status-details"></div>
            </div>
            <div id="ls-img-preview"></div>
            <input type="file" id="ls-chat-file-input" accept="image/jpeg,image/png,image/gif,image/webp">
            <div id="ls-chat-bottom">
              <button id="ls-chat-img-btn" title="\uC0AC\uC9C4 \uCCA8\uBD80">\uD83D\uDCF7</button>
              <textarea id="ls-chat-input" placeholder="\uBA54\uC2DC\uC9C0\uB97C \uC785\uB825\uD558\uC138\uC694..." rows="1"></textarea>
              <button id="ls-chat-send">\u2191</button>
            </div>
          </div>

          <!-- 화면4: 재고 결과 -->
          <div id="ls-screen-inventory" style="display:none">
            <button class="back-btn" onclick="LanstarChat._goBack()" style="padding:0 0 12px">\u2190 \uB4A4\uB85C</button>
            <div id="ls-inv-result"></div>
          </div>

          <!-- 화면5: 견적 안내 -->
          <div id="ls-screen-quote" style="display:none">
            <button class="back-btn" onclick="LanstarChat._goBack()" style="padding:0 0 12px">\u2190 \uB4A4\uB85C</button>
            <div class="ls-quote-info">
              <h3>\uD83D\uDCC4 \uACAC\uC801\uC11C \uCD9C\uB825 \uC548\uB0B4</h3>
              <ol>
                <li>\uC6D0\uD558\uC2DC\uB294 \uC81C\uD488\uC744 \uC7A5\uBC14\uAD6C\uB2C8\uC5D0 \uB2F4\uC544\uC8FC\uC138\uC694</li>
                <li>\uC7A5\uBC14\uAD6C\uB2C8\uC5D0\uC11C <strong>"\uACAC\uC801\uC11C \uCD9C\uB825"</strong> \uBC84\uD2BC\uC744 \uD074\uB9AD\uD558\uC138\uC694</li>
                <li>PDF \uCD9C\uB825 \uB610\uB294 \uC800\uC7A5\uC774 \uAC00\uB2A5\uD569\uB2C8\uB2E4</li>
              </ol>
              <a class="ls-quote-link" href="https://www.lanstar.co.kr/order/cart.php">\uC7A5\uBC14\uAD6C\uB2C8 \uBC14\uB85C\uAC00\uAE30 \u2192</a>
            </div>
          </div>

          <!-- 화면6: 주문조회 -->
          <div id="ls-screen-orders" style="display:none">
            <button class="back-btn" onclick="LanstarChat._goBack()" style="background:none;border:none;color:#666;font-size:13px;cursor:pointer;padding:0 0 12px;display:flex;align-items:center;gap:4px">\u2190 \uB4A4\uB85C</button>
            <div id="ls-orders-content"></div>
          </div>

        </div>
      </div>
    `);
  }

  // ── 화면 전환 ──────────────────────────────────────────────
  function showScreen(name) {
    ['menu', 'model', 'chat', 'inventory', 'quote', 'orders'].forEach(function(s) {
      var el = document.getElementById('ls-screen-' + s);
      if (el) el.style.display = s === name ? '' : 'none';
    });
  }

  // ── 엑셀 다운로드 (LV2 이상만) ──────────────────────────────
  function renderExcelButton() {
    var wrap = document.getElementById('ls-excel-download-wrap');
    if (!wrap) return;
    if (_memberLevel < 2) { wrap.innerHTML = ''; return; }

    var levelLabel = _memberLevel >= 3 ? '업체회원' : '사업자회원';
    wrap.innerHTML =
      '<button class="ls-excel-btn" id="ls-excel-btn" onclick="LanstarChat._downloadExcel()">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="12" y1="18" x2="12" y2="12"/><polyline points="9 15 12 18 15 15"/></svg>' +
        '<span class="excel-label">상품 엑셀 다운로드</span>' +
        '<span class="excel-sub">' + levelLabel + '</span>' +
      '</button>';
  }

  function downloadExcel() {
    var btn = document.getElementById('ls-excel-btn');
    if (!btn || btn.classList.contains('ls-excel-downloading')) return;

    btn.classList.add('ls-excel-downloading');
    btn.querySelector('.excel-label').textContent = '다운로드 중...';

    var url = BACKEND + '/api/aicc/goods-excel?level=' + _memberLevel;

    fetch(url)
      .then(function(res) {
        if (!res.ok) throw new Error('다운로드 실패 (' + res.status + ')');
        return res.blob();
      })
      .then(function(blob) {
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        var today = new Date().toISOString().slice(0,10).replace(/-/g,'');
        a.download = 'lanstar_goods_' + today + '.xlsx';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(a.href);
      })
      .catch(function(err) {
        alert('엑셀 다운로드 실패: ' + err.message);
      })
      .finally(function() {
        btn.classList.remove('ls-excel-downloading');
        btn.querySelector('.excel-label').textContent = '상품 엑셀 다운로드';
      });
  }

  // ── 이벤트 바인딩 ──────────────────────────────────────────
  function bindEvents() {
    document.getElementById('ls-chat-btn').addEventListener('click', function() { toggle(); });
    document.getElementById('ls-chat-close').addEventListener('click', function() { closePopup(); });

    // 메뉴 버튼
    document.querySelectorAll('.ls-menu-btn').forEach(function(btn) {
      btn.addEventListener('click', function () {
        onMenuSelect(this.dataset.menu);
      });
    });

    // 모델 검색
    var inp = document.getElementById('ls-model-input');
    inp.addEventListener('input', debounce(onModelSearch, 200));
    inp.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') hideDropdown();
    });
    document.addEventListener('click', function(e) {
      if (!document.getElementById('ls-model-input-wrap').contains(e.target)) hideDropdown();
    });

    // 이미지 업로드
    document.getElementById('ls-chat-img-btn').addEventListener('click', function() {
      document.getElementById('ls-chat-file-input').click();
    });
    document.getElementById('ls-chat-file-input').addEventListener('change', onImageSelected);

    // 채팅 전송 (중복 방지: 이벤트 리스너 1개만)
    var sendBtn = document.getElementById('ls-chat-send');
    var chatInp = document.getElementById('ls-chat-input');

    sendBtn.addEventListener('click', sendMessage);

    // 한글 IME 조합 상태 추적
    var _isComposing = false;
    chatInp.addEventListener('compositionstart', function() { _isComposing = true; });
    chatInp.addEventListener('compositionend', function() { _isComposing = false; });

    chatInp.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' && !e.shiftKey && !_isComposing) {
        e.preventDefault();
        sendMessage();
      }
    });
    // 자동 높이 조절
    chatInp.addEventListener('input', function () {
      this.style.height = 'auto';
      this.style.height = Math.min(this.scrollHeight, 80) + 'px';
    });
  }

  // ── 팝업 열기/닫기 ─────────────────────────────────────────
  function toggle() {
    var popup = document.getElementById('ls-chat-popup');
    var isOpen = popup.classList.contains('open');
    if (isOpen) closePopup();
    else openPopup();
  }

  function openPopup() {
    document.getElementById('ls-chat-popup').classList.add('open');
    if (!_allModels.length) loadModels();
    _saveState();
  }

  function closePopup() {
    document.getElementById('ls-chat-popup').classList.remove('open');
    _saveState();
  }

  // ── 메뉴 선택 ─────────────────────────────────────────────
  function onMenuSelect(menu) {
    _selectedMenu = menu;
    _selectedModel = null;

    if (menu === '\uACAC\uC801\uBB38\uC758') {
      showScreen('quote');
      return;
    }
    if (menu === '\uBC30\uC1A1\uBB38\uC758') {
      showOrdersScreen();
      return;
    }
    if (menu === '\uC81C\uD488\uBB38\uC758') {
      // 제품문의: 모델 선택 없이 바로 채팅
      startChat(null);
      return;
    }
    // 기술문의, 재고조회 → 제품 선택 화면
    document.getElementById('ls-model-screen-title').textContent =
      menu === '\uC7AC\uACE0\uC870\uD68C' ? '\uC7AC\uACE0\uB97C \uC870\uD68C\uD560 \uC81C\uD488 \uBAA8\uB378\uBA85\uC744 \uC785\uB825\uD574 \uC8FC\uC138\uC694' : '\uBB38\uC758\uD558\uC2E4 \uC81C\uD488 \uBAA8\uB378\uBA85\uC744 \uC785\uB825\uD574 \uC8FC\uC138\uC694';
    document.getElementById('ls-model-input').value = '';
    document.getElementById('ls-selected-model-info').style.display = 'none';
    document.getElementById('ls-model-next-btn').disabled = true;
    showScreen('model');
    setTimeout(function() { document.getElementById('ls-model-input').focus(); }, 100);
  }

  // ── 모델 검색 ─────────────────────────────────────────────
  async function loadModels() {
    try {
      var res = await fetch(BACKEND + '/api/aicc/models');
      _allModels = await res.json();
    } catch (e) {
      console.warn('[AICC] \uBAA8\uB378 \uBAA9\uB85D \uB85C\uB4DC \uC2E4\uD328:', e);
    }
  }

  function onModelSearch() {
    var q = document.getElementById('ls-model-input').value.trim().toUpperCase();
    if (q.length < 2) { hideDropdown(); return; }

    var matched = _allModels.filter(function(m) {
      return m.model_name.toUpperCase().includes(q) ||
        m.product_name.toUpperCase().includes(q);
    }).slice(0, 15);

    if (!matched.length) { hideDropdown(); return; }

    var ul = document.getElementById('ls-model-dropdown');
    ul.innerHTML = matched.map(function(m) {
      return '<li onclick="LanstarChat._selectModel(' + escAttr(JSON.stringify(m)) + ')">' +
        '<span class="model-code">' + escHtml(m.model_name) + '</span>' +
        '<span class="model-name">' + escHtml(m.product_name.replace('[LANstar] ', '').substring(0, 45)) + '</span>' +
        '</li>';
    }).join('');
    ul.style.display = 'block';
  }

  function _selectModel(model) {
    if (typeof model === 'string') model = JSON.parse(model);
    _selectedModel = model;
    document.getElementById('ls-model-input').value = model.model_name;
    document.getElementById('ls-selected-model-info').textContent = '\u2713 ' + model.model_name;
    document.getElementById('ls-selected-model-info').style.display = 'block';
    document.getElementById('ls-model-next-btn').disabled = false;
    hideDropdown();
  }

  function hideDropdown() {
    document.getElementById('ls-model-dropdown').style.display = 'none';
  }

  async function _confirmModel() {
    if (!_selectedModel) return;

    if (_selectedMenu === '\uC7AC\uACE0\uC870\uD68C') {
      await fetchInventory();
      return;
    }
    startChat(_selectedModel);
  }

  // ── 재고조회 ───────────────────────────────────────────────
  async function fetchInventory() {
    showScreen('inventory');
    var res_el = document.getElementById('ls-inv-result');
    res_el.innerHTML = '<p style="text-align:center;color:#999;padding:24px">\uC870\uD68C \uC911...</p>';

    try {
      var res = await fetch(BACKEND + '/api/aicc/inventory/' + encodeURIComponent(_selectedModel.model_name));
      var data = await res.json();

      if (!data.ok) {
        res_el.innerHTML = '<div class="ls-inv-empty">' + escHtml(data.message) + '</div>';
        return;
      }

      var stockColor = data.total > 0 ? '#e63946' : '#999';
      res_el.innerHTML =
        '<div class="ls-inv-card">' +
          '<div class="ls-inv-model">' + escHtml(data.model_name) + '</div>' +
          '<div class="ls-inv-row"><span>\uC6A9\uC0B0 \uCC3D\uACE0</span><span>' + data.yongsan + '\uAC1C</span></div>' +
          '<div class="ls-inv-row"><span>\uAE40\uD3EC \uCC3D\uACE0</span><span>' + data.gimpo + '\uAC1C</span></div>' +
          '<div class="ls-inv-row"><span>\uCD1D \uC7AC\uACE0</span><span style="color:' + stockColor + '">' + data.total + '\uAC1C</span></div>' +
        '</div>' +
        (data.total === 0 ? '<p style="font-size:12px;color:#999;text-align:center">\uD604\uC7AC \uC7AC\uACE0\uAC00 \uC5C6\uC2B5\uB2C8\uB2E4. \uC785\uACE0 \uC2DC \uC54C\uB9BC\uC774 \uD544\uC694\uD558\uC2DC\uBA74 \uC804\uD654(02-717-3386)\uB85C \uBB38\uC758\uD574 \uC8FC\uC138\uC694.</p>' : '');
    } catch (e) {
      res_el.innerHTML = '<div class="ls-inv-empty">\uC870\uD68C \uC911 \uC624\uB958\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4. \uC804\uD654(02-717-3386)\uB85C \uBB38\uC758\uD574 \uC8FC\uC138\uC694.</div>';
    }
  }

  // ── 채팅 시작 ─────────────────────────────────────────────
  function startChat(model) {
    _selectedModel = model;
    _sessionId = 'aicc-' + Date.now() + '-' + Math.random().toString(36).substr(2, 6);
    _isSending = false;

    // 채팅 화면 초기화
    document.getElementById('ls-chat-messages').innerHTML = '';
    var chatInput = document.getElementById('ls-chat-input');
    chatInput.value = '';
    chatInput.disabled = false;
    if (_selectedMenu === '제품문의') {
      chatInput.placeholder = _getRandomPlaceholder();
    } else if (_selectedMenu === '기술문의' && model) {
      chatInput.placeholder = _getTechPlaceholder(model.model_name);
      _fetchTechPlaceholders(model.model_name);
    } else {
      chatInput.placeholder = '메시지를 입력하세요...';
    }
    document.getElementById('ls-chat-send').disabled = false;

    // 상단 정보 표시 (뒤로가기 버튼 유지)
    var modelInfo = model ? '<strong>' + escHtml(model.model_name) + '</strong> | ' : '';
    document.getElementById('ls-chat-info-text').innerHTML =
      modelInfo + '<strong>' + escHtml(_selectedMenu) + '</strong>';

    showScreen('chat');
    connectWS();
    _saveState();
  }

  // ── WebSocket 연결 ─────────────────────────────────────────
  var _isResuming = false;  // 세션 복원 중 플래그

  function connectWS() {
    if (_ws) { _ws.close(); _ws = null; }

    var wsProto = BACKEND.startsWith('https') ? 'wss' : 'ws';
    var wsHost = BACKEND.replace(/^https?:\/\//, '');
    var modelName = _selectedModel ? _selectedModel.model_name : '';
    var erpCode = _selectedModel ? _selectedModel.erp_code : '';

    var url = wsProto + '://' + wsHost + '/ws/aicc/chat/' + _sessionId
      + '?name=' + encodeURIComponent(_memberName)
      + '&model=' + encodeURIComponent(modelName)
      + '&erp_code=' + encodeURIComponent(erpCode)
      + '&menu=' + encodeURIComponent(_selectedMenu)
      + (_isResuming ? '&resume=true' : '');

    _ws = new WebSocket(url);

    _ws.onmessage = function (e) {
      var msg = JSON.parse(e.data);
      switch (msg.type) {
        case 'status':
          updateStatus(msg.step, msg.detail || '');
          break;
        case 'ai_message':
          hideTyping();
          appendMsg('ai', 'AI \uC0C1\uB2F4\uC0AC', msg.content, null, true);
          if (msg.suggestions && msg.suggestions.length) {
            appendSuggestions(msg.suggestions);
          }
          break;
        case 'session_closed':
          hideTyping();
          appendMsg('system', '', msg.content);
          document.getElementById('ls-chat-input').disabled = true;
          document.getElementById('ls-chat-send').disabled = true;
          if (_ws) _ws.close();
          break;
        case 'system':
          hideTyping();
          appendMsg('system', '', msg.content);
          break;
      }
    };

    _ws.onerror = function () {
      appendMsg('system', '', '\uC5F0\uACB0 \uC624\uB958\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4. \uC7A0\uC2DC \uD6C4 \uB2E4\uC2DC \uC2DC\uB3C4\uD574 \uC8FC\uC138\uC694.');
    };

    _ws.onclose = function () {
      if (_sessionId) {
        setTimeout(function() {
          var inp = document.getElementById('ls-chat-input');
          if (inp && !inp.disabled) reconnectWS();
        }, 3000);
      }
    };
  }

  function reconnectWS() {
    if (!_sessionId || (_ws && _ws.readyState === WebSocket.OPEN)) return;
    _isResuming = true;
    connectWS();
    _isResuming = false;
  }

  // ── 이미지 업로드 ──────────────────────────────────────────
  var _pendingImage = null;  // { base64: "data:image/...", name: "file.jpg" }

  function onImageSelected() {
    var fileInput = document.getElementById('ls-chat-file-input');
    var file = fileInput.files[0];
    fileInput.value = '';
    if (!file) return;

    // 파일 타입 검증
    if (!file.type.match(/^image\/(jpeg|png|gif|webp)$/)) {
      alert('JPG, PNG, GIF, WEBP 이미지만 업로드 가능합니다.');
      return;
    }
    // 크기 제한 (4MB)
    if (file.size > 4 * 1024 * 1024) {
      alert('이미지 크기는 4MB 이하만 가능합니다.');
      return;
    }

    var reader = new FileReader();
    reader.onload = function(e) {
      _pendingImage = { base64: e.target.result, name: file.name };
      // 미리보기 표시
      var preview = document.getElementById('ls-img-preview');
      preview.innerHTML =
        '<div class="preview-wrap">' +
          '<img src="' + e.target.result + '">' +
          '<span style="font-size:11px;color:#666">' + escHtml(file.name.substring(0, 15)) + '</span>' +
          '<button class="remove-btn" onclick="LanstarChat._clearImage()">\u00D7</button>' +
        '</div>';
      preview.style.display = 'block';
    };
    reader.readAsDataURL(file);
  }

  function _clearImage() {
    _pendingImage = null;
    var preview = document.getElementById('ls-img-preview');
    preview.innerHTML = '';
    preview.style.display = 'none';
  }

  async function uploadImage(base64Data, fileName) {
    try {
      var res = await fetch(BACKEND + '/api/aicc/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: _sessionId,
          image: base64Data,
          file_name: fileName
        })
      });
      return await res.json();
    } catch (e) {
      return { ok: false, message: '이미지 업로드 실패' };
    }
  }

  // ── 메시지 전송 (중복 전송 완벽 방지) ────────────────────────
  function sendMessage() {
    if (_isSending) return;

    var inp = document.getElementById('ls-chat-input');
    var text = inp.value.trim();
    var hasImage = !!_pendingImage;
    if (!text && !hasImage) return;
    if (!_ws || _ws.readyState !== WebSocket.OPEN) {
      appendMsg('system', '', '\uC5F0\uACB0\uC774 \uB04A\uACBC\uC2B5\uB2C8\uB2E4. \uC7A0\uC2DC \uD6C4 \uB2E4\uC2DC \uC2DC\uB3C4\uD574 \uC8FC\uC138\uC694.');
      return;
    }

    _isSending = true;

    // 1. 입력창 즉시 초기화 + 예시 placeholder 제거
    inp.value = '';
    inp.style.height = 'auto';
    inp.placeholder = '\uBA54\uC2DC\uC9C0\uB97C \uC785\uB825\uD558\uC138\uC694...';

    // 이미지 캡처 후 미리보기 제거
    var imageData = _pendingImage;
    if (hasImage) _clearImage();

    // 2. 사용자 메시지 UI에 표시 (이미지 포함)
    appendMsg('user', '\uB098', text || '\uC0AC\uC9C4\uC744 \uBCF4\uB0C8\uC2B5\uB2C8\uB2E4.', imageData ? imageData.base64 : null);
    showTyping();

    // 3. 이미지가 있으면 업로드 후 WS 전송
    if (imageData) {
      uploadImage(imageData.base64, imageData.name).then(function(result) {
        if (result.ok) {
          _ws.send(JSON.stringify({ type: 'chat', content: text || '이 사진에 대해 알려주세요.', image_id: result.image_id }));
        } else {
          hideTyping();
          appendMsg('system', '', '\uC774\uBBF8\uC9C0 \uC5C5\uB85C\uB4DC \uC2E4\uD328: ' + (result.message || ''));
        }
        setTimeout(function() { _isSending = false; }, 500);
      });
      return;
    }

    // 3. WebSocket 전송 (텍스트만)
    _ws.send(JSON.stringify({ type: 'chat', content: text }));

    // 4. 0.5초 후 플래그 해제
    setTimeout(function() { _isSending = false; }, 500);
  }

  // ── UI 헬퍼 ──────────────────────────────────────────────
  var _lastAiMsgDiv = null;  // 마지막 AI 메시지 요소 추적

  function appendMsg(type, label, content, imageBase64, deferScroll) {
    var mc = document.getElementById('ls-chat-messages');
    var div = document.createElement('div');
    div.className = 'ls-msg ' + type;
    // AI/관리자 응답은 URL 링크 + 볼드 변환, 사용자 메시지는 이스케이프만
    var rendered = (type === 'ai' || type === 'assistant') ? formatMsg(content) : escHtml(content);
    var imgHtml = imageBase64 ? '<img class="ls-msg-img" src="' + imageBase64 + '" onclick="window.open(this.src)">' : '';
    if (label && type !== 'system') {
      div.innerHTML = '<span class="ls-msg-label">' + escHtml(label) + '</span><div class="ls-msg-bubble">' + imgHtml + rendered + '</div>';
    } else {
      div.innerHTML = '<div class="ls-msg-bubble">' + imgHtml + rendered + '</div>';
    }
    mc.appendChild(div);

    if (type === 'ai' || type === 'assistant') {
      _lastAiMsgDiv = div;
    }

    if (!deferScroll) {
      if (type === 'user') {
        mc.scrollTop = mc.scrollHeight;
      } else if (type === 'ai' || type === 'assistant') {
        _scrollToAiStart(mc, div);
      } else {
        mc.scrollTop = mc.scrollHeight;
      }
    }
    // 상태 저장
    _saveState();
  }

  function _scrollToAiStart(mc, div) {
    // AI 답변의 "AI 상담사" 라벨 부분이 보이도록 스크롤
    // 라벨 위치 = div 상단 - 약간의 여백
    setTimeout(function() {
      var msgTop = div.offsetTop - mc.offsetTop - 12;
      mc.scrollTo({ top: msgTop, behavior: 'smooth' });
    }, 50);
  }

  function showTyping() {
    var el = document.getElementById('ls-typing');
    var details = document.getElementById('ls-status-details');
    var step = document.getElementById('ls-status-step');
    if (details) details.innerHTML = '';
    if (step) step.textContent = 'AI가 답변 준비 중...';
    el.style.display = 'flex';
    var mc = document.getElementById('ls-chat-messages');
    if (mc) mc.scrollTop = mc.scrollHeight;
  }

  function hideTyping() {
    var el = document.getElementById('ls-typing');
    el.style.display = 'none';
    var details = document.getElementById('ls-status-details');
    if (details) details.innerHTML = '';
    _statusItemCount = 0;
    // 페이드 타이머 모두 해제
    for (var i = 0; i < _statusFadeTimers.length; i++) {
      clearTimeout(_statusFadeTimers[i]);
    }
    _statusFadeTimers = [];
  }

  var _statusItemCount = 0;
  var _statusFadeTimers = [];  // 페이드 타이머 추적

  function updateStatus(step, detail) {
    var el = document.getElementById('ls-typing');
    if (el.style.display === 'none') el.style.display = 'flex';

    // 메인 상태 텍스트 업데이트
    var stepEl = document.getElementById('ls-status-step');
    if (stepEl && step) {
      stepEl.textContent = step;
    }

    if (detail) {
      var details = document.getElementById('ls-status-details');
      if (!details) return;

      _statusItemCount++;
      var item = document.createElement('div');
      item.className = 'ls-status-item';
      item.setAttribute('data-ts', Date.now());
      item.innerHTML = '<span class="ls-status-icon">📄</span><span class="ls-status-text">' + escHtml(detail).replace(/<br>/g, ' ') + '</span>';

      // 새 항목을 맨 위에 삽입 (최신이 상단)
      if (details.firstChild) {
        details.insertBefore(item, details.firstChild);
      } else {
        details.appendChild(item);
      }

      // 3초 후 페이드아웃 예약
      var fadeTimer = setTimeout(function() {
        if (!item.parentNode) return;
        item.classList.add('fading');
        setTimeout(function() {
          if (item.parentNode) item.parentNode.removeChild(item);
        }, 600);
      }, 3500);
      _statusFadeTimers.push(fadeTimer);

      // 동시에 3개 초과 시 가장 오래된 것 즉시 페이드 (3초 지난 것만)
      var items = details.querySelectorAll('.ls-status-item:not(.fading)');
      if (items.length > 3) {
        var oldest = items[items.length - 1];
        var ts = parseInt(oldest.getAttribute('data-ts') || '0');
        if (Date.now() - ts > 2500) {
          oldest.classList.add('fading');
          setTimeout(function() {
            if (oldest.parentNode) oldest.parentNode.removeChild(oldest);
          }, 600);
        }
      }
    }
  }

  function appendSuggestions(suggestions) {
    var mc = document.getElementById('ls-chat-messages');
    if (!mc || !suggestions || !suggestions.length) return;

    var wrap = document.createElement('div');
    wrap.className = 'ls-suggestions';
    for (var i = 0; i < suggestions.length; i++) {
      var btn = document.createElement('button');
      btn.className = 'ls-suggest-btn';
      btn.textContent = suggestions[i];
      btn.setAttribute('data-question', suggestions[i]);
      btn.setAttribute('onclick', 'LanstarChat._clickSuggestion(this.getAttribute("data-question"))');
      wrap.appendChild(btn);
    }
    mc.appendChild(wrap);
    // AI 답변 시작 부분이 보이도록 스크롤 (추천 질문은 답변 아래에 위치)
    if (_lastAiMsgDiv) {
      _scrollToAiStart(mc, _lastAiMsgDiv);
    }
    _saveState();
  }

  function _clickSuggestion(question) {
    if (!question || !_ws) return;
    // 추천 질문 버튼 영역 제거
    var allSugg = document.querySelectorAll('.ls-suggestions');
    for (var i = 0; i < allSugg.length; i++) {
      allSugg[i].parentNode.removeChild(allSugg[i]);
    }
    // 메시지로 전송
    appendMsg('user', _memberName || '고객', question);
    showTyping();
    _ws.send(JSON.stringify({ type: 'chat', content: question }));
  }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/\n/g, '<br>');
  }

  function formatMsg(str) {
    var s = String(str);
    // 1. 마크다운 링크 [텍스트](URL) → 플레이스홀더
    var links = [];
    s = s.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, function(_, text, url) {
      var idx = links.length;
      links.push({ text: text, url: url });
      return '{{LINK_' + idx + '}}';
    });
    // 2. 남은 일반 URL → 플레이스홀더
    var urls = [];
    s = s.replace(/(https?:\/\/[^\s]+)/g, function(url) {
      var idx = urls.length;
      urls.push(url);
      return '{{URL_' + idx + '}}';
    });
    // 3. HTML 이스케이프
    s = s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br>');
    // 4. **bold** 변환
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    // 5. 마크다운 링크 복원 → 유튜브는 썸네일 카드, 나머지는 텍스트 링크
    s = s.replace(/\{\{LINK_(\d+)\}\}/g, function(_, idx) {
      var l = links[parseInt(idx)];
      var ytId = _extractYoutubeId(l.url);
      if (ytId) {
        return '<a href="' + l.url + '" target="_blank" rel="noopener" class="ls-yt-card">' +
          '<div class="ls-yt-thumb-wrap">' +
            '<img class="ls-yt-thumb" src="https://img.youtube.com/vi/' + ytId + '/mqdefault.jpg" alt="' + escAttr(l.text) + '">' +
            '<div class="ls-yt-play"><svg viewBox="0 0 68 48" width="48" height="34"><path d="M66.52 7.74c-.78-2.93-2.49-5.41-5.42-6.19C55.79.13 34 0 34 0S12.21.13 6.9 1.55C3.97 2.33 2.27 4.81 1.48 7.74.06 13.05 0 24 0 24s.06 10.95 1.48 16.26c.78 2.93 2.49 5.41 5.42 6.19C12.21 47.87 34 48 34 48s21.79-.13 27.1-1.55c2.93-.78 4.64-3.26 5.42-6.19C67.94 34.95 68 24 68 24s-.06-10.95-1.48-16.26z" fill="red"/><path d="M45 24L27 14v20" fill="white"/></svg></div>' +
          '</div>' +
          '<div class="ls-yt-title">' + escAttr(l.text) + '</div>' +
        '</a>';
      }
      return '<a href="' + l.url + '" target="_blank" rel="noopener" style="color:#2563eb;text-decoration:underline;font-weight:600">' + l.text + '</a>';
    });
    // 6. 일반 URL 복원 → 클릭 가능 링크
    s = s.replace(/\{\{URL_(\d+)\}\}/g, function(_, idx) {
      var url = urls[parseInt(idx)];
      return '<a href="' + url + '" target="_blank" rel="noopener" style="color:#2563eb;text-decoration:underline;word-break:break-all">링크 바로가기</a>';
    });
    return s;
  }

  function _extractYoutubeId(url) {
    // youtu.be/VIDEO_ID 또는 youtube.com/watch?v=VIDEO_ID
    var m = url.match(/youtu\.be\/([a-zA-Z0-9_-]{11})/) ||
            url.match(/youtube\.com\/watch\?v=([a-zA-Z0-9_-]{11})/) ||
            url.match(/youtube\.com\/embed\/([a-zA-Z0-9_-]{11})/);
    return m ? m[1] : null;
  }

  function escAttr(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/'/g, '&#39;')
      .replace(/"/g, '&quot;');
  }

  function debounce(fn, ms) {
    var t;
    return function () {
      var args = arguments;
      var self = this;
      clearTimeout(t);
      t = setTimeout(function() { fn.apply(self, args); }, ms);
    };
  }

  function _goBack() {
    _selectedModel = null;
    _selectedMenu = '';
    _clearState();
    showScreen('menu');
  }

  function _chatBack() {
    // 현재 WebSocket 종료
    if (_ws) { _ws.close(); _ws = null; }
    _isSending = false;
    // 채팅 내용 초기화
    document.getElementById('ls-chat-messages').innerHTML = '';
    // 세션 ID 재생성 (새 상담)
    _sessionId = null;
    // 메뉴 화면으로
    _selectedModel = null;
    _selectedMenu = '';
    _clearState();
    showScreen('menu');
  }

  // ── 주문조회 ─────────────────────────────────────────────
  function showOrdersScreen() {
    var container = document.getElementById('ls-orders-content');
    showScreen('orders');

    // 로그인 체크 (memberName이 없으면 비로그인)
    if (!_memberName && !_memberNo) {
      container.innerHTML =
        '<div class="ls-login-prompt">' +
          '<p>\uD83D\uDD12 \uC8FC\uBB38 \uC870\uD68C\uB97C \uC704\uD574<br><strong>\uB85C\uADF8\uC778\uC774 \uD544\uC694</strong>\uD569\uB2C8\uB2E4.</p>' +
          '<a class="ls-login-btn" href="https://www.lanstar.co.kr/member/login.php">\uB85C\uADF8\uC778 \uD558\uAE30 \u2192</a>' +
        '</div>';
      return;
    }

    // 회원번호가 있으면 바로 조회 (전화번호 불필요)
    if (_memberNo) {
      fetchOrders('');
      return;
    }

    // 전화번호 있으면 바로 조회
    if (_memberPhone) {
      fetchOrders(_memberPhone);
      return;
    }

    // 전화번호 없으면 입력 폼
    container.innerHTML =
      '<div class="ls-phone-form">' +
        '<p>\uD83D\uDCDE \uC8FC\uBB38 \uC870\uD68C\uB97C \uC704\uD574<br>\uD734\uB300\uD3F0 \uBC88\uD638\uB97C \uC785\uB825\uD574 \uC8FC\uC138\uC694.</p>' +
        '<input id="ls-phone-input" type="tel" placeholder="010-0000-0000" maxlength="13">' +
        '<button onclick="LanstarChat._submitPhone()">\uC8FC\uBB38 \uC870\uD68C</button>' +
      '</div>';
    setTimeout(function() {
      var pi = document.getElementById('ls-phone-input');
      if (pi) pi.focus();
    }, 100);
  }

  function _submitPhone() {
    var pi = document.getElementById('ls-phone-input');
    if (!pi) return;
    var phone = pi.value.replace(/[^0-9]/g, '');
    if (phone.length < 10) {
      alert('\uD734\uB300\uD3F0 \uBC88\uD638\uB97C \uC815\uD655\uD788 \uC785\uB825\uD574 \uC8FC\uC138\uC694.');
      return;
    }
    _memberPhone = phone;
    fetchOrders(phone);
  }

  async function fetchOrders(phone) {
    var container = document.getElementById('ls-orders-content');
    container.innerHTML = '<p style="text-align:center;color:#999;padding:32px">\uC8FC\uBB38 \uB0B4\uC5ED\uC744 \uC870\uD68C \uC911\uC785\uB2C8\uB2E4...</p>';

    try {
      var params = [];
      if (_memberNo) params.push('memNo=' + encodeURIComponent(_memberNo));
      if (phone) params.push('phone=' + encodeURIComponent(phone));
      var qs = params.length ? '?' + params.join('&') : '';
      var res = await fetch(BACKEND + '/api/aicc/orders' + qs);
      var data = await res.json();
      renderOrders(data);
    } catch (e) {
      container.innerHTML =
        '<div class="ls-order-empty">\uC8FC\uBB38 \uC870\uD68C \uC911 \uC624\uB958\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4.<br>\uC804\uD654(02-717-3386)\uB85C \uBB38\uC758\uD574 \uC8FC\uC138\uC694.</div>';
    }
  }

  function renderOrders(data) {
    var container = document.getElementById('ls-orders-content');
    var orders = data.orders || [];

    if (!orders.length) {
      container.innerHTML =
        '<div class="ls-order-empty">' +
          '<p style="font-size:32px;margin-bottom:12px">\uD83D\uDCED</p>' +
          '\uCD5C\uADFC 90\uC77C \uC774\uB0B4 \uC8FC\uBB38 \uB0B4\uC5ED\uC774 \uC5C6\uC2B5\uB2C8\uB2E4.' +
        '</div>' +
        '<div class="ls-order-bottom">' +
          '<a href="https://www.lanstar.co.kr/mypage/order_list.php">\uC8FC\uBB38 \uB0B4\uC5ED \uB354\uBCF4\uAE30</a>' +
          '<button class="primary" onclick="LanstarChat._goBack()">\uCC98\uC74C\uC73C\uB85C</button>' +
        '</div>';
      return;
    }

    var label = '<p class="ls-orders-label">\uCD5C\uADFC \uC8FC\uBB38\uB0B4\uC5ED\uC744 \uC548\uB0B4\uD574 \uB4DC\uB9B4\uAC8C\uC694.</p>';

    // 카드 생성
    var cards = orders.slice(0, 10).map(function(order) {
      var statusClass = getStatusClass(order.order_status);
      var priceHtml = order.settle_price
        ? '<div class="ls-order-row"><span class="label">\uCD1D \uAE08\uC561</span><span class="value">' + formatPrice(order.settle_price) + '</span></div>'
        : '';

      // 배송조회 버튼 (운송장 있는 상품이 있으면)
      var trackingBtn = '';
      if (order.goods && order.goods.length) {
        for (var i = 0; i < order.goods.length; i++) {
          if (order.goods[i].tracking_url) {
            trackingBtn = '<a href="' + order.goods[i].tracking_url + '" target="_blank">\uD83D\uDE9A \uBC30\uC1A1\uC870\uD68C</a>';
            break;
          }
        }
      }

      return '<div class="ls-order-card">' +
        '<div class="ls-order-row"><span class="label">\uC8FC\uBB38\uC77C</span><span class="value">' + escHtml(order.order_date) + '</span></div>' +
        '<div class="ls-order-row"><span class="label">\uC8FC\uBB38\uBC88\uD638</span><span class="value">' + escHtml(order.order_no) + '</span></div>' +
        '<div class="ls-order-row"><span class="label">\uC8FC\uBB38\uC0C1\uD488</span><span class="value">' + escHtml(order.goods_summary || '') + '</span></div>' +
        priceHtml +
        '<div class="ls-order-row"><span class="label">\uC8FC\uBB38\uC0C1\uD0DC</span><span class="value"><span class="ls-order-status ' + statusClass + '">' + escHtml(order.order_status_text) + '</span></span></div>' +
        '<div class="ls-order-btns">' +
          '<a href="https://www.lanstar.co.kr/mypage/order_view.php?orderNo=' + encodeURIComponent(order.order_no) + '" target="_blank">\uC0C1\uC138 \uD655\uC778</a>' +
          trackingBtn +
        '</div>' +
      '</div>';
    }).join('');

    // 페이지 도트
    var dotsHtml = '';
    if (orders.length > 1) {
      dotsHtml = '<div class="ls-order-dots" id="ls-order-dots">';
      for (var i = 0; i < Math.min(orders.length, 10); i++) {
        dotsHtml += '<span class="ls-order-dot' + (i === 0 ? ' active' : '') + '"></span>';
      }
      dotsHtml += '</div>';
    }

    var bottomHtml =
      '<div class="ls-order-bottom">' +
        '<a href="https://www.lanstar.co.kr/mypage/order_list.php">\uC8FC\uBB38 \uB0B4\uC5ED \uB354\uBCF4\uAE30</a>' +
        '<button class="primary" onclick="LanstarChat._goBack()">\uCC98\uC74C\uC73C\uB85C</button>' +
      '</div>';

    container.innerHTML = label + '<div class="ls-orders-scroll" id="ls-orders-scroll">' + cards + '</div>' + dotsHtml + bottomHtml;

    // 스크롤 시 도트 업데이트
    if (orders.length > 1) {
      var scrollEl = document.getElementById('ls-orders-scroll');
      scrollEl.addEventListener('scroll', function() {
        var idx = Math.round(scrollEl.scrollLeft / scrollEl.offsetWidth);
        var dots = document.querySelectorAll('#ls-order-dots .ls-order-dot');
        dots.forEach(function(dot, i) {
          dot.classList.toggle('active', i === idx);
        });
      });
    }
  }

  function getStatusClass(status) {
    if (!status) return 'ls-status-default';
    var s = status.charAt(0);
    if (s === 'd') return status === 'd2' ? 'ls-status-complete' : 'ls-status-shipping';
    if (s === 'p') return 'ls-status-complete';
    if (s === 'o' && status === 'o1') return 'ls-status-pending';
    if (s === 'c' || s === 'r' || s === 'b') return 'ls-status-cancel';
    return 'ls-status-default';
  }

  function formatPrice(price) {
    if (!price) return '';
    var n = parseInt(String(price).replace(/[^0-9]/g, ''));
    if (isNaN(n) || n === 0) return '';
    return n.toLocaleString() + '\uC6D0';
  }

  // ── 공개 API ──────────────────────────────────────────────
  window.LanstarChat = {
    init: function (opts) {
      opts = opts || {};
      _memberName = opts.memberName || '';
      _memberPhone = opts.memberPhone || '';
      _memberNo = opts.memberNo || '';
      _memberLevel = parseInt(opts.memberLevel) || 0;
      injectCSS();
      createHTML();
      bindEvents();
      loadModels();
      renderExcelButton();
      _fetchPlaceholderExamples();
      _restoreSession();

      // 페이지 이동 시 상태 자동 저장
      window.addEventListener('beforeunload', function() {
        _saveState();
      });
    },
    _goBack: _goBack,
    _chatBack: _chatBack,
    _selectModel: _selectModel,
    _confirmModel: _confirmModel,
    _submitPhone: _submitPhone,
    _clearImage: _clearImage,
    _clickSuggestion: _clickSuggestion,
    _downloadExcel: downloadExcel,
  };

})();
