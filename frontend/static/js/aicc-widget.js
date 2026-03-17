/**
 * 랜스타 AI 상담 챗봇 위젯
 * 고도몰 쇼핑몰에 직접 삽입 (iframe/별도 URL 없음)
 *
 * 사용법: 고도몰 스킨 HTML 하단에 아래 추가
 * <script src="https://order-agent-ffr7.onrender.com/static/js/aicc-widget.js"></script>
 * <script>LanstarChat.init({ memberName: '{$member.mem_name}' });</script>
 */

(function () {
  'use strict';

  const BACKEND = 'https://order-agent-ffr7.onrender.com';

  // ── 상태 변수 ─────────────────────────────────────────────
  let _memberName = '';
  let _selectedMenu = '';
  let _selectedModel = null;   // {model_name, erp_code, product_name}
  let _sessionId = null;
  let _ws = null;
  let _allModels = [];
  let _isSending = false;      // 중복 전송 방지 플래그

  // ── CSS 주입 ────────────────────────────────────────────────
  const CSS = `
  #ls-chat-btn{position:fixed;bottom:24px;right:24px;z-index:99998;width:56px;height:56px;border-radius:50%;background:#1a1a2e;color:#fff;border:none;font-size:22px;cursor:pointer;box-shadow:0 4px 16px rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;transition:transform .15s}
  #ls-chat-btn:hover{transform:scale(1.08)}
  #ls-chat-badge{position:absolute;top:-4px;right:-4px;background:#e63946;color:#fff;border-radius:50%;width:20px;height:20px;font-size:11px;display:none;align-items:center;justify-content:center;font-weight:700}
  #ls-chat-popup{position:fixed;bottom:90px;right:24px;z-index:99999;width:380px;height:600px;border-radius:16px;background:#fff;box-shadow:0 8px 40px rgba(0,0,0,.22);display:none;flex-direction:column;overflow:hidden;font-family:'Noto Sans KR',sans-serif}
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
  .ls-menu-icon{display:block;font-size:20px;margin-bottom:4px}

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
  .ls-msg.ai,.ls-msg.admin,.ls-msg.system{align-items:flex-start}
  .ls-msg-label{font-size:10px;color:#999;margin-bottom:3px}
  .ls-msg-bubble{max-width:82%;padding:9px 13px;border-radius:14px;font-size:13px;line-height:1.6;white-space:pre-wrap;word-break:break-word}
  .ls-msg.user .ls-msg-bubble{background:#1a1a2e;color:#fff;border-bottom-right-radius:4px}
  .ls-msg.ai .ls-msg-bubble{background:#f1f3f8;color:#333;border-bottom-left-radius:4px}
  .ls-msg.admin .ls-msg-bubble{background:#fff3cd;color:#333;border-bottom-left-radius:4px;border:1px solid #ffc107}
  .ls-msg.system .ls-msg-bubble{background:#e8f5e9;color:#2e7d32;border-radius:8px;font-size:12px;width:100%;text-align:center}
  #ls-typing{padding:8px 12px;display:none;align-items:center;gap:6px;color:#999;font-size:12px}
  #ls-typing .dot{width:6px;height:6px;border-radius:50%;background:#ccc;animation:ls-bounce .8s infinite}
  #ls-typing .dot:nth-child(2){animation-delay:.15s}
  #ls-typing .dot:nth-child(3){animation-delay:.3s}
  @keyframes ls-bounce{0%,80%,100%{transform:translateY(0)}40%{transform:translateY(-6px)}}
  #ls-admin-banner{background:#fff3cd;border-top:1px solid #ffc107;padding:8px 14px;font-size:12px;color:#856404;flex-shrink:0;display:none}
  #ls-chat-bottom{padding:10px 12px;border-top:1px solid #eee;display:flex;gap:8px;flex-shrink:0;background:#fff}
  #ls-chat-input{flex:1;padding:9px 12px;border:1.5px solid #ddd;border-radius:20px;font-size:13px;font-family:inherit;outline:none;resize:none;max-height:80px;line-height:1.4}
  #ls-chat-input:focus{border-color:#1a1a2e}
  #ls-chat-send{width:38px;height:38px;border-radius:50%;background:#1a1a2e;color:#fff;border:none;cursor:pointer;font-size:16px;flex-shrink:0;display:flex;align-items:center;justify-content:center}
  #ls-chat-send:disabled{background:#ccc;cursor:not-allowed}
  #ls-request-admin-btn{width:calc(100% - 24px);padding:8px;background:none;border:1px solid #ddd;border-radius:6px;font-size:12px;color:#666;cursor:pointer;margin:4px 12px 0;font-family:inherit}

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

  @media(max-width:420px){#ls-chat-popup{width:100vw;height:100vh;bottom:0;right:0;border-radius:0}}
  `;

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
        \uD83D\uDCAC
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
            <div class="ls-menu-grid">
              <button class="ls-menu-btn" data-menu="\uC81C\uD488\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDCCB</span>\uC81C\uD488 \uBB38\uC758</button>
              <button class="ls-menu-btn" data-menu="\uAE30\uC220\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDD27</span>\uAE30\uC220 \uBB38\uC758</button>
              <button class="ls-menu-btn" data-menu="\uBC30\uC1A1\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDE9A</span>\uBC30\uC1A1 \uBB38\uC758</button>
              <button class="ls-menu-btn" data-menu="\uC7AC\uACE0\uC870\uD68C"><span class="ls-menu-icon">\uD83D\uDCE6</span>\uC7AC\uACE0 \uC870\uD68C</button>
              <button class="ls-menu-btn" data-menu="\uACAC\uC801\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDCC4</span>\uACAC\uC801 \uBB38\uC758</button>
              <button class="ls-menu-btn" data-menu="AS\uBB38\uC758"><span class="ls-menu-icon">\uD83D\uDEE0</span>A/S \uBB38\uC758</button>
            </div>
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
            <div id="ls-chat-info"></div>
            <div id="ls-admin-banner">\uD83D\uDC64 \uB2F4\uB2F9\uC790\uAC00 \uC5F0\uACB0\uB418\uC5C8\uC2B5\uB2C8\uB2E4</div>
            <div id="ls-chat-messages"></div>
            <div id="ls-typing"><span class="dot"></span><span class="dot"></span><span class="dot"></span> AI\uAC00 \uB2F5\uBCC0 \uC911...</div>
            <div id="ls-chat-bottom">
              <textarea id="ls-chat-input" placeholder="\uBA54\uC2DC\uC9C0\uB97C \uC785\uB825\uD558\uC138\uC694..." rows="1"></textarea>
              <button id="ls-chat-send">\u2191</button>
            </div>
            <button id="ls-request-admin-btn" onclick="LanstarChat._requestAdmin()">\uB2F4\uB2F9\uC790 \uC5F0\uACB0 \uC694\uCCAD</button>
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

        </div>
      </div>
    `);
  }

  // ── 화면 전환 ──────────────────────────────────────────────
  function showScreen(name) {
    ['menu', 'model', 'chat', 'inventory', 'quote'].forEach(function(s) {
      var el = document.getElementById('ls-screen-' + s);
      if (el) el.style.display = s === name ? '' : 'none';
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
  }

  function closePopup() {
    document.getElementById('ls-chat-popup').classList.remove('open');
  }

  // ── 메뉴 선택 ─────────────────────────────────────────────
  function onMenuSelect(menu) {
    _selectedMenu = menu;
    _selectedModel = null;

    if (menu === '\uACAC\uC801\uBB38\uC758') {
      showScreen('quote');
      return;
    }
    if (menu === '\uBC30\uC1A1\uBB38\uC758' || menu === 'AS\uBB38\uC758') {
      startChat(null);
      return;
    }
    // 제품문의, 기술문의, 재고조회 → 제품 선택 화면
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
    document.getElementById('ls-chat-input').value = '';
    document.getElementById('ls-admin-banner').style.display = 'none';
    document.getElementById('ls-request-admin-btn').disabled = false;
    document.getElementById('ls-request-admin-btn').textContent = '\uB2F4\uB2F9\uC790 \uC5F0\uACB0 \uC694\uCCAD';
    document.getElementById('ls-chat-send').disabled = false;

    // 상단 정보 표시
    var modelInfo = model ? '<strong>' + escHtml(model.model_name) + '</strong> | ' : '';
    document.getElementById('ls-chat-info').innerHTML =
      modelInfo + '<strong>' + escHtml(_selectedMenu) + '</strong>';

    showScreen('chat');
    connectWS();
  }

  // ── WebSocket 연결 ─────────────────────────────────────────
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
      + '&menu=' + encodeURIComponent(_selectedMenu);

    _ws = new WebSocket(url);

    _ws.onmessage = function (e) {
      var msg = JSON.parse(e.data);
      hideTyping();
      switch (msg.type) {
        case 'ai_message':
          appendMsg('ai', 'AI \uC0C1\uB2F4\uC0AC', msg.content);
          break;
        case 'admin_message':
          appendMsg('admin', '\uD83D\uDC64 \uB2F4\uB2F9\uC790', msg.content);
          break;
        case 'admin_joined':
          document.getElementById('ls-admin-banner').style.display = 'block';
          appendMsg('system', '', msg.content);
          break;
        case 'session_closed':
          appendMsg('system', '', msg.content);
          document.getElementById('ls-chat-input').disabled = true;
          document.getElementById('ls-chat-send').disabled = true;
          if (_ws) _ws.close();
          break;
        case 'system':
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
    var wsProto = BACKEND.startsWith('https') ? 'wss' : 'ws';
    var wsHost = BACKEND.replace(/^https?:\/\//, '');
    var url = wsProto + '://' + wsHost + '/ws/aicc/chat/' + _sessionId + '?reconnect=1';
    _ws = new WebSocket(url);
  }

  // ── 메시지 전송 (중복 전송 완벽 방지) ────────────────────────
  function sendMessage() {
    if (_isSending) return;

    var inp = document.getElementById('ls-chat-input');
    var text = inp.value.trim();
    if (!text) return;
    if (!_ws || _ws.readyState !== WebSocket.OPEN) {
      appendMsg('system', '', '\uC5F0\uACB0\uC774 \uB04A\uACBC\uC2B5\uB2C8\uB2E4. \uC7A0\uC2DC \uD6C4 \uB2E4\uC2DC \uC2DC\uB3C4\uD574 \uC8FC\uC138\uC694.');
      return;
    }

    _isSending = true;

    // 1. 입력창 즉시 초기화 (중복 표시 방지)
    inp.value = '';
    inp.style.height = 'auto';

    // 2. 사용자 메시지 UI에 표시
    appendMsg('user', '\uB098', text);
    showTyping();

    // 3. WebSocket 전송
    _ws.send(JSON.stringify({ type: 'chat', content: text }));

    // 4. 0.5초 후 플래그 해제
    setTimeout(function() { _isSending = false; }, 500);
  }

  // ── UI 헬퍼 ──────────────────────────────────────────────
  function appendMsg(type, label, content) {
    var mc = document.getElementById('ls-chat-messages');
    var div = document.createElement('div');
    div.className = 'ls-msg ' + type;
    if (label && type !== 'system') {
      div.innerHTML = '<span class="ls-msg-label">' + escHtml(label) + '</span><div class="ls-msg-bubble">' + escHtml(content) + '</div>';
    } else {
      div.innerHTML = '<div class="ls-msg-bubble">' + escHtml(content) + '</div>';
    }
    mc.appendChild(div);
    mc.scrollTop = mc.scrollHeight;
  }

  function showTyping() { document.getElementById('ls-typing').style.display = 'flex'; }
  function hideTyping() { document.getElementById('ls-typing').style.display = 'none'; }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/\n/g, '<br>');
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
    showScreen('menu');
  }

  function _requestAdmin() {
    if (!_ws || _ws.readyState !== WebSocket.OPEN) return;
    _ws.send(JSON.stringify({ type: 'request_admin', content: '' }));
    document.getElementById('ls-request-admin-btn').disabled = true;
    document.getElementById('ls-request-admin-btn').textContent = '\uC5F0\uACB0 \uC694\uCCAD \uC644\uB8CC';
  }

  // ── 공개 API ──────────────────────────────────────────────
  window.LanstarChat = {
    init: function (opts) {
      opts = opts || {};
      _memberName = opts.memberName || '';
      injectCSS();
      createHTML();
      bindEvents();
      loadModels();
    },
    _goBack: _goBack,
    _selectModel: _selectModel,
    _confirmModel: _confirmModel,
    _requestAdmin: _requestAdmin,
  };

})();
