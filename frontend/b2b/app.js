// 랜스타 간편발주 PWA
const API = '/api/b2b';
const $ = (s) => document.querySelector(s);
const store = {
  get token() { return localStorage.getItem('b2b_token') || ''; },
  set token(v) { v ? localStorage.setItem('b2b_token', v) : localStorage.removeItem('b2b_token'); },
};
let catalog = [];          // [{prod_cd, prod_name, unit, price, price_src}]
const qty = {};            // {prod_cd: number}

// ── API helper ──
async function api(path, opts = {}) {
  const headers = Object.assign({ 'Content-Type': 'application/json' }, opts.headers || {});
  if (store.token) headers['Authorization'] = 'Bearer ' + store.token;
  const res = await fetch(API + path, Object.assign({}, opts, { headers }));
  if (res.status === 401 || res.status === 403) { logout(); throw new Error('세션이 만료되었습니다. 다시 로그인해주세요.'); }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || '요청 실패');
  return data;
}

function toast(msg, ms = 2200) {
  const t = $('#toast'); t.textContent = msg; t.classList.add('show');
  clearTimeout(toast._t); toast._t = setTimeout(() => t.classList.remove('show'), ms);
}
const won = (n) => '₩' + Math.round(n || 0).toLocaleString('ko-KR');

// ── 로그인 ──
async function login() {
  const cust_code = $('#in-cust').value.trim();
  const pin = $('#in-pin').value.trim();
  if (!cust_code || !pin) return toast('거래처코드와 PIN을 입력하세요.');
  $('#btn-login').disabled = true;
  try {
    const r = await fetch(API + '/login', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cust_code, pin }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.detail || '로그인 실패');
    store.token = d.token;
    await enterApp();
  } catch (e) {
    toast(e.message);
  } finally {
    $('#btn-login').disabled = false;
  }
}

function logout() {
  store.token = '';
  $('#app').classList.add('hidden');
  $('#login').classList.remove('hidden');
  $('#in-pin').value = '';
}

// ── 앱 진입 ──
async function enterApp() {
  $('#login').classList.add('hidden');
  $('#app').classList.remove('hidden');
  try {
    const me = await api('/me');
    $('#who-name').textContent = me.cust_name || me.cust_code;
    $('#who-tier').textContent = '거래처 ' + me.cust_code + (me.price_tier ? ' · 단가유형 ' + me.price_tier : '');
    await loadCatalog();
  } catch (e) { toast(e.message); }
}

// ── 카탈로그 ──
async function loadCatalog() {
  const box = $('#catalog');
  box.innerHTML = '<div class="empty">불러오는 중…</div>';
  try {
    const d = await api('/catalog');
    catalog = d.items || [];
    for (const k in qty) delete qty[k];
    if (!catalog.length) { box.innerHTML = '<div class="empty">등록된 품목이 없습니다.<br>관리자에게 즐겨찾기 등록을 요청하세요.</div>'; }
    else box.innerHTML = catalog.map(renderItem).join('');
    bindSteppers();
    recalc();
  } catch (e) { box.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

function renderItem(it) {
  const priceTxt = it.price > 0
    ? won(it.price) + (it.unit ? ' / ' + it.unit : '')
    : '<span class="auto">ERP 자동단가</span>';
  return `<div class="item" data-cd="${it.prod_cd}">
    <div class="info">
      <div class="name">${escapeHtml(it.prod_name || it.prod_cd)}</div>
      <div class="price">${priceTxt}</div>
    </div>
    <div class="stepper">
      <button data-act="dec">−</button>
      <input type="number" min="0" inputmode="numeric" value="0" data-cd="${it.prod_cd}">
      <button data-act="inc">＋</button>
    </div>
  </div>`;
}

function bindSteppers() {
  $('#catalog').querySelectorAll('.item').forEach((row) => {
    const cd = row.dataset.cd;
    const input = row.querySelector('input');
    row.querySelector('[data-act=dec]').onclick = () => setQty(cd, (qty[cd] || 0) - 1, input);
    row.querySelector('[data-act=inc]').onclick = () => setQty(cd, (qty[cd] || 0) + 1, input);
    input.oninput = () => setQty(cd, parseInt(input.value || '0', 10), input, true);
  });
}

function setQty(cd, val, input, fromInput) {
  val = Math.max(0, isNaN(val) ? 0 : val);
  qty[cd] = val;
  if (!fromInput) input.value = val;
  recalc();
}

function recalc() {
  let count = 0, total = 0;
  for (const it of catalog) {
    const q = qty[it.prod_cd] || 0;
    if (q > 0) { count++; total += (it.price || 0) * q; }
  }
  $('#order-count').textContent = count + '개 품목';
  $('#order-total').textContent = won(total);
  $('#btn-submit').disabled = count === 0;
}

// ── 발주 ──
async function submitOrder() {
  const lines = catalog
    .filter((it) => (qty[it.prod_cd] || 0) > 0)
    .map((it) => ({ prod_cd: it.prod_cd, qty: qty[it.prod_cd] }));
  if (!lines.length) return;
  const n = lines.reduce((s, l) => s + l.qty, 0);
  if (!confirm(`${lines.length}개 품목 (총 ${n}개)를 발주할까요?`)) return;

  $('#btn-submit').disabled = true;
  $('#btn-submit').textContent = '전송 중…';
  try {
    const r = await api('/order', { method: 'POST', body: JSON.stringify({ lines }) });
    if (r.success) {
      toast('✅ 발주 완료' + (r.erp_slip_no ? ' (전표 ' + r.erp_slip_no + ')' : ''), 3200);
      await loadCatalog();
    } else {
      toast('발주 실패: ' + (r.message || '오류'), 4000);
    }
  } catch (e) {
    toast(e.message, 4000);
  } finally {
    $('#btn-submit').textContent = '발주하기';
    recalc();
  }
}

// ── 발주내역 ──
async function loadHistory() {
  const box = $('#history');
  box.innerHTML = '<div class="empty">불러오는 중…</div>';
  try {
    const d = await api('/orders');
    const orders = d.orders || [];
    if (!orders.length) { box.innerHTML = '<div class="empty">발주 내역이 없습니다.</div>'; return; }
    box.innerHTML = orders.map((o) => `
      <div class="order-card">
        <div class="top">
          <span class="oid">${o.created_at || ''}</span>
          <span class="badge ${o.status}">${statusLabel(o.status)}</span>
        </div>
        <div style="margin-top:8px">${o.erp_slip_no ? '전표 ' + o.erp_slip_no + ' · ' : ''}${won(o.total_amt)}</div>
      </div>`).join('');
  } catch (e) { box.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

function statusLabel(s) { return ({ submitted: '완료', error: '실패', pending: '처리중' })[s] || s; }

// ── 탭 ──
function switchTab(tab) {
  document.querySelectorAll('.tabbar button').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
  $('#tab-order').classList.toggle('hidden', tab !== 'order');
  $('#tab-history').classList.toggle('hidden', tab !== 'history');
  $('#orderbar').classList.toggle('hidden', tab !== 'order');
  if (tab === 'history') loadHistory();
}

function escapeHtml(s) { return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

// ── 초기화 ──
function init() {
  $('#btn-login').onclick = login;
  $('#in-pin').addEventListener('keydown', (e) => { if (e.key === 'Enter') login(); });
  $('#btn-logout').onclick = logout;
  $('#btn-submit').onclick = submitOrder;
  document.querySelectorAll('.tabbar button').forEach((b) => b.onclick = () => switchTab(b.dataset.tab));
  if (store.token) enterApp(); else logout();
  if ('serviceWorker' in navigator) navigator.serviceWorker.register('/b2b/sw.js').catch(() => {});
}
init();
