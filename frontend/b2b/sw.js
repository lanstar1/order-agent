// 랜스타 간편발주 PWA 서비스워커 — 앱 셸 캐시(정적), API는 항상 네트워크
const CACHE = 'b2b-shell-v1';
const SHELL = ['/b2b/', '/b2b/index.html', '/b2b/style.css', '/b2b/app.js', '/b2b/manifest.json', '/b2b/icon.svg'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  // API 호출은 절대 캐시하지 않는다 (재고/단가/발주는 항상 실시간)
  if (url.pathname.startsWith('/api/')) return;
  if (e.request.method !== 'GET') return;
  // 정적 셸: 캐시 우선, 없으면 네트워크
  e.respondWith(
    caches.match(e.request).then((hit) => hit || fetch(e.request).then((res) => {
      const copy = res.clone();
      caches.open(CACHE).then((c) => c.put(e.request, copy)).catch(() => {});
      return res;
    }).catch(() => hit))
  );
});
