/* SherrByte service worker — installability, offline shell, push-ready. */
const CACHE = 'sherrbyte-v2';
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => {
  e.waitUntil((async () => {
    // Purge old caches so a stale app shell can never be served after an update.
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

// Network-first for page navigations, bypassing the HTTP disk cache so the
// freshest index.html always wins; fall back to the cached shell only offline.
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req, { cache: 'no-store' }).then((r) => { const c = r.clone(); caches.open(CACHE).then((ca) => ca.put('/', c)); return r; })
                .catch(() => caches.match('/'))
    );
  }
});

// Push notifications (activate once VAPID keys + server send are configured).
self.addEventListener('push', (e) => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; } catch (_) { d = { title: 'SherrByte', body: e.data ? e.data.text() : '' }; }
  e.waitUntil(self.registration.showNotification(d.title || 'SherrByte', {
    body: d.body || 'New stories in your topics',
    icon: '/tiger-logo.png', badge: '/tiger-logo.png',
    data: { url: d.url || '/' },
  }));
});
self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(self.clients.matchAll({ type: 'window' }).then((cl) => {
    for (const c of cl) { if ('focus' in c) return c.focus(); }
    if (self.clients.openWindow) return self.clients.openWindow(url);
  }));
});
