/* SherrByte service worker — installability, offline shell, push-ready. */
const CACHE = 'sherrbyte-v1';
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(self.clients.claim()); });

// Network-first for page navigations (always fresh), cached shell when offline.
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req).then((r) => { const c = r.clone(); caches.open(CACHE).then((ca) => ca.put('/', c)); return r; })
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
