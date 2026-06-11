/* SherrByte — Firebase Cloud Messaging background handler. */
importScripts('https://www.gstatic.com/firebasejs/10.12.2/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.12.2/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: "AIzaSyCqebJQbNp7AeiZQnScWDc8cstFYPH5wik",
  authDomain: "sherrbyte.firebaseapp.com",
  projectId: "sherrbyte",
  storageBucket: "sherrbyte.firebasestorage.app",
  messagingSenderId: "485011285385",
  appId: "1:485011285385:web:8e2f1bcea07cf84017ed58",
  measurementId: "G-4YT8KG56WZ"
});

const messaging = firebase.messaging();

// Data-only payloads → we render them ourselves (no duplicate auto-notification).
messaging.onBackgroundMessage(function (payload) {
  var d = (payload && payload.data) || {};
  return self.registration.showNotification(d.title || 'SherrByte', {
    body: d.body || '',
    icon: '/tiger-logo.png',
    badge: '/tiger-logo.png',
    data: { url: d.url || '/' }
  });
});

self.addEventListener('notificationclick', function (e) {
  e.notification.close();
  var url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(self.clients.matchAll({ type: 'window' }).then(function (cl) {
    for (var i = 0; i < cl.length; i++) { if ('focus' in cl[i]) return cl[i].focus(); }
    if (self.clients.openWindow) return self.clients.openWindow(url);
  }));
});
