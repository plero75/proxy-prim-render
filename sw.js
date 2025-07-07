const CACHE = "ratp-vincennes-v1";
self.addEventListener("install", e => {
  const scope = self.registration.scope;
  const files = ["", "index.html", "style.css", "config.js", "script.js", "manifest.json"].map(p => new URL(p, scope).href);
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(files)));
});
self.addEventListener("fetch", e => {
  if (e.request.method !== "GET") return;
  e.respondWith(caches.match(e.request).then(r => r || fetch(e.request)));
});
self.addEventListener("activate", e => {
  e.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))));
});
