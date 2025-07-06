const CACHE = "ratp-vincennes-v1";
self.addEventListener("install", e => {
  e.waitUntil(
    caches.open(CACHE).then(c =>
      c.addAll([
        "./",
        "./index.html",
        "./style.css",
        "./config.js",
        "./script.js",
        "./manifest.json"
      ])
    )
  );
});
self.addEventListener("fetch", e => {
  e.respondWith(
    caches.match(e.request).then(r =>
      r || fetch(e.request).then(resp => {
        if (resp.ok) {
          let copy = resp.clone();
          caches.open(CACHE).then(c => c.put(e.request, copy));
        }
        return resp;
      })
    )
  );
});
