// Fichier source non minifié du script principal
// Modifiez ce fichier puis minifiez-le pour mettre 
// à jour script.js : npx terser script.src.js -o script.js -c -m

import { API_PROXY, STOPS, LINES, ICONS } from "./config.js";
let stopsGtfs = {};
const REFRESH_MS = 15e3,
  maxBackoff = 120e3;
let currentInterval = REFRESH_MS;
const navitiaSA = (sa) =>
  `https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/coverage/fr-idf/stop_areas/${sa}/departures?count=6&data_freshness=realtime`;
const navitiaTraffic = (id) =>
  `https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/coverage/fr-idf/lines/${id}/traffic`;
const storeKey = (sa) => `cache_${sa}`;
async function fetchJSON(url) {
  const r = await fetch(`${API_PROXY}?url=${encodeURIComponent(url)}`);
  if (!r.ok) throw new Error(r.status);
  return r.json();
}
// Récupère les prochains départs pour un arrêt
async function fetchStop(sa) {
  try {
    const data = await fetchJSON(navitiaSA(sa));
    localStorage.setItem(storeKey(sa), JSON.stringify(data));
    return parseDep(data);
  } catch (e) {
    const cached = localStorage.getItem(storeKey(sa));
    if (cached) return parseDep(JSON.parse(cached));
    throw e;
  }
  function parseDep(d) {
    return d.departures.map((dep) => {
      const ts = Date.parse(
        dep.stop_date_time.departure_date_time.substr(0, 15),
      );
      return {
        waitSec: Math.max(0, Math.round((ts - Date.now()) / 1000)),
        dest:
          stopsGtfs[dep.display_informations.direction] ??
          dep.display_informations.direction,
        line: dep.display_informations.label,
      };
    });
  }
}
// Récupère les messages de perturbation Navitia
async function fetchTraffic() {
  const arr = await Promise.all(
    [LINES.rerA, LINES.bus77, LINES.bus201].map((id) =>
      fetchJSON(navitiaTraffic(id)).catch(() => null),
    ),
  );
  return arr
    .filter(Boolean)
    .flatMap((o) => o.pt_statuses || [])
    .filter((m) => m.severity?.effect !== "NO_EFFECT")
    .map((m) => m.message.text);
}
// Affiche la liste des prochains passages dans la page
function render(stopId, list) {
  const el = document.querySelector(`[data-stop='${stopId}'] .passages`);
  el.innerHTML = list
    .map(
      (p) =>
        `<div class=row><span class="dest"><span class=icon>${ICONS[p.line] || ""}</span>${p.dest}</span><span class=wait data-sec="${p.waitSec}">${formatMin(p.waitSec)}</span></div>`,
    )
    .join("");
}
// Formate l'attente en minutes
function formatMin(s) {
  return s < 60 ? `${Math.floor(s / 60)} min` : ">1 h";
}
// Décrémente chaque seconde les compteurs d'attente
function tickCountdown() {
  document.querySelectorAll(".wait[data-sec]").forEach((span) => {
    let v = parseInt(span.dataset.sec) - 1;
    span.dataset.sec = v;
    span.textContent = v <= 0 ? "À quai" : formatMin(v);
  });
}
// Affiche ou masque la zone d'alertes
function showAlerts(msgs) {
  const box = document.getElementById("alerts");
  if (msgs.length) {
    box.innerHTML = msgs.map((t) => `<p>${t}</p>`).join("");
    box.classList.add("show");
  } else box.classList.remove("show");
}
// Rafraîchit périodiquement l'affichage
async function refresh() {
  try {
    for (const k in STOPS) {
      render(k, await fetchStop(STOPS[k]));
    }
    showAlerts(await fetchTraffic());
    currentInterval = REFRESH_MS;
  } catch (e) {
    console.error(e);
    currentInterval = Math.min(currentInterval * 2, maxBackoff);
  } finally {
    setTimeout(refresh, currentInterval);
  }
}
// Charge les correspondances statiques puis lance le rafraîchissement
fetch("./static/horaires_export.json")
  .then((r) => r.json())
  .then((j) => {
    stopsGtfs = j;
    setInterval(tickCountdown, 1e3);
    refresh();
  })
  .catch((e) => console.error("JSON load error", e));
