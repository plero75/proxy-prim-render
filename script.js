document.addEventListener("DOMContentLoaded", async () => {
  try {
    const rerResp = await fetch("/static/rer_a_prochains_trains_by_direction.json");
    const rerData = await rerResp.json();

    const horairesResp = await fetch("proxy-prim-render/blob/main/static/horaires_export.json");
    const horairesData = await horairesResp.json();

    console.log("✅ Données chargées:", {rerData, horairesData});

    // Affichage prochains trains par direction
    const rerContainer = document.getElementById("rer-a");
    rerContainer.innerHTML = "";
    for (const [direction, trains] of Object.entries(rerData)) {
      const dirTitle = document.createElement("h3");
      dirTitle.textContent = `➡️ Direction ${direction}`;
      rerContainer.appendChild(dirTitle);
      trains.forEach(train => {
        const el = document.createElement("div");
        el.innerHTML = `🕐 ${train.heure} - ${train.minutes} min <br> 🚉 ${train.gares.join(", ")}`;
        rerContainer.appendChild(el);
      });
    }

    // Ajout premiers et derniers passages
    const horairesContainer = document.createElement("div");
    horairesContainer.innerHTML = `
      <small>
        🟢 Premier train : ${horairesData.rer_a.premier} — 🔴 Dernier train : ${horairesData.rer_a.dernier}
      </small>
    `;
    rerContainer.appendChild(horairesContainer);

  } catch (e) {
    console.error("🚨 Erreur lors du chargement des données statiques:", e);
  }
});
