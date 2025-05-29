const express = require("express");
const fetch = require("node-fetch");
const app = express();
const PORT = process.env.PORT || 3000;

const API_KEY = "7nAc6NHplCJtJ46Qw32QFtefq3TQEYrT";
const PRIM_BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring";

app.get("/api/rer", async (req, res) => {
    try {
        const url = PRIM_BASE_URL + "?MonitoringRef=STIF:StopPoint:Q:8768238:";
        const response = await fetch(url, {
            headers: { apikey: API_KEY }
        });
        const data = await response.json();
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: "Erreur API PRIM", detail: err.message });
    }
});

app.get("/api/trafic", (req, res) => {
    res.json({ alert: "Trafic normal" });
});

app.listen(PORT, () => console.log("Serveur proxy PRIM actif sur le port " + PORT));