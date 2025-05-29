// proxy-node/index.js
const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = "7nAc6NHplCJtJ46Qw32QFtefq3TQEYrT";

app.use(cors());

app.get("/proxy", async (req, res) => {
  const targetUrl = req.query.url;

  if (!targetUrl) {
    return res.status(400).json({ error: "Missing 'url' query param" });
  }

  try {
    const response = await fetch(targetUrl, {
      headers: {
        apikey: API_KEY,
      },
    });

    const data = await response.text();
    res.set("Content-Type", response.headers.get("content-type"));
    res.send(data);
  } catch (error) {
    res.status(500).json({ error: "Proxy fetch failed", details: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Proxy server running on port ${PORT}`);
});
