#!/usr/bin/env python3
"""
Récupère le dernier GTFS (OpenDataSoft) via un worker-proxy Cloudflare,
puis calcule – pour la date donnée (aujourd’hui par défaut) – le premier
et le dernier passage du RER A à Joinville-le-Pont (deux sens).

Variable d’environnement OBLIGATOIRE
------------------------------------
PROXY_WORKER = "https://<ton-worker>.workers.dev/?url="
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import duckdb
import pandas as pd
import requests
from dateutil import tz
from tqdm import tqdm

# ────────────────────────── arguments CLI ──────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument(
    "-d", "--date",
    type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
    default=dt.date.today(),
    help="Date étudiée (AAAA-MM-JJ, défaut : aujourd’hui)",
)
parser.add_argument(
    "--save-json", metavar="FICHIER",
    help="Enregistre aussi le résultat dans ce fichier JSON",
)
args = parser.parse_args()

DAY      = args.date
ROUTE_ID = "STIF:Line::C01742:"  # identifiant RER A

# ────────────────────────── worker-proxy ───────────────────────────
WORKER = os.getenv("PROXY_WORKER")  # …/workers.dev/?url=
if not WORKER:
    sys.exit("❌  PROXY_WORKER n’est pas défini")

def proxify(url: str) -> str:
    prefix = WORKER if WORKER.endswith("?url=") else WORKER + "?url="
    return f"{prefix}{quote_plus(url, safe='')}"

# ────────────────────────── API catalogue ODS ──────────────────────
ODS_ENDPOINT = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/"
    "datasets/offre-horaires-tc-gtfs-idfm"
)

def latest_zip_url() -> str:
    """Retourne l’URL .zip la plus récente du dataset GTFS IDFM."""
    resp = requests.get(proxify(ODS_ENDPOINT), timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    attachments = raw["attachments"] if isinstance(raw, dict) else raw

    zips = [
        (att["updated_at"], att["url"])                # champs URL
        for att in attachments
        if att["url"].lower().endswith(".zip")
    ]
    if not zips:
        raise RuntimeError("Aucun ZIP trouvé dans le dataset")
    zips.sort(reverse=True)                           # plus récent d’abord
    return zips[0][1]

# ────────────────────────── cache local du ZIP ─────────────────────
CACHE_DIR = Path(__file__).with_suffix(".d")
CACHE_DIR.mkdir(exist_ok=True)
LOCAL_ZIP = CACHE_DIR / "gtfs_latest.zip"

def download_gtfs() -> Path:
    if LOCAL_ZIP.exists():
        return LOCAL_ZIP
    url = latest_zip_url()
    print("⬇️  Téléchargement du GTFS IDFM …")
    with requests.get(proxify(url), stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="o", unit_scale=True) as bar, LOCAL_ZIP.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
    return LOCAL_ZIP

zip_path = download_gtfs()

# ────────────────────────── lecture DuckDB ─────────────────────────
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")
csv = lambda p: f"zip://{p}?{zip_path}"

con.execute(f"CREATE VIEW stops      AS SELECT * FROM read_csv_auto('{csv('stops.txt')}');")
con.execute(f"CREATE VIEW stop_times AS SELECT * FROM read_csv_auto('{csv('stop_times.txt')}');")
con.execute(f"CREATE VIEW trips      AS SELECT * FROM read_csv_auto('{csv('trips.txt')}');")
con.execute(f"CREATE VIEW calendar   AS SELECT * FROM read_csv_auto('{csv('calendar.txt')}');")
con.execute(f"CREATE VIEW cal_dates  AS SELECT * FROM read_csv_auto('{csv('calendar_dates.txt')}');")

# ────────────────────────── stop_id Joinville ──────────────────────
stops = con.execute("""
    SELECT stop_id FROM stops
    WHERE lower(stop_name) LIKE '%joinville-le-pont%'
""").fetch_df()
if stops.empty:
    sys.exit("Joinville-le-Pont introuvable dans le GTFS")
STOP_IDS = tuple(stops["stop_id"])

# ────────────────────────── services actifs ───────────────────────
day_int = int(DAY.strftime("%Y%m%d"))
weekday = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"][DAY.weekday()]

service_ids = con.execute(f"""
    SELECT service_id FROM calendar
    WHERE {day_int} BETWEEN start_date AND end_date AND {weekday}=1
    UNION
    SELECT service_id FROM cal_dates
    WHERE date={day_int} AND exception_type=1
""").fetch_df()
SERVICE_IDS = tuple(service_ids["service_id"])

# ────────────────────────── premier / dernier ──────────────────────
query = f"""
    SELECT trips.direction_id,
           MIN(stop_times.departure_time) AS first_time,
           MAX(stop_times.departure_time) AS last_time
    FROM stop_times
    JOIN trips ON trips.trip_id = stop_times.trip_id
    WHERE stop_times.stop_id IN {STOP_IDS}
      AND trips.route_id = '{ROUTE_ID}'
      AND trips.service_id IN {SERVICE_IDS}
    GROUP BY trips.direction_id
    ORDER BY direction_id;
"""
result = con.execute(query).fetch_df()

def gtfs_to_iso(t: str) -> str:
    h, m, s = map(int, t.split(":"))
    base = dt.datetime.combine(DAY, dt.time(0, 0, tzinfo=tz.gettz("Europe/Paris")))
    if h >= 24:
        h -= 24
        base += dt.timedelta(days=1)
    return (base + dt.timedelta(hours=h, minutes=m, seconds=s)).isoformat()

result["first_time"] = result["first_time"].map(gtfs_to_iso)
result["last_time"]  = result["last_time"].map(gtfs_to_iso)

lbl = {0: "→ Paris / St-Germain", 1: "→ Boissy / MLV"}
print(f"\nRER A – Joinville-le-Pont | {DAY:%Y-%m-%d}\n")
for _, row in result.iterrows():
    print(f"{lbl.get(row['direction_id'], row['direction_id'])}")
    print(f"  Premier : {row['first_time']}\n  Dernier  : {row['last_time']}\n")

# ────────────────────────── export JSON optionnel ──────────────────
if args.save_json:
    out = result.rename(columns={
        "direction_id": "direction",
        "first_time":   "first",
        "last_time":    "last",
    }).to_dict(orient="records")
    Path(args.save_json).write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print("💾  JSON écrit >", args.save_json)
