#!/usr/bin/env python3
"""
first_last_rera_joinville.py – Génère la liste complète des passages RER A
à Joinville-le-Pont pour la date donnée (par défaut aujourd’hui).

Sortie JSON (par défaut data/today.json) :
[
  {
    "time": "2025-07-08T05:12:00+02:00",
    "direction": "→ Paris / St-Germain",
    "destination": "Saint-Germain-en-Laye",
    "remaining_stops": ["Nogent-sur-Marne", "Vincennes", …]
  },
  …
]
"""

from __future__ import annotations
import argparse, datetime as dt, json, os, sys
from pathlib import Path
from urllib.parse import quote_plus

import duckdb, pandas as pd, requests
from dateutil import tz
from tqdm import tqdm

# ────────────────────────── CLI ──────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--date", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                    default=dt.date.today(), help="Date étudiée (AAAA-MM-JJ)")
parser.add_argument("--save-json", metavar="FICHIER",
                    default="data/today.json",
                    help="Fichier JSON de sortie (défaut : data/today.json)")
args = parser.parse_args()

DAY      = args.date
ROUTE_ID = "STIF:Line::C01742:"
WORKER   = os.getenv("PROXY_WORKER")  # ex. https://…/?url=

def proxify(url: str) -> str:
    return f"{(WORKER if WORKER and WORKER.endswith('?url=') else WORKER + '?url=' if WORKER else '')}{quote_plus(url, safe='')}"

# ─────────── GTFS direct (FTP) ───────────
GTFS_URL  = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
CACHE_DIR = Path(__file__).with_suffix(".d"); CACHE_DIR.mkdir(exist_ok=True)
LOCAL_ZIP = CACHE_DIR / "IDFM-gtfs.zip"

def download_gtfs() -> Path:
    if LOCAL_ZIP.exists(): return LOCAL_ZIP
    url = GTFS_URL
    print("🚦 Téléchargement :", url)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="o", unit_scale=True) as bar, LOCAL_ZIP.open("wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
                bar.update(len(chunk))
    return LOCAL_ZIP

zip_path = download_gtfs()

# ─────────── DuckDB views ───────────
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")

csv = lambda p: f"zip://{zip_path.resolve()}?{p}"

for t in ("stops", "stop_times", "trips", "calendar", "calendar_dates"):
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_csv_auto('{csv(t + '.txt')}');")

# stop_ids Joinville
STOP_IDS = tuple(con.execute("""
  SELECT stop_id FROM stops WHERE lower(stop_name) LIKE '%joinville-le-pont%'
""").fetch_df().stop_id)
if not STOP_IDS: sys.exit("Joinville introuvable")

# services actifs
day_int = int(DAY.strftime("%Y%m%d"))
weekday = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"][DAY.weekday()]
SERVICE_IDS = tuple(con.execute(f"""
  SELECT service_id FROM calendar
  WHERE {day_int} BETWEEN start_date AND end_date AND {weekday}=1
  UNION
  SELECT service_id FROM calendar_dates
  WHERE date={day_int} AND exception_type=1
""").fetch_df().service_id)

# passages du jour
passages = con.execute(f"""
  SELECT st.departure_time, t.direction_id, t.trip_headsign AS destination,
         st.stop_id, t.trip_id
  FROM stop_times st
  JOIN trips t ON t.trip_id = st.trip_id
  WHERE st.stop_id IN {STOP_IDS}
    AND t.route_id = '{ROUTE_ID}'
   
