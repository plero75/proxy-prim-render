#!/usr/bin/env python3
"""
first_last_rera_joinville.py
----------------------------
Télécharge le GTFS IDFM (via ton worker-proxy *ou* en direct) puis calcule,
pour la date voulue (aujourd’hui par défaut), les premiers et derniers
passages du RER A à Joinville-le-Pont dans les deux sens.

• Dépendances : duckdb, pandas, requests, tqdm, python-dateutil
• Variables d’environnement :
    - PROXY_WORKER  (facultatif) ex. "https://ratp-proxy.hippodrome-proxy42.workers.dev/?url="
    - IDFM_APIKEY   (facultatif **uniquement si tu n’utilises pas le proxy**)
"""

from __future__ import annotations

import argparse, datetime as dt, io, os, sys, zipfile
from pathlib import Path
from urllib.parse import quote_plus

import duckdb, pandas as pd, requests
from dateutil import tz
from tqdm import tqdm

# ────────────────────────── arguments CLI ──────────────────────────
parser = argparse.ArgumentParser(
    description="Premier / dernier passage du RER A à Joinville-le-Pont."
)
parser.add_argument(
    "-d", "--date",
    type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
    default=dt.date.today(),
    help="Date étudiée (AAAA-MM-JJ, défaut : aujourd’hui)",
)
parser.add_argument(
    "--save-json",
    metavar="FICHIER",
    help="Écrit aussi le résultat dans ce fichier JSON",
)
args = parser.parse_args()

DAY = args.date
ROUTE_ID = "STIF:Line::C01742:"                      # identifiant RER A

# ────────────────────────── construire l’URL GTFS ──────────────────────────
GTFS_URL_DIRECT = (
    "https://prim.iledefrance-mobilites.fr/"
    "marketplace/offer-horaires-tc-gtfs-idfm"
)
PROXY_WORKER = os.getenv("PROXY_WORKER")             # ex. …/workers.dev/?url=
if PROXY_WORKER:
    prefix = PROXY_WORKER if PROXY_WORKER.endswith("?url=") else PROXY_WORKER + "?url="
    GTFS_URL = f"{prefix}{quote_plus(GTFS_URL_DIRECT, safe='')}"
    API_KEY  = None                                  # le proxy ajoutera déjà la clé
else:
    GTFS_URL = GTFS_URL_DIRECT
    API_KEY  = os.getenv("IDFM_APIKEY")
    if not API_KEY:
        sys.exit("❌  IDFM_APIKEY manquant (pas de proxy défini)")

HEADERS = {"apikey": API_KEY} if API_KEY else {}

# ────────────────────────── cache local du ZIP ──────────────────────────
CACHE_DIR = Path(__file__).with_suffix(".d")
CACHE_DIR.mkdir(exist_ok=True)
LOCAL_ZIP = CACHE_DIR / f"gtfs_{DAY:%Y%m%d}.zip"

def download_gtfs() -> Path:
    if LOCAL_ZIP.exists():
        return LOCAL_ZIP
    print("⬇️  Téléchargement du GTFS IDFM …")
    with requests.get(GTFS_URL, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="o", unit_scale=True) as bar, LOCAL_ZIP.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
    return LOCAL_ZIP

zip_path = download_gtfs()

# ────────────────────────── ouverture DuckDB ──────────────────────────
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")

def csv(path: str) -> str:
    return f"zip://{path}?{zip_path}"

con.execute(f"CREATE VIEW stops      AS SELECT * FROM read_csv_auto('{csv('stops.txt')}');")
con.execute(f"CREATE VIEW stop_times AS SELECT * FROM read_csv_auto('{csv('stop_times.txt')}');")
con.execute(f"CREATE VIEW trips      AS SELECT * FROM read_csv_auto('{csv('trips.txt')}');")
con.execute(f"CREATE VIEW calendar   AS SELECT * FROM read_csv_auto('{csv('calendar.txt')}');")
con.execute(f"CREATE VIEW cal_dates  AS SELECT * FROM read_csv_auto('{csv('calendar_dates.txt')}');")

# ────────────────────────── stop_id de Joinville ──────────────────────────
stops_df = con.execute("""
    SELECT stop_id FROM stops
    WHERE lower(stop_name) LIKE '%joinville-le-pont%'
""").fetch_df()
if stops_df.empty:
    sys.exit("❌  Joinville-le-Pont introuvable dans ce GTFS")
STOP_IDS = tuple(stops_df["stop_id"])

# ────────────────────────── services actifs ──────────────────────────
day_int = int(DAY.strftime("%Y%m%d"))
weekday = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"][DAY.weekday()]
service_ids_df = con.execute(f"""
    SELECT service_id FROM calendar
    WHERE {day_int} BETWEEN start_date AND end_date AND {weekday}=1
    UNION
    SELECT service_id FROM cal_dates
    WHERE date={day_int} AND exception_type=1
""").fetch_df()
if service_ids_df.empty:
    sys.exit("❌  Aucun service actif ce jour-là")
SERVICE_IDS = tuple(service_ids_df["service_id"])

# ────────────────────────── premier / dernier ──────────────────────────
query = f"""
    SELECT
        trips.direction_id,
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

label = {0: "→ Paris / St-Germain", 1: "→ Boissy / MLV"}
print(f"\nRER A – Joinville-le-Pont  |  {DAY:%Y-%m-%d}\n")
for _, row in result.iterrows():
    print(f"{label.get(row['direction_id'], row['direction_id'])}")
    print(f"  Premier : {row['first_time']}")
    print(f"  Dernier : {row['last_time']}\n")

if args.save_json:
    out = result.rename(
        columns={"direction_id":"direction","first_time":"first","last_time":"last"}
    ).to_dict(orient="records")
    Path(args.save_json).write_text(pd.json.dumps(out, indent=2, ensure_ascii=False))
    print(f"💾  Enregistré dans {args.save_json}")
