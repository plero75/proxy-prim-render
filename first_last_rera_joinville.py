#!/usr/bin/env python3
"""
first_last_rera_joinville.py
----------------------------
Télécharge le GTFS IDFM depuis l’API OpenDataSoft (via ton worker-proxy),
puis calcule, pour la date voulue (aujourd’hui par défaut), les premiers
et derniers passages du RER A à Joinville-le-Pont dans les deux sens.

• Dépendances : duckdb, pandas, requests, tqdm, python-dateutil
• Variable obligatoire  : PROXY_WORKER = "https://<ton-worker>.workers.dev/?url="
• Aucune clé PRIM n’est requise (jeu open data).
"""

from __future__ import annotations

import argparse, datetime as dt, io, os, sys
from pathlib import Path
from urllib.parse import quote_plus

import duckdb, pandas as pd, requests
from dateutil import tz
from tqdm import tqdm

# ────────────────────────── CLI ──────────────────────────
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

# ────────────────────────── Proxy obligatoire ──────────────────────────
PROXY_WORKER = os.getenv("PROXY_WORKER")             # …/workers.dev/?url=
if not PROXY_WORKER:
    sys.exit("❌  PROXY_WORKER n’est pas défini")

def proxify(url: str) -> str:
    prefix = PROXY_WORKER if PROXY_WORKER.endswith("?url=") else PROXY_WORKER + "?url="
    return f"{prefix}{quote_plus(url, safe='')}"

# ────────────────────────── Source ODS ──────────────────────────
ODS_DATASET_API = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/"
    "catalog/datasets/offre-horaires-tc-gtfs-idfm/exports/json"
)

def latest_zip_href() -> str:
    resp = requests.get(proxify(ODS_DATASET_API), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    zips = [
        (att["updated_at"], att["href"])
        for att in data.get("attachments", [])
        if att["href"].lower().endswith(".zip")
    ]
    if not zips:
        raise RuntimeError("Aucun ZIP trouvé dans le dataset ODS")
    zips.sort(reverse=True)              # le plus récent en premier
    return zips[0][1]

# ────────────────────────── Téléchargement / cache ──────────────────────────
CACHE_DIR = Path(__file__).with_suffix(".d")
CACHE_DIR.mkdir(exist_ok=True)
LOCAL_ZIP = CACHE_DIR / "gtfs_latest.zip"

def download_gtfs() -> Path:
    if LOCAL_ZIP.exists():
        return LOCAL_ZIP
    zip_url = latest_zip_href()
    print("⬇️  Téléchargement du GTFS via ODS …")
    with requests.get(proxify(zip_url), stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="o", unit_scale=True) as bar, LOCAL_ZIP.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
    return LOCAL_ZIP

zip_path = download_gtfs()

# ────────────────────────── DuckDB in-place ──────────────────────────
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")
def csv(path: str) -> str: return f"zip://{path}?{zip_path}"

con.execute(f"CREATE VIEW stops      AS SELECT * FROM read_csv_auto('{csv('stops.txt')}');")
con.execute(f"CREATE VIEW stop_times AS SELECT * FROM read_csv_auto('{csv('stop_times.txt')}');")
con.execute(f"CREATE VIEW trips      AS SELECT * FROM read_csv_auto('{csv('trips.txt')}');")
con.execute(f"CREATE VIEW calendar   AS SELECT * FROM read_csv_auto('{csv('calendar.txt')}');")
con.execute(f"CREATE VIEW cal_dates  AS SELECT * FROM read_csv_auto('{csv('calendar_dates.txt')}');")

# ────────────────────────── stop_id Joinville ──────────────────────────
stops_df = con.execute("""
    SELECT stop_id FROM stops
    WHERE lower(stop_name) LIKE '%joinville-le-pont%'
""").fetch_df()
if stops_df.empty:
    sys.exit("❌  Joinville-le-Pont introuvable dans ce GTFS")
STOP_IDS = tuple(stops_df["stop_id"])

# ────────────────────────── services actifs ──────────────────────────
day_int  = int(DAY.strftime("%Y%m%d"))
weekday  = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"][DAY.weekday()]
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
    if h >= 24: h -= 24; base += dt.timedelta(days=1)
    return (base + dt.timedelta(hours=h, minutes=m, seconds=s)).isoformat()

result["first_time"] = result["first_time"].map(gtfs_to_iso)
result["last_time"]  = result["last_time"].map(gtfs_to_iso)

# ────────────────────────── affichage / export ──────────────────────────
label = {0:"→ Paris / St-Germain", 1:"→ Boissy / MLV"}
print(f"\nRER A – Joinville-le-Pont | {DAY:%Y-%m-%d}\n")
for _, row in result.iterrows():
    print(f"{label.get(row['direction_id'], row['direction_id'])}")
    print(f"  Premier : {row['first_time']}\n  Dernier  : {row['last_time']}\n")

if args.save_json:
    out = result.rename(columns={"direction_id":"direction",
                                 "first_time":"first",
                                 "last_time":"last"}).to_dict(orient="records")
    Path(args.save_json).write_text(pd.json.dumps(out, indent=2, ensure_ascii=False))
    print(f"💾  Enregistré dans {args.save_json}")
