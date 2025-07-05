
#!/usr/bin/env python3
"""
first_last_rera_joinville.py
----------------------------
Télécharge le GTFS IDFM (via un worker-proxy facultatif) et calcule,
pour une date donnée (aujourd’hui par défaut), les premiers et derniers
passages du RER A à la gare de Joinville-le-Pont dans les deux sens.

Dépendances : duckdb, pandas, requests, tqdm, python-dateutil
Variables d’environnement :
    - IDFM_APIKEY   → jeton « données statiques » PRIM
    - PROXY_WORKER  → https://<ton-worker>.workers.dev    (facultatif)
"""
from __future__ import annotations
import argparse, datetime as dt, io, os, sys, zipfile
from pathlib import Path
from urllib.parse import quote_plus

import duckdb, pandas as pd, requests
from dateutil import tz
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Premier / dernier passage du RER A à Joinville-le-Pont."
)
parser.add_argument(
    "-d", "--date",
    type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
    default=dt.date.today(),
    help="Date d’étude (AAAA-MM-JJ, défaut : aujourd’hui)",
)
parser.add_argument(
    "--save-json",
    metavar="FICHIER",
    help="Chemin d’un fichier JSON pour sauvegarder le résultat",
)
args = parser.parse_args()

DAY = args.date
ROUTE_ID = "STIF:Line::C01742:"          # RER A

GTFS_URL_DIRECT = (
    "https://prim.iledefrance-mobilites.fr/marketplace/offer-horaires-tc-gtfs-idfm"
)
PROXY_WORKER = os.getenv("PROXY_WORKER")
GTFS_URL = (
    f"{PROXY_WORKER}?url={quote_plus(GTFS_URL_DIRECT, safe='')}" if PROXY_WORKER else GTFS_URL_DIRECT
)

API_KEY = os.getenv("IDFM_APIKEY")
if not API_KEY:
    sys.exit("IDFM_APIKEY manquant")

CACHE_DIR = Path(__file__).with_suffix(".d")
CACHE_DIR.mkdir(exist_ok=True)
LOCAL_ZIP = CACHE_DIR / f"gtfs_{DAY:%Y%m%d}.zip"

def download_gtfs():
    if LOCAL_ZIP.exists():
        return LOCAL_ZIP
    headers = {"apikey": API_KEY}
    with requests.get(GTFS_URL, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="o", unit_scale=True) as bar, LOCAL_ZIP.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
    return LOCAL_ZIP

zip_path = download_gtfs()

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")

def csv(name: str) -> str:
    return f"zip://{name}?{zip_path}"

con.execute(f"""CREATE VIEW stops      AS SELECT * FROM read_csv_auto('{csv('stops.txt')}');""")
con.execute(f"""CREATE VIEW stop_times AS SELECT * FROM read_csv_auto('{csv('stop_times.txt')}');""")
con.execute(f"""CREATE VIEW trips      AS SELECT * FROM read_csv_auto('{csv('trips.txt')}');""")
con.execute(f"""CREATE VIEW calendar   AS SELECT * FROM read_csv_auto('{csv('calendar.txt')}');""")
con.execute(f"""CREATE VIEW cal_dates  AS SELECT * FROM read_csv_auto('{csv('calendar_dates.txt')}');""")

stops_df = con.execute("""SELECT stop_id FROM stops WHERE lower(stop_name) LIKE '%joinville-le-pont%'""").fetch_df()
if stops_df.empty:
    sys.exit("Joinville non trouvée")
STOP_IDS = tuple(stops_df["stop_id"])

day_int = int(DAY.strftime("%Y%m%d"))
weekday_fields = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]
weekday_flag = weekday_fields[DAY.weekday()]
service_ids_df = con.execute(f"""SELECT service_id FROM calendar WHERE {day_int} BETWEEN start_date AND end_date AND {weekday_flag}=1 UNION SELECT service_id FROM cal_dates WHERE date={day_int} AND exception_type=1""").fetch_df()
if service_ids_df.empty:
    sys.exit("Pas de service actif ce jour")
SERVICE_IDS = tuple(service_ids_df["service_id"])

query = f"""SELECT trips.direction_id, MIN(stop_times.departure_time) AS first_time, MAX(stop_times.departure_time) AS last_time FROM stop_times JOIN trips ON trips.trip_id = stop_times.trip_id WHERE stop_times.stop_id IN {STOP_IDS} AND trips.route_id = '{ROUTE_ID}' AND trips.service_id IN {SERVICE_IDS} GROUP BY trips.direction_id ORDER BY direction_id;"""
result = con.execute(query).fetch_df()

def gtfs_to_iso(t:str)->str:
    h,m,s=map(int,t.split(":")); base=dt.datetime.combine(DAY, dt.time(0,0,tzinfo=tz.gettz("Europe/Paris"))); 
    if h>=24: h-=24; base+=dt.timedelta(days=1)
    return (base+dt.timedelta(hours=h,minutes=m,seconds=s)).isoformat()

result["first_time"]=result["first_time"].map(gtfs_to_iso)
result["last_time"]=result["last_time"].map(gtfs_to_iso)

label={0:"→ Paris / St‑Germain",1:"→ Boissy / MLV"}
print(f"\nRER A – Joinville‑le‑Pont ({DAY})\n")
for _,row in result.iterrows():
    print(f"{label.get(row['direction_id'],row['direction_id'])}\n  Premier: {row['first_time']}\n  Dernier : {row['last_time']}\n")

if args.save_json:
    out=result.rename(columns={"direction_id":"direction","first_time":"first","last_time":"last"}).to_dict(orient="records")
    Path(args.save_json).write_text(json.dumps(out,indent=2,ensure_ascii=False))
