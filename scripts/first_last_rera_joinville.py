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
    AND t.service_id IN {SERVICE_IDS}
  ORDER BY st.departure_time
""").fetch_df()

def to_iso(t: str) -> str:
    h, m, s = map(int, t.split(":"))
    base = dt.datetime.combine(DAY, dt.time(0, 0, tzinfo=tz.gettz("Europe/Paris")))
    if h >= 24:
        h -= 24
        base += dt.timedelta(days=1)
    return (base + dt.timedelta(hours=h, minutes=m, seconds=s)).isoformat()

def remaining(trip_id: str, join_id: str) -> list[str]:
    df = con.execute(f"""
        SELECT stop_id, stop_sequence FROM stop_times
        WHERE trip_id='{trip_id}' ORDER BY stop_sequence
    """).fetch_df()
    seq = df.loc[df.stop_id == join_id, "stop_sequence"].iloc[0]
    after = df[df.stop_sequence > seq].stop_id.tolist()
    if not after: return []
    names = con.execute(f"SELECT stop_id, stop_name FROM stops WHERE stop_id IN {tuple(after)}")\
        .fetch_df().set_index("stop_id").stop_name.to_dict()
    return [names[s] for s in after]

labels = {0: "→ Paris / St-Germain", 1: "→ Boissy / MLV"}
records = []
for _, row in passages.iterrows():
    records.append({
        "time": to_iso(row.departure_time),
        "direction": labels.get(row.direction_id, row.direction_id),
        "destination": row.destination,
        "remaining_stops": remaining(row.trip_id, row.stop_id)
    })

# export JSON
out_path = Path(args.save_json)
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
print(f"💾 {len(records)} passages écrits dans {out_path}")
