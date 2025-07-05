#!/usr/bin/env python3
"""
first_last_rera_joinville.py
----------------------------
Télécharge le dernier GTFS « offre horaires » Île-de-France Mobilités via
un worker-proxy Cloudflare, puis calcule — pour la date choisie — les premiers
et derniers passages du RER A à Joinville-le-Pont (deux sens).

Variable d’environnement OBLIGATOIRE
------------------------------------
PROXY_WORKER = "https://<ton-worker>.workers.dev/?url="   (inclut déjà '?url=')
"""

from __future__ import annotations
import argparse, datetime as dt, json, os, sys
from pathlib import Path
from urllib.parse import quote_plus

import duckdb, pandas as pd, requests
from dateutil import tz
from tqdm import tqdm

# ────────────────────────── CLI ──────────────────────────
parser = argparse.ArgumentParser(
    description="Premier & dernier passage RER A à Joinville-le-Pont"
)
parser.add_argument(
    "-d", "--date",
    type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
    default=dt.date.today(),
    help="Date étudiée (AAAA-MM-JJ, défaut : aujourd’hui)",
)
parser.add_argument(
    "--save-json", metavar="FICHIER",
    help="Enregistre le résultat dans ce fichier JSON",
)
args = parser.parse_args()

DAY       = args.date
ROUTE_ID  = "STIF:Line::C01742:"      # identifiant RER A

# ────────────────────────── Proxy Cloudflare ─────────────
WORKER = os.getenv("PROXY_WORKER")
if not WORKER:
    sys.exit("❌  PROXY_WORKER n’est pas défini")

def proxify(url: str) -> str:
    prefix = WORKER if WORKER.endswith("?url=") else WORKER + "?url="
    return f"{prefix}{quote_plus(url, safe='')}"

# ────────────────────────── API ODS (attachments) ────────
ATTACH_API = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/"
    "datasets/offre-horaires-tc-gtfs-idfm/attachments"
)
LOCAL_ZIP = Path("gtfs_idfm_latest.zip")  # Déclaration ici

def latest_zip_url() -> str:
    """Retourne l’URL .zip la plus récente du dataset GTFS IDFM."""
    resp = requests.get(proxify(ATTACH_API), timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    # Gestion de la structure de la réponse
    attachments = []
    if isinstance(raw, dict):
        if "attachments" in raw:
            attachments = raw["attachments"]
        elif "resources" in raw:
            attachments = raw["resources"]
        else:
            print("Réponse API inattendue :", json.dumps(raw, indent=2, ensure_ascii=False), file=sys.stderr)
            raise RuntimeError("Aucun champ 'attachments' ou 'resources' trouvé dans la réponse API")
    else:
        attachments = raw

    zips = [
        (att["updated_at"], att["url"])
        for att in attachments
        if att.get("url", "").lower().endswith(".zip")
    ]
    if not zips:
        print("Réponse API :", json.dumps(raw, indent=2, ensure_ascii=False), file=sys.stderr)
        raise RuntimeError("Aucun ZIP trouvé dans le dataset")
    zips.sort(reverse=True)
    return zips[0][1]

def download_gtfs() -> Path:
    if LOCAL_ZIP.exists():
        return LOCAL_ZIP
    url = latest_zip_url()
    print("🚦 Téléchargement du GTFS :", url)
    with requests.get(proxify(url), stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="o", unit_scale=True) as bar, LOCAL_ZIP.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
    return LOCAL_ZIP

zip_path = download_gtfs()

# ────────────────────────── Lecture DuckDB ───────────────
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")
csv = lambda p: f"zip://{p}?{zip_path}"

for name in ("stops", "stop_times", "trips", "calendar", "cal_dates"):
    con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_csv_auto('{csv(name + '.txt')}');")

# ────────────────────────── stop_id Joinville ────────────
stops_df = con.execute("""
    SELECT stop_id FROM stops
    WHERE lower(stop_name) LIKE '%joinville-le-pont%'
""").fetch_df()
if stops_df.empty:
    sys.exit("Aucun stop_id pour Joinville-le-Pont")
STOP_IDS = tuple(stops_df.stop_id)

# ────────────────────────── Services actifs ──────────────
day_int  = int(DAY.strftime("%Y%m%d"))
weekday  = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"][DAY.weekday()]

svc_df = con.execute(f"""
    SELECT service_id FROM calendar
    WHERE {day_int} BETWEEN start_date AND end_date AND {weekday}=1
    UNION
    SELECT service_id FROM cal_dates
    WHERE date={day_int} AND exception_type=1
""").fetch_df()
SERVICE_IDS = tuple(svc_df.service_id)

# ────────────────────────── Calcul premier/dernier ───────
result = con.execute(f"""
    SELECT trips.direction_id,
           MIN(stop_times.departure_time) AS first_time,
           MAX(stop_times.departure_time) AS last_time
    FROM stop_times
    JOIN trips ON trips.trip_id = stop_times.trip_id
    WHERE stop_times.stop_id IN {STOP_IDS}
      AND trips.route_id = '{ROUTE_ID}'
      AND trips.service_id IN {SERVICE_IDS}
    GROUP BY trips.direction_id
    ORDER BY direction_id
""").fetch_df()

def to_iso(t: str) -> str:
    h,m,s = map(int, t.split(":"))
    base  = dt.datetime.combine(DAY, dt.time(0,0,tzinfo=tz.gettz("Europe/Paris")))
    if h >= 24:
        h -= 24
        base += dt.timedelta(days=1)
    return (base + dt.timedelta(hours=h, minutes=m, seconds=s)).isoformat()

result["first_time"] = result.first_time.map(to_iso)
result["last_time"]  = result.last_time.map(to_iso)

labels = {0:"→ Paris / St-Germain", 1:"→ Boissy / MLV"}
print(f"\nRER A – Joinville-le-Pont | {DAY:%Y-%m-%d}\n")
for _, row in result.iterrows():
    print(f"{labels.get(row.direction_id,row.direction_id)}")
    print(f"  Premier : {row.first_time}")
    print(f"  Dernier : {row.last_time}\n")

# ────────────────────────── Export optionnel ─────────────
if args.save_json:
    out = result.rename(columns={
        "direction_id": "direction",
        "first_time":   "first",
        "last_time":    "last",
    }).to_dict("records")
    Path(args.save_json).write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print("💾  JSON écrit →", args.save_json)
