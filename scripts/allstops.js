import zipfile
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta

GTFS_URL = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
ARRET_JOINVILLE = "IDFM:87759009"  # stop_id Joinville-le-Pont RER
ROUTE_RER_A = "IDFM:C01742"        # route_id RER A

# On cible aujourd'hui
today = datetime.now().date()
day_str = today.strftime("%Y%m%d")

resp = requests.get(GTFS_URL)
with zipfile.ZipFile(BytesIO(resp.content)) as z:
    stops = pd.read_csv(z.open("stops.txt"))
    stop_times = pd.read_csv(z.open("stop_times.txt"), low_memory=False)
    trips = pd.read_csv(z.open("trips.txt"), low_memory=False)
    routes = pd.read_csv(z.open("routes.txt"))
    calendar = pd.read_csv(z.open("calendar.txt"))
    if "calendar_dates.txt" in z.namelist():
        calendar_dates = pd.read_csv(z.open("calendar_dates.txt"))
    else:
        calendar_dates = pd.DataFrame()

# 1. Service_ids actifs aujourd'hui pour RER A
dow = today.weekday()
active_service_ids = []
for idx, row in calendar.iterrows():
    start = datetime.strptime(str(row['start_date']), "%Y%m%d").date()
    end = datetime.strptime(str(row['end_date']), "%Y%m%d").date()
    if not (start <= today <= end):
        continue
    if dow < 5 and row['monday']: active_service_ids.append(row['service_id'])
    if dow == 5 and row['saturday']: active_service_ids.append(row['service_id'])
    if dow == 6 and row['sunday']: active_service_ids.append(row['service_id'])
if not calendar_dates.empty:
    today_exceptions = calendar_dates[calendar_dates['date'] == int(day_str)]
    for _, ex in today_exceptions.iterrows():
        if ex['exception_type'] == 1 and ex['service_id'] not in active_service_ids:
            active_service_ids.append(ex['service_id'])
        if ex['exception_type'] == 2 and ex['service_id'] in active_service_ids:
            active_service_ids.remove(ex['service_id'])

# 2. Trips RER A du jour
trips_today = trips[(trips['route_id'] == ROUTE_RER_A) & (trips['service_id'].isin(active_service_ids))]
trip_ids_today = trips_today['trip_id'].tolist()

# 3. Tous les passages à Joinville-le-Pont pour le jour
passages = stop_times[(stop_times['stop_id'] == ARRET_JOINVILLE) & (stop_times['trip_id'].isin(trip_ids_today))]
passages = passages.merge(trips_today[['trip_id', 'trip_headsign']], on='trip_id')

results = []

for _, row in passages.iterrows():
    trip_id = row['trip_id']
    heure = row['departure_time'][:5]
    destination = row['trip_headsign']
    # Liste complète des arrêts après Joinville-le-Pont (y compris)
    stops_seq = stop_times[(stop_times['trip_id'] == trip_id) & (stop_times['stop_sequence'] >= row['stop_sequence'])].sort_values('stop_sequence')['stop_id'].tolist()
    stop_names = stops[stops['stop_id'].isin(stops_seq)].set_index('stop_id')['stop_name'].to_dict()
    ordered_stop_names = [stop_names.get(sid, sid) for sid in stops_seq]
    results.append({
        "mission": trip_id.split(":")[-1],  # si code mission dans trip_id
        "heure": heure,
        "destination": destination,
        "gares_restantes": ordered_stop_names
    })

import json
with open("static/rer_a_prochains_trains.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print("✅ Exporté static/rer_a_prochains_trains.json")
