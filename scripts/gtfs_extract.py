import zipfile
import pandas as pd
import requests
from io import BytesIO
import json
import os

GTFS_URL = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"

print("Téléchargement du GTFS IDFM...")
resp = requests.get(GTFS_URL)
with zipfile.ZipFile(BytesIO(resp.content)) as z:
    stops = pd.read_csv(z.open("stops.txt"))
    stop_times = pd.read_csv(z.open("stop_times.txt"))
    trips = pd.read_csv(z.open("trips.txt"))
    calendar = pd.read_csv(z.open("calendar.txt"))
    routes = pd.read_csv(z.open("routes.txt"))

target_route_short_name = "A"  # RER A
route_ids = routes[routes['route_short_name'] == target_route_short_name]['route_id'].tolist()
trips_rera = trips[trips['route_id'].isin(route_ids)]
trip_ids = trips_rera['trip_id'].tolist()

# Récupère tous les arrêts sur la ligne
stoptimes_rera = stop_times[stop_times['trip_id'].isin(trip_ids)]
stop_ids = stoptimes_rera['stop_id'].unique()
stops_rera = stops[stops['stop_id'].isin(stop_ids)]

output = {}

for stop_id in stop_ids:
    nom = stops_rera[stops_rera['stop_id'] == stop_id]['stop_name'].values[0]
    horaires = {"Lun-Ven": [], "Sam": [], "Dim": []}
    for idx, row in calendar.iterrows():
        sid = row['service_id']
        if row['monday'] or row['tuesday'] or row['wednesday'] or row['thursday'] or row['friday']:
            key = "Lun-Ven"
        elif row['saturday']:
            key = "Sam"
        elif row['sunday']:
            key = "Dim"
        else:
            continue
        trips_for_service = trips_rera[trips_rera['service_id'] == sid]['trip_id']
        times = stoptimes_rera[(stoptimes_rera['stop_id'] == stop_id) & (stoptimes_rera['trip_id'].isin(trips_for_service))]['departure_time']
        horaires[key] += list(times)
    premiers = {k: min(v) if v else None for k, v in horaires.items()}
    derniers = {k: max(v) if v else None for k, v in horaires.items()}
    output[stop_id] = {
        "nom": nom,
        "premier": premiers,
        "dernier": derniers
    }

os.makedirs("static", exist_ok=True)
with open("static/gtfs-stops-full.json", "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)
print("✅ Données GTFS RER A exportées dans static/gtfs-stops-full.json")
