import os
import zipfile
import pandas as pd
import requests
from io import BytesIO
import json

APIKEY = os.environ.get("IDFM_APIKEY")
if not APIKEY:
    raise RuntimeError("IDFM_APIKEY n'est pas défini dans les variables d'environnement.")

url = f"https://data.iledefrance-mobilites.fr/explore/dataset/gtfs-horaires/download/?format=zip&apikey={APIKEY}"

print("Téléchargement du GTFS sécurisé...")
resp = requests.get(url)
with zipfile.ZipFile(BytesIO(resp.content)) as z:
    stops = pd.read_csv(z.open("stops.txt"))
    stop_times = pd.read_csv(z.open("stop_times.txt"))
    trips = pd.read_csv(z.open("trips.txt"))
    calendar = pd.read_csv(z.open("calendar.txt"))
    routes = pd.read_csv(z.open("routes.txt"))

output = {}
target_route_short_name = "A"  # Exemple : RER A
rera_route_ids = routes[routes['route_short_name'] == target_route_short_name]['route_id'].tolist()
rera_trip_ids = trips[trips['route_id'].isin(rera_route_ids)]['trip_id'].tolist()

for stop_id in stops['stop_id']:
    nom = stops[stops['stop_id'] == stop_id]['stop_name'].values[0]
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
        trip_ids = set(trips[trips['service_id'] == sid]['trip_id'])
        times = stop_times[(stop_times['stop_id'] == stop_id) & (stop_times['trip_id'].isin(trip_ids))]['departure_time']
        horaires[key] += list(times)
    premiers = {k: min(v) if v else None for k, v in horaires.items()}
    derniers = {k: max(v) if v else None for k, v in horaires.items()}
    # Gares desservies par RER A (pour ce stop_id)
    trips_with_stop = stop_times[(stop_times['stop_id'] == stop_id) & (stop_times['trip_id'].isin(rera_trip_ids))]['trip_id']
    desserte_stops = stop_times[stop_times['trip_id'].isin(trips_with_stop)]['stop_id'].unique().tolist()
    output[stop_id] = {
        "nom": nom,
        "premier": premiers,
        "dernier": derniers,
        "dessertes_RERA": desserte_stops
    }

os.makedirs("static", exist_ok=True)
with open("static/gtfs-stops-full.json", "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)
print("✅ Données GTFS exportées dans static/gtfs-stops-full.json")
