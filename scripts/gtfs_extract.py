import zipfile
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta

GTFS_URL = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
STOP_ID = "stop_area:IDFM:87759009"  # Joinville-le-Pont
ROUTE_SHORT_NAME = "A"

# Récupère la date du jour et les 4 suivants
today = datetime.now().date()
days = [today + timedelta(days=i) for i in range(5)]

print("Téléchargement du GTFS IDFM...")
resp = requests.get(GTFS_URL)
with zipfile.ZipFile(BytesIO(resp.content)) as z:
    stops = pd.read_csv(z.open("stops.txt"))
    stop_times = pd.read_csv(z.open("stop_times.txt"))
    trips = pd.read_csv(z.open("trips.txt"))
    calendar = pd.read_csv(z.open("calendar.txt"))
    if "calendar_dates.txt" in z.namelist():
        calendar_dates = pd.read_csv(z.open("calendar_dates.txt"))
    else:
        calendar_dates = pd.DataFrame()
    routes = pd.read_csv(z.open("routes.txt"))

# Filtre la ligne (RER A)
route_ids = routes[routes['route_short_name'] == ROUTE_SHORT_NAME]['route_id'].tolist()
trips_rera = trips[trips['route_id'].isin(route_ids)]

results = {}

for day in days:
    day_str = day.strftime("%Y%m%d")
    dow = day.weekday()  # 0=lundi ... 6=dimanche

    # Pour chaque service_id, vérifie si ce jour est inclus dans la période et le bon type de jour
    active_service_ids = []
    for idx, row in calendar.iterrows():
        start = datetime.strptime(str(row['start_date']), "%Y%m%d").date()
        end = datetime.strptime(str(row['end_date']), "%Y%m%d").date()
        if not (start <= day <= end):
            continue
        if dow < 5 and row['monday']: active_service_ids.append(row['service_id'])
        if dow == 5 and row['saturday']: active_service_ids.append(row['service_id'])
        if dow == 6 and row['sunday']: active_service_ids.append(row['service_id'])
    # Gère les exceptions (ajouts/suppressions ponctuels)
    if not calendar_dates.empty:
        today_exceptions = calendar_dates[calendar_dates['date'] == int(day_str)]
        for _, ex in today_exceptions.iterrows():
            if ex['exception_type'] == 1 and ex['service_id'] not in active_service_ids:
                active_service_ids.append(ex['service_id'])
            if ex['exception_type'] == 2 and ex['service_id'] in active_service_ids:
                active_service_ids.remove(ex['service_id'])

    # Liste les trips actifs ce jour
    trips_today = trips_rera[trips_rera['service_id'].isin(active_service_ids)]
    trip_ids_today = trips_today['trip_id'].tolist()
    horaires_today = stop_times[(stop_times['stop_id'] == STOP_ID) & (stop_times['trip_id'].isin(trip_ids_today))]['departure_time'].tolist()
    horaires_today.sort()
    results[day.strftime("%A %d/%m/%Y")] = horaires_today

# Affichage :
for d, horaires in results.items():
    print(f"{d}:")
    print(", ".join(horaires[:5]), "...", ", ".join(horaires[-5:]) if horaires else "Aucun passage ce jour")
import json
import os

os.makedirs("static", exist_ok=True)
with open("static/gtfs-stops-full.json", "w") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
