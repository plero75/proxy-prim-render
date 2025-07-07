import zipfile
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta

GTFS_URL = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
STOP_ID = "IDFM:87759009"  # Joinville-le-Pont RER
ROUTE_ID = "IDFM:C01742"   # RER A

today = datetime.now()
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

# Détermination des services actifs aujourd'hui pour le RER A
dow = today.weekday()
active_service_ids = []
for idx, row in calendar.iterrows():
    start = datetime.strptime(str(row['start_date']), "%Y%m%d").date()
    end = datetime.strptime(str(row['end_date']), "%Y%m%d").date()
    if not (start <= today.date() <= end):
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

# Filtrer les trips du jour sur la ligne RER A
trips_today = trips[(trips['route_id'] == ROUTE_ID) & (trips['service_id'].isin(active_service_ids))]
trip_ids_today = trips_today['trip_id'].tolist()

# Tous les passages à Joinville-le-Pont pour ces trips
passages = stop_times[(stop_times['stop_id'] == STOP_ID) & (stop_times['trip_id'].isin(trip_ids_today))]
passages = passages.merge(trips_today[['trip_id', 'trip_headsign']], on='trip_id')

# Calcul du temps d'attente
def next_time_to_minutes(departure_str, ref_dt):
    h, m, *_ = map(int, departure_str.split(":"))
    target = ref_dt.replace(hour=h, minute=m, second=0, microsecond=0)
    if h >= 24:
        target += timedelta(days=1)
        target = target.replace(hour=h - 24)
    delta = (target - ref_dt).total_seconds() // 60
    return int(delta)

# Grouper par direction
results = {}
for direction, group in passages.groupby('trip_headsign'):
    trains = []
    group = group.sort_values('departure_time')
    for _, row in group.head(4).iterrows():
        trip_id = row['trip_id']
        heure = row['departure_time'][:5]
        minutes = next_time_to_minutes(row['departure_time'], today)
        stops_seq = stop_times[(stop_times['trip_id'] == trip_id) & (stop_times['stop_sequence'] >= row['stop_sequence'])].sort_values('stop_sequence')['stop_id'].tolist()
        stop_names = stops.set_index('stop_id').loc[stops_seq]['stop_name'].tolist()
        trains.append({
            "mission": trip_id.split(":")[-1],
            "heure": heure,
            "minutes": minutes,
            "gares": stop_names,
            "status": "on time"
        })
    results[direction] = trains

# Export des prochains trains par direction
import json
with open("static/rer_a_prochains_trains_by_direction.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print("✅ Exporté static/rer_a_prochains_trains_by_direction.json")

# Calcul des horaires premier et dernier passage théorique
firsts = passages.groupby('trip_headsign')['departure_time'].min()
lasts = passages.groupby('trip_headsign')['departure_time'].max()

horaires_export = {
    "rer_a": {
        "premier": firsts.min()[:5] if not firsts.empty else "N/A",
        "dernier": lasts.max()[:5] if not lasts.empty else "N/A"
    }
}

with open("static/horaires_export.json", "w", encoding="utf-8") as f:
    json.dump(horaires_export, f, indent=2, ensure_ascii=False)
print("✅ Exporté static/horaires_export.json")
