import zipfile
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime

GTFS_URL = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
TARGET_STOP = "Joinville-le-Pont"
TARGET_ROUTE = "RER A"

now = datetime.now()
today = now.date()
day_str = today.strftime("%Y%m%d")

resp = requests.get(GTFS_URL)
with zipfile.ZipFile(BytesIO(resp.content)) as z:
    stops = pd.read_csv(z.open("stops.txt"))
    routes = pd.read_csv(z.open("routes.txt"))
    trips = pd.read_csv(z.open("trips.txt"))
    stop_times = pd.read_csv(z.open("stop_times.txt"))
    calendar = pd.read_csv(z.open("calendar.txt"))
    if "calendar_dates.txt" in z.namelist():
        calendar_dates = pd.read_csv(z.open("calendar_dates.txt"))
    else:
        calendar_dates = pd.DataFrame()

# Tous les stop_ids Joinville-le-Pont
joinville_stops = stops[stops['stop_name'].str.contains(TARGET_STOP, case=False)]
joinville_stop_ids = joinville_stops['stop_id'].tolist()

# route_id(s) RER A
rer_a_routes = routes[
    (routes['route_long_name'].str.contains(TARGET_ROUTE, case=False, na=False)) |
    (routes['route_short_name'].str.contains("A", case=False, na=False))
]
rer_a_route_ids = rer_a_routes['route_id'].unique().tolist()

# Services actifs aujourd'hui
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

# Trips RER A aujourd'hui
trips_today = trips[
    (trips['route_id'].isin(rer_a_route_ids)) &
    (trips['service_id'].isin(active_service_ids))
]
trip_ids_today = trips_today['trip_id'].tolist()

# Passages à Joinville aujourd'hui
passages = stop_times[
    (stop_times['stop_id'].isin(joinville_stop_ids)) &
    (stop_times['trip_id'].isin(trip_ids_today))
].merge(trips_today[['trip_id', 'trip_headsign']], on='trip_id')

# Heures de passage en minutes
def heure_to_minutes(hhmm):
    h, m, *_ = map(int, hhmm.split(":"))
    return h * 60 + m

results_by_direction = {}

for direction, group in passages.groupby("trip_headsign"):
    group = group.copy()
    group['heure_minutes'] = group['departure_time'].str.slice(0, 5).apply(heure_to_minutes)
    # Filtrer trains futurs uniquement
    now_minutes = now.hour * 60 + now.minute
    group = group[group['heure_minutes'] >= now_minutes]
    group = group.sort_values('heure_minutes').head(4)
    trains = []
    for _, row in group.iterrows():
        trip_id = row['trip_id']
        heure = row['departure_time'][:5]
        # Liste stops restants
        stops_seq = stop_times[
            (stop_times['trip_id'] == trip_id) &
            (stop_times['stop_sequence'] >= row['stop_sequence'])
        ].sort_values('stop_sequence')['stop_id'].tolist()
        stop_names = stops[stops['stop_id'].isin(stops_seq)].set_index('stop_id')['stop_name'].to_dict()
        ordered_stop_names = [stop_names.get(sid, sid) for sid in stops_seq]
        trains.append({
            "mission": trip_id.split(":")[-1],
            "heure": heure,
            "destination": direction,
            "gares_restantes": ordered_stop_names
        })
    results_by_direction[direction] = trains

import json
with open("static/rer_a_prochains_trains_by_direction.json", "w", encoding="utf-8") as f:
    json.dump(results_by_direction, f, indent=2, ensure_ascii=False)
print("✅ Exporté static/rer_a_prochains_trains_by_direction.json")
