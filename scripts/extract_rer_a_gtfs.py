@@ -2,29 +2,40 @@
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
