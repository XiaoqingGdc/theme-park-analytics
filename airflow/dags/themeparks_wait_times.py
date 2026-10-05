import csv
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task

API = "https://api.themeparks.wiki/v1"
# Données stockées dans le dépôt (dossier data/ exclu par .gitignore)
DATA_DIR = Path(os.environ.get(
    "THEMEPARKS_DATA_DIR",
    "/mnt/c/Projets_Data/theme-park-analytics/data/themeparks",
))

# Plage horaire de collecte, en heure LOCALE de chaque parc
OPEN_HOUR = 8     # à partir de 8h00
CLOSE_HOUR = 24   # jusqu'à 23h50

# 8 destinations, 14 parcs
SITES = [
    {
        "destination": "Disneyland Paris", "country": "FR",
        "tz": "Europe/Paris", "lat": 48.8722, "lon": 2.7758,
        "parks": {
            "dae968d5-630d-4719-8b06-3d107e944401": "Disneyland Park",
            "ca888437-ebb4-4d50-aed2-d227f7096968": "Disney Adventure World",
        },
    },
    {
        "destination": "Walt Disney World", "country": "US",
        "tz": "America/New_York", "lat": 28.3852, "lon": -81.5639,
        "parks": {
            "75ea578a-adc8-4116-a54d-dccb60765ef9": "Magic Kingdom",
            "47f90d2c-e191-4239-a466-5892ef59a88b": "EPCOT",
            "288747d1-8b4f-4a64-867e-ea7c9b27bad8": "Disney's Hollywood Studios",
            "1c84a229-8862-4648-9c71-378ddd2c7693": "Disney's Animal Kingdom",
        },
    },
    {
        "destination": "Europa-Park", "country": "DE",
        "tz": "Europe/Berlin", "lat": 48.2660, "lon": 7.7220,
        "parks": {"639738d3-9574-4f60-ab5b-4c392901320b": "Europa-Park"},
    },
    {
        "destination": "Alton Towers", "country": "GB",
        "tz": "Europe/London", "lat": 52.9874, "lon": -1.8869,
        "parks": {"0d8ea921-37b1-4a9a-b8ef-5b45afea847b": "Alton Towers"},
    },
    {
        "destination": "Parc Astérix", "country": "FR",
        "tz": "Europe/Paris", "lat": 49.1340, "lon": 2.5710,
        "parks": {"9e938687-fd99-46f3-986a-1878210378f8": "Parc Astérix"},
    },

    {
        "destination": "LEGOLAND Windsor", "country": "GB",
        "tz": "Europe/London", "lat": 51.4634, "lon": -0.6504,
        "parks": {"a4f71074-e616-4de4-9278-72fdecbdc995": "LEGOLAND Windsor"},
    },
        {
        "destination": "Phantasialand", "country": "DE",
        "tz": "Europe/Berlin", "lat": 50.8005, "lon": 6.8793,
        "parks": {"abb67808-61e3-49ef-996c-1b97ed64fac6": "Phantasialand"},
    },

    {
        "destination": "Universal Orlando", "country": "US",
        "tz": "America/New_York", "lat": 28.4407, "lon": -81.4457,
        "parks": {
            "eb3f4560-2383-4a36-9152-6b3e5ed6bc57": "Universal Studios Florida",
            "267615cc-8943-4c2a-ae2c-5da728ca591f": "Universal Islands of Adventure",
            "12dbb85b-265f-44e6-bccf-f1faa17211fc": "Universal Epic Universe",
        },
    },


]

FIELDS = [
    "collected_at_utc", "collected_at_local", "local_hour",
    "destination", "country", "park_name",
    "attraction_id", "attraction_name", "status", "wait_time_min", "last_updated",
    "temperature_c", "precipitation_mm", "weather_code",
]


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "theme-park-analytics/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


@dag(
    dag_id="themeparks_wait_times",
    schedule="*/10 * * * *",        # toutes les 10 min, 24h/24 (le filtre horaire est par parc)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1, # pas de nouvelle exécution tant que la précédente tourne
    tags=["themeparks"],
)
def themeparks_wait_times():

    @task
    def collect_site(site):
        """Une destination : météo + temps d'attente de tous ses parcs"""
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(ZoneInfo(site["tz"]))

        # En dehors de la plage horaire locale : aucune requête
        if not (OPEN_HOUR <= now_local.hour < CLOSE_HOUR):
            print(f"{site['destination']} : {now_local:%H:%M} heure locale, hors plage, ignoré")
            return []

        raw_dir = DATA_DIR / "raw" / now_utc.strftime("%Y-%m-%d")
        raw_dir.mkdir(parents=True, exist_ok=True)

        # 天气（失败不影响排队数据）
        weather = {"temperature_c": None, "precipitation_mm": None, "weather_code": None}
        try:
            url = (f"https://api.open-meteo.com/v1/forecast?latitude={site['lat']}"
                   f"&longitude={site['lon']}&current=temperature_2m,precipitation,weather_code")
            cur = get_json(url)["current"]
            weather = {
                "temperature_c": cur["temperature_2m"],
                "precipitation_mm": cur["precipitation"],
                "weather_code": cur["weather_code"],
            }
        except Exception as e:
            print(f"Météo indisponible pour {site['destination']} : {e}")

        rows = []
        for park_id, park_name in site["parks"].items():
            try:
                live = get_json(f"{API}/entity/{park_id}/live")
            except Exception as e:
                print(f"Erreur pour {park_name} : {e}")
                continue  # un parc en erreur n'empêche pas les autre

            # Sauvegarde du JSON brut compressé （raw storage）
            raw_file = raw_dir / f"{park_id}_{now_utc.strftime('%H%M')}.json.gz"
            with gzip.open(raw_file, "wt", encoding="utf-8") as f:
                json.dump(live, f)

            for item in live.get("liveData", []):
                if item.get("entityType") != "ATTRACTION":
                    continue
                standby = (item.get("queue") or {}).get("STANDBY") or {}
                rows.append({
                    "collected_at_utc": now_utc.isoformat(timespec="seconds"),
                    "collected_at_local": now_local.strftime("%Y-%m-%d %H:%M:%S"),
                    "local_hour": now_local.hour,
                    "destination": site["destination"],
                    "country": site["country"],
                    "park_name": park_name,
                    "attraction_id": item.get("id"),
                    "attraction_name": item.get("name"),
                    "status": item.get("status"),
                    "wait_time_min": standby.get("waitTime"),
                    "last_updated": item.get("lastUpdated"),
                    **weather,
                })
        print(f"{site['destination']} : {len(rows)} attractions")
        return rows

    @task
    def load(results):
        """Regroupe toutes les destinations et ajoute les lignes au CSV"""
        rows = [r for site_rows in results for r in site_rows]
        if not rows:
            print("Aucune donnée (toutes les destinations hors plage horaire)")
            return
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        path = DATA_DIR / "wait_times.csv"
        new_file = not path.exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerows(rows)
        print(f"{len(rows)} lignes ajoutées à {path}")

    # Mapping dynamique : une tâche parallèle par destination
    load(collect_site.expand(site=SITES))


themeparks_wait_times()