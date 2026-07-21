import pandas as pd
import requests
import time


df = pd.DataFrame({"MyPark": ["Disneyland","Parc Astérix","EuropaPark","Alton Towers","Legoland Windsor","Phantasialand","Universal Orlando",]})


def lat_lon_osm(dataframe):
  url = "https://nominatim.openstreetmap.org/search"
  headers = {"User-Agent": "data analyst (myparks@gmail.com)"}

  results = []


  for park in dataframe["MyPark"]:
    clean_park = park.strip()
    params = {
        "q": clean_park,
        "format": "json",
        "limit": "1",
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
      data = response.json()
      if data:
        results.append(
            {"Park": park, "lat": data[0]["lat"], "lon": data[0]["lon"]}
        )
      else:
        results.append({"Park": park, "lat": None, "lon": None})
    else:
      results.append({"Park": park, "lat": None, "lon": None})
  
    time.sleep(1)

  df_results = pd.DataFrame(results)
  return df_results

df_final = lat_lon_osm(df)
print("Résultat final :")
print(df_final)
