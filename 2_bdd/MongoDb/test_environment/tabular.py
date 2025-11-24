import pandas as pd
import psycopg2
from pymongo import MongoClient
from pprint import pprint

#Connexion MongoDB
client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)
db = client["airfrance_db"]
collection = db["operationalFlights"]

clean_data = []

for doc in collection.find():
    # Niveau racine : OperationalFlight
    flightNumber = doc.get("flightNumber")
    flightStatusPublic = doc.get("flightStatusPublic")

    # Niveau 1 : FlightLegs[0]
    leg = doc.get("flightLegs", [{}])[0] # On prend le 1er segment de vol

    # Niveau 2 : Aircraft
    aircraft = leg.get("aircraft", {})
    ownerAirlineCode = aircraft.get("ownerAirlineCode")
    typeCode = aircraft.get("typeCode")

    # Niveau 2 : Service & Statut
    serviceType = leg.get("serviceType")
    status = leg.get("status")

    # Niveau 2 : Irregularity
    irregularity = leg.get("irregularity", {})
    irregularity_code = irregularity.get("code")
    irregularity_description = irregularity.get("description")

    # Niveau 2 : Departure Information
    departure = leg.get("departureInformation", {})
    dep_airport = departure.get("airport", {})
    dep_times = departure.get("times", {})

    dep_airportCode = dep_airport.get("code")
    dep_city = dep_airport.get("place", {}).get("city")
    dep_country = dep_airport.get("place", {}).get("country")
    dep_lat = dep_airport.get("location", {}).get("latitude")
    dep_lon = dep_airport.get("location", {}).get("longitude")

    dep_scheduled = dep_times.get("scheduled")
    dep_estimated = dep_times.get("estimated", {}).get("latestPublished")
    dep_actual = dep_times.get("actual")

    # Niveau 2 : Arrival Information
    arrival = leg.get("arrivalInformation", {})
    arr_airport = arrival.get("airport", {})
    arr_times = arrival.get("times", {})

    arr_airportCode = arr_airport.get("code")
    arr_city = arr_airport.get("place", {}).get("city")
    arr_country = arr_airport.get("place", {}).get("country")
    arr_terminal = arr_airport.get("place", {}).get("arrivalPositionTerminal")
    arr_lat = arr_airport.get("location", {}).get("latitude")
    arr_lon = arr_airport.get("location", {}).get("longitude")

    arr_scheduled = arr_times.get("scheduled")
    arr_estimated = arr_times.get("estimated", {}).get("value")
    arr_actual = arr_times.get("actual")

    # Ajout à la liste clean_data (format ligne tabulaire)
    clean_data.append({
        "flightNumber": flightNumber,
        "flightStatusPublic": flightStatusPublic,
        "ownerAirlineCode": ownerAirlineCode,
        "aircraft_typeCode": typeCode,
        "serviceType": serviceType,
        "status": status,
        "irregularity_code": irregularity_code,
        "irregularity_description": irregularity_description,
        "dep_airportCode": dep_airportCode,
        "dep_city": dep_city,
        "dep_country": dep_country,
        "dep_latitude": dep_lat,
        "dep_longitude": dep_lon,
        "dep_scheduled": dep_scheduled,
        "dep_estimated": dep_estimated,
        "dep_actual": dep_actual,
        "arr_airportCode": arr_airportCode,
        "arr_city": arr_city,
        "arr_country": arr_country,
        "arr_terminal": arr_terminal,
        "arr_latitude": arr_lat,
        "arr_longitude": arr_lon,
        "arr_scheduled": arr_scheduled,
        "arr_estimated": arr_estimated,
        "arr_actual": arr_actual
    })

# Conversion en DataFrame
df = pd.DataFrame(clean_data)
print("Extraction terminée. Aperçu :")
print(df.head(10).to_string(index=False))
