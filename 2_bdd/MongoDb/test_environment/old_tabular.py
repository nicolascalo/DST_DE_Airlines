import json
from pymongo import MongoClient

# chargement local du JSON
with open("airfrance.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# la clé exacte renvoyée par l'API Air France KLM
flights = data["operationalFlights"]

# connexion MongoDB
client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)
db = client["airfrance_db"]
collection = db["cleanFlights"] # sert à stocker les données filtrées et pertinentes
collection.delete_many({}) # on vide la collection avant de recharger des données pour éviter les doublons

for doc in flights:
    leg = doc.get("flightLegs", [{}])[0] # premier segment de vol

    clean_doc = {
        # Identification
        "flightNumber": doc.get("flightNumber"),
        "flightStatusPublic": doc.get("flightStatusPublic"),

        # Aircraft
        "ownerAirlineCode": leg.get("aircraft", {}).get("ownerAirlineCode"),
        "typeCode": leg.get("aircraft", {}).get("typeCode"),
        "serviceType": leg.get("serviceType"),

        # Departure
        "dep_airportCode": leg.get("departureInformation", {}).get("airport", {}).get("code"),
        "dep_scheduled": leg.get("departureInformation", {}).get("times", {}).get("scheduled"),

        # Arrival
        "arr_airportCode": leg.get("arrivalInformation", {}).get("airport", {}).get("code"),
        "arr_scheduled": leg.get("arrivalInformation", {}).get("times", {}).get("scheduled"),

        # Status / Irregularity
        "status": leg.get("status"),
        "irregularity_code": leg.get("irregularity", {}).get("code"),
        "irregularity_description": leg.get("irregularity", {}).get("description")
    }

    # Insérer dans MongoDB
    collection.insert_one(clean_doc)

print(" Insertion terminée : champs pertinents enregistrés dans MongoDB ('cleanFlights')")
