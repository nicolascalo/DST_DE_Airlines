import json
from pymongo import MongoClient

#Connexion à MongoDB
#client = MongoClient("mongodb://localhost:27017/")
client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)
db = client["airfrance_db"]
collection = db["operationalFlights"]

#Chargement du fichier JSON Air France
with open("airfrance.json", "r", encoding="utf-8") as f:
    data = json.load(f)

#Vérifier structure
if "operationalFlights" in data:
    docs = data["operationalFlights"]
elif isinstance(data, list):
    docs = data
else:
    raise ValueError("Clé 'operationalFlights' non trouvée dans le JSON")

#Insertion MongoDB
collection.insert_many(docs)

print("Import terminé : données disponibles dans MongoDB > airfrance_db.operationalFlight")

