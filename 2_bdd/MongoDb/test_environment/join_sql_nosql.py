import pandas as pd
import psycopg2
from pymongo import MongoClient
from pprint import pprint

#Connexion PostgreSQL
conn = psycopg2.connect(database="fly_project",
                        host="localhost",
                        user="daniel",
                        password="datascientest",
                        port="5432")
cur = conn.cursor()

#Charger la table airport en DataFrame
airports_df = pd.read_sql_query("SELECT * FROM airport;", conn)

#Connexion MongoDB
client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)
db = client["airfrance_db"]
collection = db["operationalFlights"]

#Récupérer les vols et les convertir en DataFrame
flights = list(collection.find())
flights_df = pd.DataFrame(flights)

#Extraire code IATA départ
flights_df['departure_iata'] = flights_df['flightLegs'].apply(
    lambda x: x[0]['departureInformation']['airport']['code'] if isinstance(x, list) else None
)

#Jointure : Vol + Infos aéroport
merged_df = flights_df.merge(
    airports_df,
    left_on="departure_iata",
    right_on="iata_code",
    how="inner"
)

#Afficher les infos utiles
print(merged_df[['flightNumber', 'departure_iata', 'airport_name', 'city', 'country']].head(100).to_string(index=False))

#Terminer la connexion
conn.close()
client.close()
