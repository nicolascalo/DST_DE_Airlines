import pandas as pd
import numpy as np
from pprint import pprint

# Chargement du csv dans un dataframe
df = pd.read_csv("csv_exploration.csv", low_memory=False)

# Remplacer les crochets vides par des valeurs manquantes réelles
df = df.replace('[]', np.nan)

# Suppression des colonnes inutiles
cols_to_drop = ['_id', 'id', 'airline_code', 'airline_name']
df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

# Colonnes liées aux irrégularités
# (ajuste ici les noms selon ton CSV si nécessaire)
irregularity_cols = [
    'flightLegs_irregularity_delayDuration'
]
irregularity_cols = [col for col in irregularity_cols if col in df.columns] # sécurité

# Création de la colonne indiquant la présence d'une irrégularité 
df['has_irregularity'] = df[irregularity_cols].notna().any(axis=1)

# Liste des colonnes à analyser 
category_cols = [
    'flightStatusPublic',
    'flightLegs_status',
    'flightLegs_serviceType',
    'flightLegs_aircraft_ownerAirlineCode',
    'flightLegs_aircraft_typeCode'
]
category_cols = [col for col in category_cols if col in df.columns] # vérification des colonnes existantes

# Fonction d’analyse
def analyse_irregularities_by(col_name):
    summary = (
        df.groupby(col_name)['has_irregularity']
        .agg(['count', 'sum'])
        .rename(columns={'count': 'Total vols', 'sum': 'Vols avec irrégularité'})
        .reset_index()
    )
    summary["% d'irrégularités"] = (
        summary["Vols avec irrégularité"] / summary["Total vols"] * 100
    ).round(2)
# Affichage du top 10 pour le champ ownerAirLineCode
    if col_name in ['flightLegs_aircraft_ownerAirlineCode','flightLegs_aircraft_typeCode']:
        summary = summary.sort_values(by="% d'irrégularités", ascending=False).head(10)
    print(f"\n=== Synthèse des irrégularités par {col_name} ===")
    print(summary)
    print("\n")

# === Lancement des analyses ===
for col in category_cols:
    analyse_irregularities_by(col)

