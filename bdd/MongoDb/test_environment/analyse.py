import pandas as pd
import numpy as np
from pprint import pprint

# Chargement du csv dans un dataframe
df = pd.read_csv("afklm_flight_from_mongo_filtered.csv", low_memory=False)

# Remplacer les crochets vides par des valeurs manquantes réelles
df = df.replace('[]', np.nan)

# Aperçu des colonnes
#print(df.head())
#print(df.columns.tolist())
for col in df.columns:
    print(col)
print("Nombre de colonnes: ", len(df.columns))

# Statistiques: valeurs manquantes par colonne
missing = df.isnull().mean() * 100
print(missing.sort_values(ascending=False))

# Export du fichier de stats en csv
missing = missing.round(2)
missing.sort_values(ascending=False).to_csv("valeurs_manquantes.csv", header=['Champs, Taux_valeurs_manquantes'])

# Valeurs extrêmes (rares)
pprint(df['flightStatusPublic'].value_counts(normalize=True))
