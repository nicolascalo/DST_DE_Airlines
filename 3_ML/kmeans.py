import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

# 1. Chargement des données
col_delay = "flightLegs_irregularity_delayDuration_total"
col_arr = "flightLegs_arrivalInformation_airport_code"
col_dep = "flightLegs_departureInformation_airport_code"

df = pd.read_csv("csv_exploration.csv", usecols=[col_delay, col_arr, col_dep]) # indiquer le nom du fichier csv

top30 = ['CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO']

# Préfiltrage aéroport d'arrivée Europe - aéroport de départ Europe
df = df[df[col_arr].isin(top30) & df[col_dep].isin(top30)].copy()

# Extraction des retards
data = df[col_delay].dropna()

# 2. Préparation des données pour KMeans
data_reshaped = data.values.reshape(-1, 1)

# 3. Calcule K-Means pour créer 4 clusters
k = 4
kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
kmeans.fit(data_reshaped)

# 4. Ajout de la colonne cluster au DF filtré
df.loc[data.index, "delay_cluster"] = kmeans.labels_

# 5. Centres naturels des clusters
centers = sorted(kmeans.cluster_centers_.flatten())
print("\n=== Centres naturels des clusters (moyennes) ===")
for i, c in enumerate(centers):
    print(f"Cluster {i+1} ≈ {c:.1f} minutes")

# 6. Calcul des bornes réelles par cluster
print("\n=== Bornes observées (min, max) ===")

cluster_bounds = {}

for cl in range(k):
    # valeurs du cluster réel (pas triées par centre)
    vals = data[df["delay_cluster"] == cl]

    if len(vals) == 0:
        continue

    cmin = vals.min()
    cmax = vals.max()
    cluster_bounds[cl] = (cmin, cmax)

sorted_bounds = sorted(
    [(cl, low_high[0], low_high[1]) for cl, low_high in cluster_bounds.items()],
    key=lambda x: x[1] # tri par borne minimale
)

for rank, (cl, low, high) in enumerate(sorted_bounds, start=1):
    print(f"Tranche {rank} : [{low:.0f}, {high:.0f}] minutes")

# 7. Population des clusters (triée selon les bornes)
print("\n=== Population des clusters  ===")

for rank, (cl, low, high) in enumerate(sorted_bounds, start=1):
    cluster_size = len(df[df["delay_cluster"] == cl])
    print(f"Population {rank} : {cluster_size} vols")
