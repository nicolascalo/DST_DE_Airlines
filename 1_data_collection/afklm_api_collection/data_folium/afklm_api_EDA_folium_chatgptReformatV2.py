"""
Flight Route Mapping Script
===========================

Author: Nicolas Calo
Description:
------------
This script aggregates and visualizes airline route data between airports worldwide.
It loads airport and flight call parameter data, merges and processes them to compute
daily flight frequencies, and generates an interactive map using Folium.

Features:
---------
- Cleans and merges AFKLM API datasets.
- Calculates daily flight averages per route.
- Summarizes total flight counts per airport.
- Displays interactive Folium map with:
    - Airport markers.
    - Intra- and inter-regional flight routes.
    - Colored lines scaled by daily flight frequency.
- Adds coordinate display and layer control.

Dependencies:
-------------
pandas, folium, seaborn, pyproj, numpy, re, datetime, IPython.display
"""

# === Imports ===
import os
import re
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
import folium
import pyproj
from folium.plugins import (
    MarkerCluster,
    FeatureGroupSubGroup,
    MousePosition
)
from IPython.display import display
import webbrowser

pd.options.mode.chained_assignment = None


# =============================================================================
# 1. WORKING DIRECTORY SETUP
# =============================================================================

if bool(re.search("DST_DE_Airlines$", os.getcwd())):
    os.chdir("1_data_collection/afklm_api_collection")

if bool(re.search("1_data_collection$", os.getcwd())):
    os.chdir("afklm_api_collection")

if bool(re.search("ongoing_work$", os.getcwd())):
    os.chdir("..")

print(f"Current working directory: {os.getcwd()}")

# Define key paths
path_call_parameter_file_folder = "call_parameter_lists"
path_call_parameter_csv_root = "df_call_parameters"
plot_folder = "EDA_plots"


# =============================================================================
# 2. LOAD DATA
# =============================================================================

print("\nLoading airport data...")
df_airports = pd.read_csv("../df_iata_icao_wiki_final_world.csv").fillna("")
#df_airports.info()

print("\nLoading call parameter files...")
path_call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)
call_parameter_csv_list = [
    val for val in path_call_parameter_csv_list if "df_call_parameters" in val
]

df_call_parameters = pd.DataFrame()

for csv_file in call_parameter_csv_list:
    df_add = pd.read_csv(f"{path_call_parameter_file_folder}/{csv_file}").fillna("")
    df_call_parameters = (
        pd.concat([df_call_parameters, df_add], ignore_index=True)
        .fillna("")
        .sort_values(["startRange", "endRange"])
    )

# Filter out invalid routes (origin = destination)
df_call_parameters = df_call_parameters[
    df_call_parameters["origin"] != df_call_parameters["destination"]
]


# =============================================================================
# 3. MERGE AIRPORT INFORMATION
# =============================================================================

print("\nMerging airport metadata...")

df_airports_country_origin = df_airports[
    ["continent", "subcontinent", "country", "iata", "airport", "latitude", "longitude"]
].copy()
df_airports_country_origin.columns = [
    "origin_continent", "origin_subcontinent", "origin_country",
    "origin", "origin_airport", "origin_latitude", "origin_longitude"
]

df_airports_country_destination = df_airports_country_origin.copy()
df_airports_country_destination.columns = [
    "destination_continent", "destination_subcontinent", "destination_country",
    "destination", "destination_airport", "destination_latitude", "destination_longitude"
]

df_call_parameters = (
    df_call_parameters.merge(df_airports_country_origin)
    .merge(df_airports_country_destination)
    .replace("", None)
)

#df_call_parameters.info()


# =============================================================================
# 4. CALCULATE DAILY FLIGHT FREQUENCIES
# =============================================================================

print("\nCalculating daily flight averages...")

df_call_parameters["dailyFlights"] = df_call_parameters.apply(
    lambda row: None
    if row["totalFlights"] is None
    else int(
        row["totalFlights"]
        / (
            datetime.datetime.fromisoformat(row["endRange"])
            - datetime.datetime.fromisoformat(row["startRange"])
        ).days
    ),
    axis=1,
)


# =============================================================================
# 5. SUMMARIZE AIRPORT-LEVEL DATA
# =============================================================================

def make_airport_df(df, prefix):
    """Extract airport-level flight data (origin or destination)."""
    cols = [
        f"{prefix}_continent", f"{prefix}_subcontinent", f"{prefix}_country",
        f"{prefix}_latitude", f"{prefix}_longitude", prefix, f"{prefix}_airport",
        "totalFlights", "totalPages", "dailyFlights"
    ]
    new_cols = [
        "continent", "subcontinent", "country", "latitude", "longitude",
        "iata", "airport", "totalFlights", "totalPages", "dailyFlights"
    ]
    df_out = df[cols].copy()
    df_out.columns = new_cols
    return df_out


print("\nAggregating data by airport...")
df_ori = make_airport_df(df_call_parameters, "origin")
df_dest = make_airport_df(df_call_parameters, "destination")

df_airport = (
    pd.concat([df_ori, df_dest])
    .groupby(["continent", "subcontinent", "country", "latitude", "longitude", "iata", "airport"])
    .sum()
    .reset_index()
    .sort_values(["dailyFlights"], ascending=False)
    .reset_index(drop=True)
)


# =============================================================================
# 6. BUILD AIRPORT DESCRIPTIONS AND CONNECTIONS
# =============================================================================

print("\nBuilding airport connection descriptions...")

df_airport["airport_desc"] = (
    df_airport["country"] + "<br>" + df_airport["iata"] + "<br>" + df_airport["airport"]
)

connection_list = []

for _, record in df_airport.iterrows():
    df_temp = df_call_parameters[df_call_parameters["dailyFlights"] > 0].reset_index(drop=True)
    df_temp = df_temp[
        (df_temp["origin"] == record.iata) | (df_temp["destination"] == record.iata)
    ]

    connection_iata = set(df_temp["origin"]) | set(df_temp["destination"])
    df_filtered = df_airport[
        df_airport["iata"].isin(connection_iata) & (df_airport["iata"] != record.iata)
    ]

    df_filtered["connections"] = (
        df_filtered["continent"] + " - " + df_filtered["subcontinent"] + " - " +
        df_filtered["country"] + " - " + df_filtered["iata"] + " - " + df_filtered["airport"]
    )

    connections = "<br>".join(sorted(df_filtered["connections"].to_list()))
    connection_list.append(connections)

df_airport["airport_desc"] += "<br><br>Connections to:<br>" + pd.Series(connection_list)

# Filter only airports with active flights
valid_airports = set(
    df_call_parameters[df_call_parameters["dailyFlights"] > 0][["origin", "destination"]].to_numpy().flatten()
)
df_airport = df_airport[df_airport["iata"].isin(valid_airports)]


# =============================================================================
# 7. BUILD FOLIUM MAP AND LAYERS
# =============================================================================

print("\nCreating interactive Folium map...")

m = folium.Map(
    crs="EPSG3857",
    location=[50, 15],
    zoom_start=2,
    min_zoom=2,
    max_bounds=True
)

# --- Add airport markers ---
marker_cluster = MarkerCluster(name="Airport", overlay=True, control=False)
m.add_child(marker_cluster)

for _, record in df_airport.iterrows():
    marker = folium.Marker(
        location=[record.latitude, record.longitude],
        popup=folium.Popup(record.airport_desc, max_width=3000),
        icon=folium.Icon(color="white", icon_color="red"),
    )
    marker_cluster.add_child(marker)


# --- Build intra/inter-region layers ---
location_airports_intra, location_airports_inter = {}, {}
intra = MarkerCluster(name="Intra-region", overlay=True)
inter = MarkerCluster(name="Inter-region", overlay=True)

for continent in df_airport["continent"].unique():
    cont_intra = FeatureGroupSubGroup(intra, f"--- Intra {continent}")
    cont_inter = FeatureGroupSubGroup(inter, f"--- Inter {continent}")
    location_airports_intra[f"--- Intra {continent}"] = cont_intra
    location_airports_inter[f"--- Inter {continent}"] = cont_inter

    for subcontinent in df_airport[df_airport["continent"] == continent]["subcontinent"].unique():
        location_airports_intra[f"-------- Intra {subcontinent}"] = FeatureGroupSubGroup(
            cont_intra, f"-------- Intra {subcontinent}"
        )
        location_airports_inter[f"-------- Inter {subcontinent}"] = FeatureGroupSubGroup(
            cont_inter, f"-------- Inter {subcontinent}"
        )


# =============================================================================
# 8. SUMMARIZE ROUTE CONNECTIONS
# =============================================================================

print("\nSummarizing route connections...")

df_sum = df_call_parameters.dropna(subset=["origin", "destination"]).copy()
df_sum["origin_reordered"] = df_sum[["origin", "destination"]].min(axis=1)
df_sum["dest_reordered"] = df_sum[["origin", "destination"]].max(axis=1)

df_sum = (
    df_sum[["origin_reordered", "dest_reordered", "totalFlights", "dailyFlights"]]
    .groupby(["origin_reordered", "dest_reordered"])
    .sum()
    .reset_index()
)
df_sum.columns = ["origin", "destination", "totalFlights", "dailyFlights"]

df_sum = (
    df_sum.merge(df_airports_country_origin)
    .merge(df_airports_country_destination)
    .replace("", None)
)

df_sum["itinerary"] = (
    df_sum.origin_country + " - " + df_sum.origin + " - " + df_sum.origin_airport +
    " <-> " + df_sum.destination_country + " - " + df_sum.destination + " - " +
    df_sum.destination_airport + "<br>totalFlights = " +
    df_sum.totalFlights.astype(str) + "<br>dailyFlights = " +
    df_sum.dailyFlights.astype(str)
)
df_sum = df_sum[df_sum["dailyFlights"] > 0]


# =============================================================================
# 9. COLOR MAPPING AND ROUTE PLOTTING
# =============================================================================

print("\nDrawing great-circle routes...")

max_n = df_sum.dailyFlights.max()
min_n = df_sum.dailyFlights.min()
palette = sns.color_palette("plasma", n_colors=int(np.log2(max_n - min_n + 1))).as_hex()
palette_dict = dict(enumerate(palette, start=int(min_n)))
max_dailyFlights = df_sum["dailyFlights"].max()

g = pyproj.Geod(ellps="WGS84")

for _, row in df_sum.iterrows():
    # Compute great-circle path
    az12, az21, dist = g.inv(
        row.origin_longitude, row.origin_latitude,
        row.destination_longitude, row.destination_latitude
    )

    lonlats = g.npts(
        lon1=row.origin_longitude, lat1=row.origin_latitude,
        lon2=row.destination_longitude, lat2=row.destination_latitude,
        npts=1 + int(dist / 1000)
    )

    # Adjust coordinates if crossing dateline
    lonlats = [
        [lat, lon + 360] if (lon < 0 and abs(row.destination_longitude - row.origin_longitude) > 180)
        else [lat, lon]
        for lon, lat in lonlats
    ]

    opacity = np.log2(row.dailyFlights) / np.log2(max_n)
    color = palette_dict.get(int(np.log2(row.dailyFlights)))

    polyline = folium.PolyLine(
        lonlats,
        color=color,
        opacity=opacity,
        popup=folium.Popup(row.itinerary, max_width=4000),
        weight=2.5,
    )

    if row.origin_subcontinent == row.destination_subcontinent:
        key = f"-------- Intra {row.destination_subcontinent}"
        polyline.add_to(location_airports_intra[key])
    else:
        key = f"-------- Inter {row.destination_subcontinent}"
        polyline.add_to(location_airports_inter[key])


# =============================================================================
# 10. FINAL MAP SETUP AND SAVE
# =============================================================================

print("\nFinalizing map...")

m.add_child(intra)
for grp in location_airports_intra.values():
    m.add_child(grp)

m.add_child(inter)
for grp in location_airports_inter.values():
    m.add_child(grp)

folium.LayerControl(collapsed=False).add_to(m)

# Add coordinate readout
formatter = "function(num) {return L.Util.formatNum(num, 5);};"
mouse_position = MousePosition(
    position="topright",
    separator=" Long: ",
    empty_string="NaN",
    lng_first=False,
    num_digits=20,
    prefix="Lat:",
    lat_formatter=formatter,
    lng_formatter=formatter,
)
m.add_child(mouse_position)

# Save final output
m.save("folium_map.html")
# webbrowser.open("folium_map.html")

print("\n✅ Map successfully saved as 'folium_map.html'")
