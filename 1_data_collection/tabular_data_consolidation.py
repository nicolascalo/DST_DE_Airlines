import pandas as pd
import os

df_wikipedia_airport_list = pd.read_csv("./wikipedia_airport_list/airport_list.csv")
df_edigla = pd.read_csv("./edi_gla_flight_routes/edi_gla_flight_routes_20250401to20251013.csv")
df_iataicao = pd.read_csv("https://raw.githubusercontent.com/ip2location/ip2location-iata-icao/refs/heads/master/iata-icao.csv")


def clean_column_names(df):
    df.columns = (df.columns
               .str.strip()
               .str.lower()
               .str.replace(" ", "_")
               .str.replace("[()€$]", "",

                            regex=True))
    return df




df_wikipedia_airport_list_clean = clean_column_names(df_wikipedia_airport_list)
df_edigla_clean = clean_column_names(df_edigla)
df_iataicao_clean = clean_column_names(df_iataicao)

df_wikipedia_airport_list_clean = df_wikipedia_airport_list_clean.rename(columns={"iata_code": "iata", "icao_code": "icao"})


df_edigla_clean.info()
df_iataicao_clean.info()
df_wikipedia_airport_list_clean.info()



df_wikipedia_airport_list_clean_short = df_wikipedia_airport_list_clean[['continent','subcontinent','country','iata']].drop_duplicates()

df_wikipedia_airport_list_clean_short.info()

'''

df_int_airports= df_wikipedia_airport_list_clean.merge(df_iataicao_clean, left_on='iata_code',right_on='iata')

df_int_airports.to_csv("df_int_airports_iataicao.csv")

'''


df_edigla_clean_count = df_edigla_clean[['departure_icao','destination_icao','callsign']].groupby(['departure_icao','destination_icao']).count().reset_index()



df_iataicao_clean_departure = df_wikipedia_airport_list_clean_short.merge(df_iataicao_clean).copy(deep=True)


df_iataicao_clean_departure.columns = "departure_" + df_iataicao_clean_departure.columns
df_iataicao_clean_departure.info()

df_iataicao_clean_destination = df_wikipedia_airport_list_clean_short.merge(df_iataicao_clean).copy(deep=True)
df_iataicao_clean_destination.columns = "destination_" + df_iataicao_clean_destination.columns
df_iataicao_clean_destination.info()



df_edigla_clean_count_iata = df_edigla_clean_count.merge(df_iataicao_clean_departure).merge(df_iataicao_clean_destination)
df_edigla_clean_count_iata.info()



df_edigla_clean_count_iata.to_csv("df_edigla_clean_count_iata_icao_wiki")

df_edigla_clean_count_iata_eu = df_edigla_clean_count_iata[(df_edigla_clean_count_iata['departure_continent'] == 'Europe' )&( df_edigla_clean_count_iata['destination_continent'] == 'Europe')]


df_edigla_clean_count_iata_eu.to_csv("df_edigla_clean_count_iata_icao_wiki_eu")