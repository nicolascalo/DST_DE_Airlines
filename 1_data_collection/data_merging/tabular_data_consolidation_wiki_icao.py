import pandas as pd
import os




### Setting up working directory



df_wikipedia_airport_list = pd.read_csv("./wikipedia_airport_list/airport_list.csv")
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

df_iataicao_clean = clean_column_names(df_iataicao)

df_wikipedia_airport_list_clean = df_wikipedia_airport_list_clean.rename(columns={"iata_code": "iata", "icao_code": "icao"})



df_iataicao_clean.info()
df_wikipedia_airport_list_clean.info()



df_wikipedia_airport_list_clean_short = df_wikipedia_airport_list_clean[['continent','subcontinent','country','iata']].drop_duplicates()

df_wikipedia_airport_list_clean_short.info()

'''

df_int_airports= df_wikipedia_airport_list_clean.merge(df_iataicao_clean, left_on='iata_code',right_on='iata')

df_int_airports.to_csv("df_int_airports_iataicao.csv")

'''


df_wikipedia_airport_list_clean_short.merge(df_iataicao_clean).to_csv("df_iata_icao_wiki_final_world.csv",index=0)
df_wikipedia_airport_list_clean_short.merge(df_iataicao_clean).query('continent == "Europe"').to_csv("df_iata_icao_wiki_final_eu.csv",index=0)

