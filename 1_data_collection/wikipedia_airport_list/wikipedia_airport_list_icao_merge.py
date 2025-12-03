import pandas as pd


df_wikipedia_airport_list = pd.read_csv("./wikipedia_airport_list.csv")
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

df_wikipedia_airport_list_clean_short = df_wikipedia_airport_list_clean[['continent','subcontinent','country','iata']].drop_duplicates().dropna(subset='iata')

df_wikipedia_airport_list_clean_short.info()

df_wikipedia_airport_list_icao_merge = df_wikipedia_airport_list_clean_short.merge(df_iataicao_clean)

df_wikipedia_airport_list_icao_merge.info()

df_wikipedia_airport_list_icao_merge.to_csv("wikipedia_airport_list_icao_merge.csv",index=0)

