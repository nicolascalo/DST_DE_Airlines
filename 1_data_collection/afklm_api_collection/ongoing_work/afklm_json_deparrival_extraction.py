import pandas as pd
import json
import os


os.chdir("1_data_collection/afklm_api_collection") 

### Setup 

df_flights_all = pd.DataFrame() # Initialize empty dataframe to store final values

### json file import

json_list = os.listdir("data") # Listing of json files to process


for json_file_name in json_list:


    with open("data/" + json_file_name) as json_file:
        json_file = json.load(json_file)
        
    print(json_file_name)
    df = pd.json_normalize(json_file)
    df_flights = pd.json_normalize(df['operationalFlights'][0])     
    flightLeg = pd.json_normalize(df_flights['flightLegs'])[0]
    iata_departure = pd.json_normalize(flightLeg)['departureInformation.airport.code']
    df_flights['iata_departure'] = iata_departure
    iata_arrival = pd.json_normalize(flightLeg)['arrivalInformation.airport.code']
    df_flights['iata_arrival'] = iata_arrival
        

    
    df_flights_all = pd.concat([df_flights_all, df_flights],ignore_index=True)
        

iata_departure_arrival = df_flights_all[['iata_departure','iata_arrival']].drop_duplicates()



### import EU airport list from wikipedia files

eu_airports = pd.read_csv("../wikipedia_airport_list/airport_list.csv")eu_airports = eu_airports[eu_airports['continent'] == 'Europe']
eu_airports_iata_codes = eu_airports['IATA Code'].values # extracting values from df
eu_airports_iata_codes.sort()

eu_airports_iata_codes = [i.split("/") for i in eu_airports_iata_codes] # splitting combined codes
eu_airports_iata_codes = [item for sublist in eu_airports_iata_codes for item in sublist] # flattening the list



### Filtering departure/arrival for EU only

iata_departure_arrival_eu = iata_departure_arrival[iata_departure_arrival['iata_departure'].isin(eu_airports_iata_codes)]

iata_departure_arrival_eu = iata_departure_arrival_eu[iata_departure_arrival_eu['iata_arrival'].isin(eu_airports_iata_codes)]

iata_departure_arrival_eu.to_csv("afklm_iata_departure_arrival_eu.csv",index=0)