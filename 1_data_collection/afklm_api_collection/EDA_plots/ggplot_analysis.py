import pandas as pd
from plotnine import *
import os
import re

df_airports = pd.read_csv("../df_iata_icao_wiki_final_eu.csv").fillna('')


df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')



if  bool(re.search("afklm_api_collection",os.getcwd())) == False:
    os.chdir("1_data_collection/afklm_api_collection") 

if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


### Airport analysis


df_airports = pd.read_csv("../df_iata_icao_wiki_final_eu.csv").fillna('')
df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')







### Separated A/R



df_airports_country_origin = df_airports[['country','iata']] 
df_airports_country_origin.columns = ["origin_country", "origin"]
df_airports_country_destination = df_airports[['country','iata']] 
df_airports_country_destination.columns = ["destination_country", "destination"]

df_call_parameters = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_origin).query('totalFlights != ""')
df_call_parameters['totalFlights'] = df_call_parameters['totalFlights'].astype('int')


p = (ggplot(df_call_parameters,aes("origin", "destination", fill="totalFlights")) + geom_tile())


p.save("my_plot.png")

p.show()






### Combined A/R

fromto_table_df = df_call_parameters[['origin','destination','totalFlights']].query('totalFlights != ""')
fromto_table_df.columns = ["from", "to", "value"]


resorted_from = []
resorted_to = []

i = 0

for i in range(0,len(fromto_table_df)) :
    
    old_from = fromto_table_df['from'].values[i]
    old_to = fromto_table_df['to'].values[i]
    
    if old_from > old_to:
        resorted_from.append(old_from)
        resorted_to.append(old_to)
    else:
        resorted_from.append(old_to)
        resorted_to.append(old_from)
        
    
fromto_table_df['from'] = resorted_from
fromto_table_df['to'] = resorted_to

fromto_table_df = fromto_table_df.groupby(['from','to']).sum().reset_index()

sum_totalFlights =  fromto_table_df['value'].sum()


p = (ggplot(fromto_table_df,aes("from", "to", fill="value")) + geom_tile())


p.show()


p.save("my_plot.png")







### Country analysis

df_airports_country_origin = df_airports[['country','iata']] 
df_airports_country_origin.columns = ["from", "origin"]



df_airports_country_destination = df_airports[['country','iata']] 
df_airports_country_destination.columns = ["to", "destination"]


 
df_call_parameters_country = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination)

fromto_table_df = df_call_parameters_country[['from','to','totalFlights']].query('totalFlights != ""')

fromto_table_df.columns = ["from", "to", "value"]


resorted_from = []
resorted_to = []

i = 0

for i in range(0,len(fromto_table_df)) :
    
    old_from = fromto_table_df['from'].values[i]
    old_to = fromto_table_df['to'].values[i]
    
    if old_from > old_to:
        resorted_from.append(old_from)
        resorted_to.append(old_to)
    else:
        resorted_from.append(old_to)
        resorted_to.append(old_from)
        
    
fromto_table_df['from'] = resorted_from
fromto_table_df['to'] = resorted_to

fromto_table_df = fromto_table_df.groupby(['from','to']).sum().reset_index()


p = (ggplot(fromto_table_df,aes("from", "to", fill="value")) + geom_tile())


p.show()


p.save("my_plot.png")


