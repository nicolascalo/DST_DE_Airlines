from pycirclize import Circos
from pycirclize.parser import Matrix
import pandas as pd
import os
import re


### Setting up working directory

if  bool(re.search("afklm_api_collection",os.getcwd())) == False:
    os.chdir("1_data_collection/afklm_api_collection") 

# Create from-to table dataframe & convert to matrix

os.getcwd()

df_airports = pd.read_csv("../df_iata_icao_wiki_final_eu.csv").fillna('')


df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')



### Airport analysis

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

fromto_table_df['value'] = 360*fromto_table_df['value']/sum_totalFlights

matrix = Matrix.parse_fromto_table(fromto_table_df)




circos_airport = Circos.chord_diagram(
    matrix,
    cmap="plasma",
    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)



circos_airport.savefig("custom_chord_fromto.png")

fig = circos.plotfig()





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

sum_totalFlights =  fromto_table_df['value'].sum()

fromto_table_df['value'] = 360*fromto_table_df['value']/sum_totalFlights

matrix = Matrix.parse_fromto_table(fromto_table_df)




circos_country = Circos.chord_diagram(
    matrix,

    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)



circos_country.savefig("custom_chord_fromto.png")

fig = circos.plotfig()


