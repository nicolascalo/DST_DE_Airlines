from pycirclize import Circos
from pycirclize.parser import Matrix
import pandas as pd
import os
import re
import numpy as np


### Setting up working directory

if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


if  bool(re.search("1_data_collection$",os.getcwd())) == True:
    os.chdir("afklm_api_collection") 



# Create from-to table dataframe & convert to matrix


df_airports = pd.read_csv("../df_iata_icao_wiki_final_eu.csv").fillna('')


df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')



### Airport analysis

fromto_table_df = df_call_parameters[['origin','destination','totalFlights']].query('totalFlights != ""')

fromto_table_df.columns = ["from", "to", "value"]

fromto_table_df = fromto_table_df[fromto_table_df['from'] != fromto_table_df['to']]


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

plot = circos_airport.plotfig()
plot.show()


circos_airport.savefig("circos_airport.png")





### Country analysis

df_airports_country_origin = df_airports[['country','iata']] 
df_airports_country_origin.columns = ["from", "origin"]



df_airports_country_destination = df_airports[['country','iata']] 
df_airports_country_destination.columns = ["to", "destination"]


 
df_call_parameters_country = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination)




df_call_parameters_country = df_call_parameters_country[df_call_parameters_country['destination'] != df_call_parameters_country['origin']]



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
    # order = 'desc',
    cmap='rainbow',
    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)



circos_country.savefig("circos_country.png")

plot = circos_country.plotfig()
plot.show()

### Country analysis Condensed


country_list_from = list(fromto_table_df['from'].values)
country_list_to = list(fromto_table_df['to'].values)

country_set = set(country_list_to+country_list_from)


country_list = []
country_prop = []

for country in country_set:
    degrees = circos_country.get_group_sectors_deg_lim([country])
    prop = ((degrees[1]-degrees[0])/360)
    country_list.append(country)
    country_prop.append(prop)
    
    
country_prop = np.array(country_prop)
country_list = np.array(country_list)
    
small_country_treshold = 0.015
    
small_countries = country_list[country_prop < small_country_treshold ]
big_countries =  country_list[country_prop >= small_country_treshold ]
      
      
      
      
df_airports_country_origin = df_airports[['country','iata']] 
df_airports_country_origin.columns = ["from", "origin"]



df_airports_country_destination = df_airports[['country','iata']] 
df_airports_country_destination.columns = ["to", "destination"]


 
df_call_parameters_country = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination)




df_call_parameters_country = df_call_parameters_country[df_call_parameters_country['destination'] != df_call_parameters_country['origin']]



fromto_table_df = df_call_parameters_country[['from','to','totalFlights']].query('totalFlights != ""')

fromto_table_df.columns = ["from", "to", "value"]


resorted_from = []
resorted_to = []

i = 0

for i in range(0,len(fromto_table_df)) :
    
    old_from = fromto_table_df['from'].values[i]
    old_to = fromto_table_df['to'].values[i]
    
    if old_to in small_countries:
        old_to = 'Other'
        
    if old_from in small_countries:
        old_from = 'Other'
        
    
    
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
    # order = 'desc',
    cmap='rainbow',
    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)




plot = circos_country.plotfig()
plot.show()

circos_country.savefig("circos_country_condensed.png")
      
      
### Country analysis Condensed no other


      
country_list_from = list(fromto_table_df['from'].values)
country_list_to = list(fromto_table_df['to'].values)

country_set = set(country_list_to+country_list_from)


country_list = []
country_prop = []

for country in country_set:
    degrees = circos_country.get_group_sectors_deg_lim([country])
    prop = ((degrees[1]-degrees[0])/360)
    country_list.append(country)
    country_prop.append(prop)
    
    
country_prop = np.array(country_prop)
country_list = np.array(country_list)
    
small_country_treshold = 0.015
    
small_countries = country_list[country_prop < small_country_treshold ]
big_countries =  country_list[country_prop >= small_country_treshold ]
      
      
      
small_countries = country_list[country_prop < small_country_treshold ]
      
      
      
      
df_airports_country_origin = df_airports[['country','iata']] 
df_airports_country_origin.columns = ["from", "origin"]



df_airports_country_destination = df_airports[['country','iata']] 
df_airports_country_destination.columns = ["to", "destination"]


 
df_call_parameters_country = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination)




df_call_parameters_country = df_call_parameters_country[df_call_parameters_country['destination'] != df_call_parameters_country['origin']]



fromto_table_df = df_call_parameters_country[['from','to','totalFlights']].query('totalFlights != ""')

fromto_table_df.columns = ["from", "to", "value"]


resorted_from = []
resorted_to = []

i = 0

for i in range(0,len(fromto_table_df)) :
    
    old_from = fromto_table_df['from'].values[i]
    old_to = fromto_table_df['to'].values[i]
    
    if old_to in small_countries:
        old_to = 'Other'
        
    if old_from in small_countries:
        old_from = 'Other'
        
    
    
    if old_from > old_to:
        resorted_from.append(old_from)
        resorted_to.append(old_to)
    else:
        resorted_from.append(old_to)
        resorted_to.append(old_from)
    
    
        
    
fromto_table_df['from'] = resorted_from
fromto_table_df['to'] = resorted_to

fromto_table_df = fromto_table_df[(fromto_table_df['from'] != 'Other' ) & (fromto_table_df['to'] != 'Other')]

fromto_table_df = fromto_table_df.groupby(['from','to']).sum().reset_index()

sum_totalFlights =  fromto_table_df['value'].sum()

fromto_table_df['value'] = 360*fromto_table_df['value']/sum_totalFlights

matrix = Matrix.parse_fromto_table(fromto_table_df)




circos_country = Circos.chord_diagram(
    matrix,
    # order = 'desc',
    cmap='rainbow',
    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)


plot = circos_country.plotfig()
plot.show()

      

circos_country.savefig("circos_country_nosmall.png")

      

### Country analysis local

fromto_table_df_local = fromto_table_df[(fromto_table_df['from'] == fromto_table_df['to'] )]
sum_totalFlights_local =  fromto_table_df['value'].sum()

fromto_table_df_local['value'] = 360*fromto_table_df['value']/sum_totalFlights_local

matrix = Matrix.parse_fromto_table(fromto_table_df_local)

circos_country_local = Circos.chord_diagram(
    matrix,
    cmap = 'rainbow',

    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)



plot = circos_country_local.plotfig()
plot.show()


circos_country_local.savefig("circos_country_local.png")



### Country analysis international


fromto_table_df_int = fromto_table_df[(fromto_table_df['from'] != fromto_table_df['to'] )]


sum_totalFlights_int =  fromto_table_df['value'].sum()

fromto_table_df_int['value'] = 360*fromto_table_df['value']/sum_totalFlights_int

matrix = Matrix.parse_fromto_table(fromto_table_df_int)

circos_country_int = Circos.chord_diagram(
    matrix,
    cmap = 'rainbow',
    #order = 'desc',
    label_kws = dict(r=110, orientation="vertical"),
    space = 1

)


plot = circos_country_int.plotfig()
plot.show()



circos_country_int.savefig("circos_country_int.png")


