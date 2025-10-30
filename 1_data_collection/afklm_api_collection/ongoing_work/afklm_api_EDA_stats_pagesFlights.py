import pandas as pd
import os
import re
import numpy as np



### Setting up working directory

if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


if  bool(re.search("1_data_collection$",os.getcwd())) == True:
    os.chdir("afklm_api_collection") 


path_call_parameter_file_folder = "call_parameter_lists"
path_call_parameter_csv_root = "df_call_parameters"
remove_loop_from_to = True
plot_folder = 'EDA_plots'



df_airports = pd.read_csv("../df_iata_icao_wiki_final_world.csv").fillna('')


path_call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)

call_parameter_csv_list = [val for val in path_call_parameter_csv_list if 'df_call_parameters'  in val]

df_call_parameters = pd.DataFrame()



for call_parameter_csv in call_parameter_csv_list :
    
    df_call_parameters_to_add = pd.read_csv(path_call_parameter_file_folder +"/"+call_parameter_csv).fillna('')

    df_call_parameters = pd.concat([df_call_parameters, df_call_parameters_to_add],ignore_index=True).fillna('').sort_values(['startRange','endRange'])





df_call_parameters = df_call_parameters[df_call_parameters['origin'] != df_call_parameters['destination']]    





df_airports_country_origin = df_airports[['continent','subcontinent','country','iata']] 
df_airports_country_origin.columns = ['origin_continent','origin_subcontinent',"origin_country", "origin"]
df_airports_country_destination = df_airports[['continent','subcontinent','country','iata']] 
df_airports_country_destination.columns = ['destination_continent','destination_subcontinent',"destination_country", "destination"]


df_call_parameters = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination).replace('',None)
    
    


df_call_parameters_20250515to20251014 = df_call_parameters[(df_call_parameters['startRange'] == '2025-05-15T09:00:00Z') & (df_call_parameters['endRange'] == '2025-10-14T23:59:59Z')]    



df_call_parameters_20250515to20251014_continent = df_call_parameters_20250515to20251014[['destination_continent','origin_continent','totalFlights']].groupby(['destination_continent','origin_continent']).sum().reset_index()


pd.pivot_table(df_call_parameters_20250515to20251014_continent, values='totalFlights',   index=['origin_continent'], columns=['destination_continent'], aggfunc="sum")


df_call_parameters_20250515to20251014.info()


df_call_parameters_20250515to20251014_aiport_ori = df_call_parameters_20250515to20251014[['origin_continent','origin','totalFlights',"totalPages"]]
df_call_parameters_20250515to20251014_aiport_ori.columns = ['continent',"airport", "totalFlights","totalPages"]

df_call_parameters_20250515to20251014_aiport_dest = df_call_parameters_20250515to20251014[['destination_continent','destination','totalFlights',"totalPages"]]
df_call_parameters_20250515to20251014_aiport_dest.columns = ['continent',"airport", "totalFlights","totalPages"]

df_call_parameters_20250515to20251014_aiport = pd.concat([df_call_parameters_20250515to20251014_aiport_ori,df_call_parameters_20250515to20251014_aiport_dest]).groupby(['continent',"airport"]).sum().reset_index()




df_call_parameters_20250515to20251014_aiport = df_call_parameters_20250515to20251014_aiport.sort_values(['totalFlights'],ascending=False).reset_index().drop(['index'], axis=1)

df_call_parameters_20250515to20251014_aiport['page_per_day'] = df_call_parameters_20250515to20251014_aiport['totalPages'] / (5*30)


df_call_parameters_20250515to20251014_aiport['page_per_day_cum_sum'] = df_call_parameters_20250515to20251014_aiport['page_per_day'].cumsum() 


df_call_parameters_20250515to20251014_aiport.to_csv('df_call_parameters_20250515to20251014_aiport_world_cumsumPages.csv', index=0)

df_call_parameters_20250515to20251014_aiport_eu = df_call_parameters_20250515to20251014_aiport[df_call_parameters_20250515to20251014_aiport['continent'] == 'Europe']

df_call_parameters_20250515to20251014_aiport_eu['page_per_day_cum_sum'] = df_call_parameters_20250515to20251014_aiport_eu['page_per_day'].cumsum()

df_call_parameters_20250515to20251014_aiport_eu.to_csv('df_call_parameters_20250515to20251014_aiport_eu_cumsumPages.csv', index=0)