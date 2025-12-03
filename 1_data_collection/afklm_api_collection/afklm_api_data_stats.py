import pandas as pd
import os
import re
import datetime


### Setting up working directory

if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


if  bool(re.search("1_data_collection$",os.getcwd())) == True:
    os.chdir("afklm_api_collection") 


path_call_parameter_file_folder = "call_parameter_lists"
path_call_parameter_file_folder_others = "call_parameter_lists/not_in_use"
path_call_parameter_csv_root = "df_call_parameters"
remove_loop_from_to = True
plot_folder = 'EDA_plots'



df_airports = pd.read_csv("../wikipedia_airport_list/wikipedia_airport_list_icao_merge.csv").fillna('')


call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)
call_parameter_csv_list_other = os.listdir(path_call_parameter_file_folder_others)


call_parameter_csv_list = [path_call_parameter_file_folder +"/"+val for val in call_parameter_csv_list if 'df_call_parameters'  in val]
call_parameter_csv_list_other = [path_call_parameter_file_folder_others +"/"+val for val in call_parameter_csv_list_other if 'df_call_parameters'  in val]


call_parameter_csv_list = call_parameter_csv_list + call_parameter_csv_list_other


df_call_parameters = pd.DataFrame()



for call_parameter_csv in call_parameter_csv_list :
    
    df_call_parametersto_add = pd.read_csv(call_parameter_csv).fillna('')

    df_call_parameters = pd.concat([df_call_parameters, df_call_parametersto_add],ignore_index=True).fillna('').sort_values(['startRange','endRange'])


df_call_parameters = df_call_parameters[df_call_parameters['origin'] != df_call_parameters['destination']]    




df_airports_country_origin = df_airports[['continent','subcontinent','country','airport','iata']] 
df_airports_country_origin.columns = ['origin_continent','origin_subcontinent',"origin_country", 'origin_airport', "origin"]
df_airports_country_destination = df_airports[['continent','subcontinent','country','airport','iata']] 
df_airports_country_destination.columns = ['destination_continent','destination_subcontinent',"destination_country", 'destination_airport', "destination"]


df_call_parameters = df_call_parameters.merge(df_airports_country_origin,how='left').merge(df_airports_country_destination,how='left').replace('',None)

df_call_parameters = df_call_parameters.dropna(subset=['totalPages'])




df_call_parameters = df_call_parameters.loc[:, ['origin_continent','origin_country','origin_airport','origin','destination_continent','destination_country','destination_airport','destination','startRange','endRange','totalPages','totalFlights']]


df_call_parameters['period_duration_day'] = df_call_parameters.apply(lambda row: (datetime.datetime.fromisoformat(row['endRange'].replace("Z","")) - datetime.datetime.fromisoformat(row['startRange'].replace("Z",""))   )  , axis=1)

df_call_parameters['period_duration_day'] = df_call_parameters.apply(lambda row: row.period_duration_day.days + row.period_duration_day.seconds /(24*60*60)     , axis=1)




df_call_parameters['pages_per_day'] = df_call_parameters['totalPages'] /  df_call_parameters['period_duration_day']  
df_call_parameters['flights_per_day'] = df_call_parameters['totalFlights'] /  df_call_parameters['period_duration_day']  


df_call_parameters['pages_per_day'] = df_call_parameters['pages_per_day'].astype('float').round(2)
df_call_parameters['flights_per_day'] = df_call_parameters['flights_per_day'].astype('float').round(2)

df_call_parameters.info()




df_call_parameters_agg = df_call_parameters.groupby(['origin','destination','origin_continent','origin_country','origin_airport','destination_continent','destination_country','destination_airport'],
    dropna=False).aggregate({"pages_per_day":['mean'],"flights_per_day":['mean'],"totalPages":['sum'],"totalFlights":['sum']}).reset_index()

df_call_parameters_agg.columns = ['origin','destination','origin_continent','origin_country','origin_airport','destination_continent','destination_country','destination_airport','pages_per_day','flights_per_day','totalPages','totalFlights']

df_call_parameters_agg = df_call_parameters_agg.loc[:, ['origin_continent','origin_country','origin_airport','origin','destination_continent','destination_country','destination_airport','destination','pages_per_day','flights_per_day','totalPages','totalFlights']]

df_call_parameters_agg['pages_per_day'] = df_call_parameters_agg['pages_per_day'].astype('float').round(2)
df_call_parameters_agg['flights_per_day'] = df_call_parameters_agg['flights_per_day'].astype('float').round(2)






df_call_parameters_agg_origin = df_call_parameters_agg.groupby(['origin','origin_continent','origin_country','origin_airport'],
    dropna=True).aggregate({"pages_per_day":['sum'],"flights_per_day":['sum'],"totalPages":['sum'],"totalFlights":['sum']}).reset_index()

df_call_parameters_agg_origin.columns = ['origin','origin_continent','origin_country','origin_airport','pages_per_day','flights_per_day','totalPages','totalFlights']

df_call_parameters_agg_origin = df_call_parameters_agg_origin.loc[:, ['origin_continent','origin_country','origin_airport','origin','pages_per_day','flights_per_day','totalPages','totalFlights']]

df_call_parameters_agg_origin['pages_per_day'] = df_call_parameters_agg_origin['pages_per_day'].astype('float').round(2)
df_call_parameters_agg_origin['flights_per_day'] = df_call_parameters_agg_origin['flights_per_day'].astype('float').round(2)



df_call_parameters_agg_origin_pairs = df_call_parameters_agg_origin.dropna(subset=['origin'])

df_call_parameters_agg_origin_pairs.to_csv('data_stats/afklm_api_data_stats_origin.csv', index=0)


df_call_parameters_agg_destination = df_call_parameters_agg.groupby(['destination','destination_continent','destination_country','destination_airport'],
    dropna=True).aggregate({"pages_per_day":['sum'],"flights_per_day":['sum'],"totalPages":['sum'],"totalFlights":['sum']}).reset_index()

df_call_parameters_agg_destination.columns = ['destination','destination_continent','destination_country','destination_airport','pages_per_day','flights_per_day','totalPages','totalFlights']

df_call_parameters_agg_destination = df_call_parameters_agg_destination.loc[:, ['destination_continent','destination_country','destination_airport','destination','pages_per_day','flights_per_day','totalPages','totalFlights']]

df_call_parameters_agg_destination['pages_per_day'] = df_call_parameters_agg_destination['pages_per_day'].astype('float').round(2)
df_call_parameters_agg_destination['flights_per_day'] = df_call_parameters_agg_destination['flights_per_day'].astype('float').round(2)



df_call_parameters_agg_destination_pairs = df_call_parameters_agg_destination.dropna(subset=['destination'])

df_call_parameters_agg_destination_pairs.to_csv('data_stats/afklm_api_data_stats_destination.csv', index=0)





df_call_parameters_agg_pairs = df_call_parameters_agg.dropna(subset=['origin','destination'])

df_call_parameters_agg_pairs.to_csv('data_stats/afklm_api_data_stats_pairs.csv', index=0)

