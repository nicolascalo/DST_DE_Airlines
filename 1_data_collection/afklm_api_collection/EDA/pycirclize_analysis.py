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


path_call_parameter_file_folder = "call_parameter_lists"
path_call_parameter_csv_root = "df_call_parameters"
remove_loop_from_to = True



def make_plot_chord(df,
                    append_parent_group = False,
                    group_by:str = None, 
                    treshold:float = None,
                    gather_all_but_n_higher:int = None,
                    remove_all_but_n_higher:bool = False,
                    flight_type:str=None,
                    space:float = 0.5,
                    order:str = None,
                    cmap :str='rainbow',
                    label_kws:dict=dict(r=110, orientation="vertical"),
                    *args_chord_diagram,
                    **kwargs_chord_diagram):
    
# Create from-to table dataframe & convert to matrix


    
    if group_by == 'country':
        
        
        if append_parent_group:
        
            fromto_table_df = df[['origin_continent','origin_country','destination_continent','destination_country','totalFlights']].copy(deep=True)  
            
            fromto_table_df['origin_country']  = fromto_table_df['origin_continent'] + " - "+ fromto_table_df['origin_country'] 
            fromto_table_df['destination_country']  = fromto_table_df['destination_continent'] + " - "+ fromto_table_df['destination_country'] 
            fromto_table_df = fromto_table_df[['origin_country','destination_country','totalFlights']]     
        else:
            fromto_table_df = df[['origin_country','destination_country','totalFlights']]     
            
        
    elif group_by == 'subcontinent':    
        fromto_table_df = df[['origin_subcontinent','destination_subcontinent','totalFlights']]       
    elif group_by == 'continent':    
        fromto_table_df = df[['origin_continent','destination_continent','totalFlights']]       
        
        
    else:
        
        fromto_table_df = df_call_parameters[['origin_country',"origin",'destination_country',"destination",'totalFlights']].copy(deep=True)
        
        
        fromto_table_df['origin_country_airport']  = fromto_table_df['origin_country'] + " - "+ fromto_table_df['origin'] 
        fromto_table_df['destination_country_airport']  =fromto_table_df['destination_country'] + " - "+ fromto_table_df['destination'] 
        
        
        fromto_table_df = fromto_table_df[['origin_country_airport','destination_country_airport','totalFlights']]
        

    ### Analysis


    fromto_table_df.columns = ["from", "to", "value"]
    
    if (flight_type == 'local') & (group_by is not None):
        fromto_table_df = fromto_table_df[fromto_table_df['to'] == fromto_table_df['from']]

    if flight_type == 'int':
        fromto_table_df = fromto_table_df[fromto_table_df['to'] != fromto_table_df['from']]

    fromto_table_df = fromto_table_df[fromto_table_df['value']!='']
        
    total_flights =    fromto_table_df['value'].astype('float').sum() 
    fromto_table_df['value'] =     fromto_table_df['value'] / total_flights
    
    

    from_list = list(fromto_table_df['from'].values)
    to_list = list(fromto_table_df['to'].values)

    location_set = set(from_list+to_list)
    location_list = list(location_set)
    location_cum_prop = []    
    
    
    for location in location_set:
        cum_prop = fromto_table_df[(fromto_table_df['to'] == location)| (fromto_table_df['from'] == location)]['value'].sum()
        location_cum_prop.append(cum_prop)
    

    if treshold is not None :


        under_treshold = (np.array(location_cum_prop) < treshold).tolist()
        locations_to_gather = (np.array(location_list)[under_treshold]).tolist() 
        
    elif gather_all_but_n_higher is not None:
        
        rank_index = [sorted(location_cum_prop, reverse=True).index(x) for x in location_cum_prop]
        lower = np.array(rank_index) >= gather_all_but_n_higher
        
        locations_to_gather = (np.array(location_list)[lower]).tolist()
    else:
        locations_to_gather = []
        
    
    fromto_table_df = fromto_table_df.replace(to_replace=locations_to_gather,value = 'Other').infer_objects(copy=False).sort_values(['from','to'])
        
    if remove_all_but_n_higher:
        fromto_table_df = fromto_table_df[(fromto_table_df['to'] != 'Other' )& (fromto_table_df['from'] != 'Other')]
            
        
    matrix = Matrix.parse_fromto_table(fromto_table_df)

    circos = Circos.chord_diagram(
        matrix,
        cmap=cmap,
        order= order,
        label_kws = label_kws,
        space = space,
        *args_chord_diagram,
        **kwargs_chord_diagram

    )

    plot = circos.plotfig()
    plot.show()
    return plot







df_airports = pd.read_csv("../df_iata_icao_wiki_final_world.csv").fillna('')


path_call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)

call_parameter_csv_list = [val for val in path_call_parameter_csv_list if 'df_call_parameters'  in val]

df_call_parameters = pd.DataFrame()



for call_parameter_csv in call_parameter_csv_list :
    
    df_call_parameters_to_add = pd.read_csv(path_call_parameter_file_folder +"/"+call_parameter_csv).fillna('')

    df_call_parameters = pd.concat([call_parameter_csv_merged, df_call_parameters],ignore_index=True).fillna('').sort_values(['startRange','endRange'])





if remove_loop_from_to:
    df_call_parameters = df_call_parameters[df_call_parameters['origin'] != df_call_parameters['destination']]    

df_airports_country_origin = df_airports[['continent','subcontinent','country','iata']] 
df_airports_country_origin.columns = ['origin_continent','origin_subcontinent',"origin_country", "origin"]
df_airports_country_destination = df_airports[['continent','subcontinent','country','iata']] 
df_airports_country_destination.columns = ['destination_continent','destination_subcontinent',"destination_country", "destination"]


df_call_parameters = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination)
    
    

df_call_parameters_from_france = df_call_parameters.query('origin_country == "France"')


df_call_parameters_to_france = df_call_parameters.query('destination_country == "France"')





make_plot_chord(df_call_parameters,
                group_by = False,
                    treshold = None,
                    gather_all_but_n_higher = None,
                    flight_type=None,
                    order='asc',
                    space = 0.5)



make_plot_chord(df_call_parameters,
                group_by = True,
                    treshold = None,
                    gather_all_but_n_higher = 15,
                    remove_all_but_n_higher = True,
                    flight_type=None,
                    order=None,
                    space = 0.5)


make_plot_chord(df_call_parameters,
                append_parent_group= True,
                 group_by = 'country',
                    treshold = None,
                    gather_all_but_n_higher = 20,
                    remove_all_but_n_higher = False,
                    flight_type=None,
                    order=None,
                    space = 0.5)



make_plot_chord(df_call_parameters,
                group_by = True,
                    treshold = None,
                    gather_all_but_n_higher = 7,
                    flight_type='int',
                    order=None,
                    space = 0.5)


make_plot_chord(df_call_parameters_from_france,
                group_by = True,
                    treshold = None,
                    gather_all_but_n_higher = 7,
                    flight_type='int',
                    order='asc',
                    space = 0.5)

make_plot_chord(df_call_parameters_to_france,
                group_by,
                    treshold = None,
                    gather_all_but_n_higher = 15,
                    flight_type='int',
                    order='asc',
                    space = 0.5)



plot.show()



circos_country_int.savefig("circos_country_int.png")


