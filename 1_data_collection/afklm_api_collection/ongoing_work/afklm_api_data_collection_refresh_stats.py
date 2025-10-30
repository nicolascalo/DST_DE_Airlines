### Library import


import pandas as pd
import re
import os
from colorama import Fore, Style
import gzip
import json

### Script parameters

path_data_storage = "data"
path_call_parameter_files = "call_parameter_lists"
path_call_parameter_csv_root = "df_call_parameters"



non_parameters = ["call_parameters",'response', 'message', 'timestamp', 'nb_of_pages_already_retrieved', 'totalPages', 'completion','totalFlights']



### Setting up working directory


if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


if  bool(re.search("1_data_collection$",os.getcwd())) == True:
    os.chdir("afklm_api_collection") 


### Creation of folders for retrieved data

if not os.path.isdir(path_data_storage):
    os.mkdir(path_data_storage)


### List of already retrieved data

json_list = os.listdir(path_data_storage)


path_call_parameter_csv_list = os.listdir('./call_parameter_lists')

call_parameter_csv_list = [val for val in path_call_parameter_csv_list if 'df_call_parameters'  in val]

for call_parameter_csv in call_parameter_csv_list :
    
    print(call_parameter_csv)
    
    

    ### Import query parameters
    
    df_call_parameters = pd.read_csv(path_call_parameter_files + "/" + call_parameter_csv).fillna('')
    
    df_call_parameters = df_call_parameters.drop(non_parameters,axis=1)
    
    
    call_parameters_list = []

    
    for i in range(len(df_call_parameters)):
     
        df_subset_parameter = df_call_parameters.iloc[[i]].to_dict(orient="list")

        call_parameters_url = "&".join([key + "=" + str(val[0]) for key, val in df_subset_parameter.items()
                            if val[0] != '' and val[0] != '[nan]'])
        
        call_parameters_list.append(call_parameters_url)
        
    df_call_parameters['call_parameters'] = call_parameters_list

    

    ### Refresh stats completion


        
    json_list = [val for val in json_list if '.json.gz' in val]
    
    df_json_list = pd.DataFrame({"index":range(0,len(json_list)), 'json_file':json_list})
    
    
    json_param_list = [re.sub("afklm_api_data_collection_","",json) for json in json_list]
    json_param_list = [re.sub("Z_.*","Z",json) for json in json_param_list]
    json_param_list = [re.sub("_",":",json) for json in json_param_list]
    
    df_json_list['call_parameters'] = json_param_list
    
    
    json_pages = [re.sub(".*_","",json) for json in json_list]
    json_pages = [re.sub("\\..*","",json) for json in json_pages]
    df_json_list['page'] = json_pages
    
    df_retrived_nb = df_json_list['call_parameters'].value_counts().rename_axis('call_parameters').reset_index(name='nb_of_pages_already_retrieved')
    df_retrived_nb['nb_of_pages_already_retrieved'] = df_retrived_nb['nb_of_pages_already_retrieved'].astype('int')
    
    max_page_list = []
    totalFlights_list = []
    
    df_single_file_list = df_json_list.groupby('call_parameters').head(1).copy(deep= True)
    
    
    
    
    for json_file in df_single_file_list['json_file'].values:
        
        
        with  gzip.open(f"{path_data_storage}/" + json_file) as json_file:
            
            data = json.load(json_file)
            page_max =  data['page']['totalPages']
            max_page_list.append(page_max)
            totalFlights =  data['page']['fullCount']
            totalFlights_list.append(totalFlights)
            
            


    df_single_file_list['totalPages'] = max_page_list
    df_single_file_list['totalPages'] = df_single_file_list['totalPages'].astype('int')
    df_single_file_list['totalFlights'] = totalFlights_list
    df_single_file_list['totalFlights'] = df_single_file_list['totalFlights'].astype('int')
    
    
    
    
    df_call_parameters_updated = df_call_parameters.drop(['nb_of_pages_already_retrieved','totalPages','totalFlights','completion','response','message'],axis = 1,errors='ignore').merge(df_single_file_list.drop(['page','json_file','index'], axis = 1), how= 'inner').merge(df_retrived_nb, how= 'inner')
    
    df_call_parameters_updated['completion'] = (100 * float(df_call_parameters_updated['nb_of_pages_already_retrieved'].values)  / df_call_parameters_updated['totalPages'].values ).round(0)
    df_call_parameters_updated['completion'] = df_call_parameters_updated['completion'].astype('float')
    
    
    df_call_parameters_updated['response'] = '<Response [200]>'
    
    
    
    df_call_parameters_final = pd.concat([df_call_parameters,df_call_parameters_updated]).drop_duplicates(subset='call_parameters', keep='last').sort_values(['completion'], na_position='first')
    
    df_call_parameters_final.to_csv(path_call_parameter_files + "/" + call_parameter_csv, index=0)
        

        