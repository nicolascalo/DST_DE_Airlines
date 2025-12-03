"""
Written by Nicolas Calo

Script for data mining on the Air France KLM "https://api.airfranceklm.com/opendata/flightstatus/" API.

This script requires an afklm_api_keys.txt file containing the personal API keys (one on each line) to use to query the API.

An additional df_call_parameters.csv file can be provided to set up the parameters to query. This csv should contain a dataframe specifying all the combinations of values needed to be retrieved (one query for each row of the dataframe)

The script will iterate over:
- each row of the dataframe for the parameters (or use a single set of default values if no csv provided)
- each page of the query results
- each provided API keys when a key daily allotment has been totally consumed

For each query, a .json file named according to the parameters of the query and page number will be produced in the /data folder. Upon subsequent runs of this script, it will skip any API calls for which there is already a corresponding .json file (page nb and call parameter) 

!!! define max_page_to_fetch to limit the number of pages to retrieve
!!! Always define dates otherwise it will get the current date and tracking of what has already been retrieved will not be ensured!!!
"""

### Library import
import pandas as pd
import requests
import re
import time
import json
import os
import datetime
from colorama import Fore, Style
import gzip
import base64
import functions_framework
import pandas as pd
import requests
import re
import time
import json
import os
from io import BytesIO
import datetime
import gzip

PROJECT_ID = "trusty-anchor-473006-u9"
bucket_name = "airfrance-bucket"


pd.set_option('future.no_silent_downcasting', True)

### Script parameters
path_data_storage = "data"
path_call_parameter_file_folder = "call_parameter_lists"
skip_previously_failed_serverError = True
skip_previously_failed_flightNotFound = True
skip_previously_failed_invalidDateRange = True # The API allows to retrieve 180 days in the past and 1 year in the future
skip_previously_failed_otherErrors = True
api_key_list_folder = "api_keys"
skip_complete = True
add_new_dates_csv_parameters = True

future_days_to_retrieve = 365

max_daily_api_call = 100 # API limited to 1 call / s, 100 / day
max_page_to_fetch = 10000000000
pageNumberStart = 0
page_max = 100000  # Will auto-adjust after first page retrieved
refresh_stats = False
time_delay_query = 0 # to increase time between queries. If 0, will anyway check for 1.1 seconds between calls

non_parameters = [
    "call_parameters", "response", "message", "timestamp",
    "nb_of_pages_already_retrieved", "totalPages", "completion", "totalFlights"
]

pd.options.mode.chained_assignment = None  # suppress warnings


on_cloud = False
bucket = None




### general functions for GCP/local handling

def import_csv(path_folder:str,path_file:str, bucket = bucket):
    if on_cloud:
        csv_blob = bucket.blob(path_file)
        csv_data = csv_blob.download_as_bytes()         

        data =  pd.read_csv(BytesIO(csv_data),encoding="utf-8",low_memory=False)

    else:    

        data =  pd.read_csv('/'.join([path_folder,path_file]),low_memory=False)
    return data


def save_csv(df, path_folder:str,path_file:str, bucket = bucket) -> None:
    if on_cloud:

        csv_blob = bucket.blob(path_file)
        csv_buffer = BytesIO(bytes(df.to_csv(index=False), encoding='utf-8'))
        
        csv_blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        # logger.info(f"{path_file} updated")
       
    else:    
        df.to_csv('/'.join([path_folder,path_file]),index = 0)
    return None





def info_message(text:str, color:str=None, level_info:str=None) -> None:
    
    if color is None:
        print(Fore.RESET + text)
    else:
        print(eval(f'Fore.{color.upper()}') + text)
    '''
    if on_cloud:

        match level_info:
            case 'info':
                logger.info(text)
            case 'warning':
                logger.warning(text)
            case 'error':
                logger.error(text)
            case _:
                logger.info(text)
    '''
    return None




def list_json_files(path_data_storage:str, bucket = bucket) -> list:
    if on_cloud:
        json_list_blobs = client_storage.list_blobs(bucket, prefix=path_data_storage)
        json_list = [val.name for val in json_list_blobs if 'json' in val.name]
    
    else:
        json_list = os.listdir(path_data_storage)
        json_list = [val for val in json_list if 'json' in val]

    
    json_list.sort()

    return json_list






def open_json(path_data_storage:str,file_to_open:str, bucket = bucket) -> None:

    if on_cloud:

        gzip_blob_name = f"{path_data_storage}/{json_to_make}.gz"    
        gzip_blob = bucket.blob(gzip_blob_name)
        buffer=BytesIO()
        with gzip.open(fileobj=buffer, mode='rb') as json_file:
            data = json.load(json_file)

    else:
        with gzip.open(f"{path_data_storage}/" + file_to_open) as json_file:
            data = json.load(json_file)
    
    return data



    

def list_call_parameters(path_call_parameter_file_folder:str, bucket = bucket) -> list:

    if on_cloud:
        path_call_parameter_csv_list = client_storage.list_blobs(bucket,prefix=path_call_parameter_file_folder)
        call_parameter_csv_list = [val.name for val in path_call_parameter_csv_list if ('df_call_parameters'  in val.name) & (len(re.findall("csv$",val.name)) > 0)]
    
    else:
        call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)
        call_parameter_csv_list = [val for val in call_parameter_csv_list if ('df_call_parameters'  in val) & (len(re.findall("csv$",val) )> 0

)]
    
    call_parameter_csv_list.sort()

    return call_parameter_csv_list

    



### Working directory adjustments


call_parameter_csv_list = list_call_parameters(path_call_parameter_file_folder=path_call_parameter_file_folder,bucket=bucket)
json_list = list_json_files(path_data_storage,bucket)







for call_parameter_csv in call_parameter_csv_list:

    
    
    info_message("\n".join(["#"*90,call_parameter_csv,"#"*90,""]))

    
    ### Import query parameters
    df_call_parameters = import_csv(
        path_call_parameter_file_folder ,call_parameter_csv
    ).fillna('')
    
    try:
        df_call_parameters = df_call_parameters.sort_values(['endRange','completion'])
    except:
        print("Columns endRange or completion not found in the dataframe")


    call_parameters_list = []

    for i in range(len(df_call_parameters)):
        df_subset_parameter = df_call_parameters.iloc[[i]].to_dict(orient="list")
        call_parameters_url = "&".join(
            ["=".join([key,str(val[0])]) for key, val in df_subset_parameter.items()
            if val[0] and val[0] != '[nan]']
        )
        call_parameters_list.append(call_parameters_url)

    df_call_parameters['call_parameters'] = call_parameters_list
    


    ### Definition of base URLs for API call


    
    df_call_parameters = df_call_parameters.sort_values(['startRange','endRange','origin','destination','completion'])


        

    i = 0
    df_call_parameters_new = df_call_parameters.copy(deep=True)  # to update the CSV after each query

    df_call_parameters_to_update = df_call_parameters[(df_call_parameters['totalFlights'] == '') | (df_call_parameters['totalPages'] == '')]





    ### Loop over the CSV file containing the parameter list to send to the API
    for i in range(0, len(df_call_parameters_to_update)):

        df_subset = df_call_parameters_to_update.iloc[[i]].copy(deep=True).reset_index().drop(['index'], axis=1)
                
        

        if df_subset['totalFlights'].item() != '':
            
            continue


        startRange = str(df_subset['startRange'].values.item()).replace('Z','')
        endRange = str(df_subset['endRange'].values.item()).replace('Z','')
        

        
        
        print("")


        ### Cleaning of empty parameter calls
        parameter_list = df_subset.drop(non_parameters, axis=1, errors='ignore').columns.to_list()
        dict_call_parameters = df_subset.drop(non_parameters, axis=1, errors='ignore').to_dict(orient="list")
        call_parameters_url = "&".join([key + "=" + str(val[0])
                                        for key, val in dict_call_parameters.items()
                                        if val[0] != '' and val[0] != '[nan]'])

        info_message(f"{call_parameters_url}")
        


        ### Loop until desired number of pages or max pages reached

        json_to_make_root = f"afklm_api_data_collection_{re.sub(':', '_', call_parameters_url)}"
        
        try:
            json_to_read = [file for file in json_list if json_to_make_root in file][0]

            file_to_open = json_to_read
            data = open_json(path_data_storage,file_to_open,bucket)
            page_max = data['page']['totalPages']
            totalFlights = data['page']['fullCount']
            df_subset.loc[0, ['totalPages']] = page_max
            df_subset.loc[0, ['totalFlights']] = totalFlights

            df_subset.loc[0, ['call_parameters']] = call_parameters_url





            df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)




        except:
            continue    


    df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
    df_call_parameters_new = df_call_parameters_new.fillna('').sort_values(['endRange','completion'])


    save_csv(
        df_call_parameters_new, path_folder=path_call_parameter_file_folder,path_file=call_parameter_csv, bucket = bucket
        )

