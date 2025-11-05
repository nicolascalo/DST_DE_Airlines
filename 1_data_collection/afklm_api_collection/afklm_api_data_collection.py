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


### Script parameters
path_data_storage = "data"
path_call_parameter_file_folder = "call_parameter_lists"
output_format = ["gzip"]  # ["json","gzip"]
skip_previously_failed_serverError = True
skip_previously_failed_flightNotFound = True
skip_previously_failed_otherErrors = True
api_key_list_folder = "api_keys"
skip_complete = True
add_new_dates_csv_parameters = True

future_days_to_retrieve = 30

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

### Working directory adjustments
cwd = os.getcwd()


if cwd.endswith("DST_DE_Airlines"):
    os.chdir("1_data_collection/afklm_api_collection")
elif cwd.endswith("1_data_collection"):
    os.chdir("afklm_api_collection")
else:
    script_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_path)
    
### Create folder for retrieved data
os.makedirs(path_data_storage, exist_ok=True)

### List of already retrieved data and parameter CSV files
json_list = os.listdir(path_data_storage)
call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)

if add_new_dates_csv_parameters:
    for call_parameter_csv in call_parameter_csv_list:
    
    ### Update with new dates when all pages of current parameter file retrieved or failed
        print(f"adding missing dates to {call_parameter_csv}")

        df_call_parameters = pd.read_csv(path_call_parameter_file_folder + "/" + call_parameter_csv,low_memory=False).fillna('').sort_values(['endRange','completion'])
        
        df_call_parameters_root = df_call_parameters.drop(non_parameters,axis =1 ,errors='ignore').drop_duplicates()
        
        params = list(df_call_parameters_root.columns)
        params.remove('startRange')
        params.remove('endRange')
        
        df_call_parameters_root = df_call_parameters_root.drop('startRange',axis=1).groupby(params).max().reset_index()
        
        for index, row in df_call_parameters_root.iterrows():
            
            endRange = str(row.endRange)
            endRange = endRange.replace('Z','')

            
            while ((datetime.datetime.fromisoformat(endRange).date() - datetime.datetime.now().date() ).days) < future_days_to_retrieve :
            
                row_new =  df_call_parameters_root.iloc[[index]].copy()
                row_new['startRange'] = (datetime.datetime.fromisoformat(endRange) + datetime.timedelta(seconds=1)).isoformat() + "Z"
                endRange = (datetime.datetime.fromisoformat(endRange) + datetime.timedelta(days=1)).isoformat()
                row_new['endRange'] = endRange + "Z"
                
                with open(f"{path_call_parameter_file_folder + "/" + call_parameter_csv}","a") as f:
                    row_new.to_csv(f, header=False,index = 0, lineterminator='\n')


                endRange = row_new['endRange'].item().replace('Z','')
                
        
        print(f"adding missing dates to {call_parameter_csv} over")
        











### Load API keys
API_key_list_cleaned = pd.DataFrame()
for file in os.listdir(api_key_list_folder):
    if file.endswith(".tsv"):
        API_key_list = pd.read_csv(os.path.join(api_key_list_folder, file), delimiter="\t",low_memory=False)
        API_key_list_cleaned = pd.concat([API_key_list_cleaned, API_key_list], ignore_index=True)

API_key_list_cleaned = (
    API_key_list_cleaned.sort_values("timestamp")
    .drop_duplicates(subset="api_key", keep="last")
)

API_key_list_cleaned["timestamp"] = API_key_list_cleaned["timestamp"].fillna(datetime.datetime.now().isoformat())

API_key_list_cleaned["nb_calls_today"] = API_key_list_cleaned.apply(
    lambda row: 0
    if (datetime.datetime.now().date() - datetime.datetime.fromisoformat(row["timestamp"]).date()).days > 0
    else row["nb_calls_today"],
    axis=1,
)
API_key_list_cleaned["timestamp"] = API_key_list_cleaned.apply(
    lambda row: datetime.datetime.now().isoformat()
    if (datetime.datetime.now().date() - datetime.datetime.fromisoformat(row["timestamp"]).date()).days > 0
    else row["timestamp"],
    axis=1,
)

last_call_time = datetime.datetime.now()
char = " "



for index, record in API_key_list_cleaned.iterrows():
    
    
        
    API_key = record['api_key']
    nb_calls_today = record['nb_calls_today']
    
    print(Style.RESET_ALL)
    
    api_key_desc =  f"API key {record['key_desc']}"
    
    print(f"{api_key_desc}")
    
    if nb_calls_today == max_daily_api_call:
        print(Fore.YELLOW + f"-> Daily call quota reached. Trying next API key")
        
        continue
    
    print(Fore.GREEN + f"{max_daily_api_call - nb_calls_today} / 100 API calls left for today")
    
    for call_parameter_csv in call_parameter_csv_list:

        
        print(Style.RESET_ALL)
        print("#"*90+ "\n"+call_parameter_csv+ "\n"+"#"*90+ "\n")

        
        ### Import query parameters
        df_call_parameters = pd.read_csv(
            path_call_parameter_file_folder + "/" + call_parameter_csv,low_memory=False
        ).fillna('')

        call_parameters_list = []

        for i in range(len(df_call_parameters)):
            df_subset_parameter = df_call_parameters.iloc[[i]].to_dict(orient="list")
            call_parameters_url = "&".join(
                [key + "=" + str(val[0]) for key, val in df_subset_parameter.items()
                if val[0] != '' and val[0] != '[nan]']
            )
            call_parameters_list.append(call_parameters_url)

        df_call_parameters['call_parameters'] = call_parameters_list
        

        ### Loading API keys to use
        
        

        ### Definition of base URLs for API call
        base_url = "https://api.airfranceklm.com/opendata/flightstatus/?"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        ### Definition of default parameters for API call
        dict_call_parameters = {
            "aircraftRegistration": '',  # string Registration code of the aircraft
            "aircraftType": '',  # string Filter by a type of aircraft
            "arrivalCity": '',  # string Filter by airport code of arrival city
            "carrierCode": [],  # array[string] Airline code
            "consumerHost": '',  # string System info from which request is launched
            "departureCity": '',  # string IATA departure city code
            "destination": '',  # string Destination airport
            "flightNumber": '',  # string Filter by flight number
            "movementType": '',  # string Focus (Departure/Arrival)
            "operatingAirlineCode": [],  # array[string] Operating airline code
            "operationalSuffix": '',  # string Operational suffix
            "origin": '',  # string Departure airport
            "serviceType": [],  # array[string] IATA service type code
            "timeOriginType": '',  # string S/M/I/P
            "timeType": '',  # string U/L
            "endRange": '2025-07-23T23:59:59Z',  # string<date-time>
            "startRange": '2025-07-21T09:00:00Z',  # string<date-time>
            "call_parameters": '',  # repopulated after request
            'response': '',  # repopulated after request
            'message': '',  # repopulated after request
            'timestamp': '',  # repopulated after request
            'nb_of_pages_already_retrieved': '',  # repopulated after request
            'totalPages': '',  # repopulated after request
            'completion': ''  # repopulated after request
        }

        dict_call_parameters["carrierCode"] = ",".join(dict_call_parameters['carrierCode'])
        dict_call_parameters["operatingAirlineCode"] = ",".join(dict_call_parameters['operatingAirlineCode'])
        dict_call_parameters["serviceType"] = ",".join(dict_call_parameters['serviceType'])

        df_call_parameters = pd.DataFrame(dict_call_parameters, index=[0])  # from defaults

        if os.path.isfile(path_call_parameter_file_folder + "/" + call_parameter_csv):
            df_call_parameters = pd.read_csv(path_call_parameter_file_folder + "/" + call_parameter_csv,low_memory=False).fillna('')


        print( f"Max number of pages to retrieve: {max_page_to_fetch} ")


        print( f"Number of API call parameters to process = {len(df_call_parameters)}")

            
    
        i = 0
        df_call_parameters_new = df_call_parameters.copy(deep=True)  # to update the CSV after each query



        ### Loop over the CSV file containing the parameter list to send to the API
        for i in range(0, len(df_call_parameters)):
            to_test = float(df_call_parameters.iloc[[i]]['completion'].replace('','0').item())
            
            if skip_complete & (to_test == 100):
                continue
                # print( f"Skipping already completely retrieved API calls ")
            print("")
            df_subset = df_call_parameters.iloc[[i]].copy(deep=True).reset_index().drop(['index'], axis=1)
            pageNumber = pageNumberStart  # first page is 1; page 0 returns same results

            ### Check if query parameter already tested and skip previously failed if chosen
            text_response = str(df_subset['response'])
            match_error = re.search("\\d\\d\\d", text_response)
            if match_error is None:
                match_error = "000"
            else:
                match_error = match_error[0]

            ### Cleaning of empty parameter calls
            parameter_list = df_subset.drop(non_parameters, axis=1, errors='ignore').columns.to_list()
            dict_call_parameters = df_subset.drop(non_parameters, axis=1, errors='ignore').to_dict(orient="list")
            call_parameters_url = "&".join([key + "=" + str(val[0])
                                            for key, val in dict_call_parameters.items()
                                            if val[0] != '' and val[0] != '[nan]'])

            print(Fore.RESET + f"{call_parameters_url}")

            


            if skip_previously_failed_serverError & (int(match_error) >= 500):
                print(Fore.MAGENTA + f"skipped because previously failed due to server error")
                continue

            if skip_previously_failed_flightNotFound & (int(match_error) == 404):
                print(Fore.MAGENTA + f"skipped because previously obtained 'FLIGHT NOT FOUND'")
                continue
            
            if skip_previously_failed_otherErrors & (int(match_error) > 200):
                print(Fore.MAGENTA + f"skipped because previously obtained another error")
                continue
            
            

            url = (base_url + call_parameters_url).replace(" ", "")

            ### Check date query coherence
            if df_subset['endRange'].item() < df_subset['startRange'].item():
                print("ERROR: endRange < startRange")
                break

            ### Loop until desired number of pages or max pages reached
            while (pageNumber + 1 <= page_max) & (pageNumber + 1 <= max_page_to_fetch) & (nb_calls_today < 101):

                json_to_make_root = f"afklm_api_data_collection_{re.sub(':', '_', call_parameters_url)}"
                
                
                date_diff = (datetime.datetime.fromisoformat(df_subset['startRange'].item()).date() - datetime.datetime.date(datetime.datetime.now())).days
                
                if date_diff > 0:
                    json_to_make = json_to_make_root + f"_{pageNumber}_sched.json"
                elif date_diff == 0:
                    json_to_make = json_to_make_root + f"_{pageNumber}_updSchedD1.json"
                else:
                    json_to_make = json_to_make_root + f"_{pageNumber}.json"
                    

                df_totalPages = df_subset['totalPages']
                df_item = df_subset['totalPages'].item()
                df_values = df_subset['totalPages'].values
                time_analysis = datetime.datetime.now().isoformat()

                # Skip current query if file already exists
                if (json_to_make in json_list) | (json_to_make + '.gz' in json_list):
                    if df_item == '':
                        print(Fore.BLUE + "loading page info from already retrieved files")
                        file_to_open = [file for file in json_list if json_to_make in file][0]
                        if '.gz' in file_to_open:
                            with gzip.open(f"{path_data_storage}/" + file_to_open) as json_file:
                                data = json.load(json_file)
                        else:
                            with open(f"{path_data_storage}/" + file_to_open) as json_file:
                                data = json.load(json_file)
                        page_max = data['page']['totalPages']
                        df_subset.loc[0, ['totalPages']] = page_max
                    else:
                        page_max = df_item

                if (json_to_make in json_list) | (json_to_make + '.gz' in json_list):
                    print(Fore.BLUE + f"Page {pageNumber} : skipped because already retrieved")
                    if (page_max == pageNumber + 1):
                        print(Fore.BLUE + f"All pages already retrieved")
                        df_subset.loc[0, ['nb_of_pages_already_retrieved']] = df_subset.loc[0, ['totalPages']].item()
                        df_subset.loc[0, ['completion']] = 100
                        df_subset.loc[0, ['timestamp']] = time_analysis

                        df_subset.loc[0, ['call_parameters']] = call_parameters_url
                        df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)
                        df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
                        df_call_parameters_new.fillna('').sort_values(['endRange','completion']).to_csv(path_call_parameter_file_folder + "/" + call_parameter_csv, index=0)
                    pageNumber += 1
                    continue

                # Main API request logic
                
                headers['API-Key'] = API_key
                url_page = (url + f"&{pageNumber=}").replace("?&", "?")
                
                
                while (datetime.datetime.now() - last_call_time).seconds < 1.1:
                    time.sleep(0.1)
                    
                time.sleep(time_delay_query)
                
                response = requests.get(url_page, headers=headers)
                
                last_call_time = datetime.datetime.now()
                
                
                nb_calls_today = nb_calls_today + 1
                
                API_key_list_cleaned['nb_calls_today'] = API_key_list_cleaned.apply(lambda row: nb_calls_today if row['api_key'] == API_key else row['nb_calls_today'] , axis=1)
                API_key_list_cleaned.to_csv( api_key_list_folder+ "/afklm_api_keys.tsv",sep = '\t',index = 0)
                
                
                

                
                df_subset.loc[0, ['timestamp']] = time_analysis
                df_subset.loc[0, ['call_parameters']] = call_parameters_url

                if response.__bool__():
                    data = response.json()
                    page_max = data['page']['totalPages']
                    fullCount = data['page']['fullCount']

                    if "json" in output_format:
                        with open(f"{path_data_storage}/{json_to_make}", 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=4)
                    if "gzip" in output_format:
                        with gzip.open(f"{path_data_storage}/{json_to_make}.gz", 'wt', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=4)

                    print(Fore.GREEN + f"Page {pageNumber} : retrieval OK" + Fore.RESET + f"    Total: {page_max}")

                    if df_subset['nb_of_pages_already_retrieved'].item() == '':
                        df_subset.loc[0, ['nb_of_pages_already_retrieved']] = 0

                    if int(pageNumber + 1) > int(df_subset['nb_of_pages_already_retrieved'].item()):
                        df_subset.loc[0, ['nb_of_pages_already_retrieved']] = float(f"{(pageNumber+1):.0f}")

                    df_subset.loc[0, ['response']] = str(response)
                    df_subset.loc[0, ['totalPages']] = float(f"{(page_max):.0f}")
                    df_subset.loc[0, ['totalFlights']] = float(f"{(fullCount):.0f}")
                    df_subset.loc[0, ['completion']] = float(f"{100*(pageNumber+1)/page_max:.0f}")
                    df_subset.loc[0, ['message']] = ""

                    df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)
                    df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
                    df_call_parameters_new.fillna('').to_csv(path_call_parameter_file_folder + "/" + call_parameter_csv, index=0)

                    pageNumber += 1

                elif ("Developer" in response.text):
                    print(Fore.RED + "API daily quota consumed")
                    print(Style.RESET_ALL)
                    nb_calls_today = 100
                    

                    API_key_list_cleaned['nb_calls_today'] = API_key_list_cleaned.apply(lambda row: 100 if row['api_key'] == API_key else row['nb_calls_today'] , axis=1)
                    API_key_list_cleaned['timestamp'] = API_key_list_cleaned.apply(lambda row: time_analysis if row['api_key'] == API_key else row['timestamp'] , axis=1)

                    API_key_list_cleaned = API_key_list_cleaned.drop_duplicates(subset='api_key',keep='last')
                    API_key_list_cleaned.to_csv( api_key_list_folder+ "/afklm_api_keys.tsv",sep = '\t',index = 0)
                    

                    break

                else:
                    print(Fore.RED + f"Issues with the call: {response} {response.text}")
                    df_subset.loc[0, ['response']] = str(response)
                    df_subset.loc[0, ['message']] = str(response.text)
                    df_call_parameters_new = pd.concat([df_call_parameters_new, df_subset], ignore_index=True)
                    df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=['call_parameters'], keep='last')
                    df_call_parameters_new.fillna('').sort_values(['endRange','completion']).to_csv(path_call_parameter_file_folder + "/" + call_parameter_csv, index=0)
                    break
            
            if nb_calls_today == 100:
                break

        
            

print(Style.RESET_ALL)
