'''
Written by Nicolas Calo

Script for data mining on the AIr France KLM ""https://api.airfranceklm.com/opendata/flightstatus/" API.

This script requires an afklm_api_keys.txt file containing the personal API keys (one on each line) to use to querry the API.

An additional df_call_parameters.csv file can be provided to set up the parameters to query. This csv should contain a dataframe precising all the combinations of values needed to be retrived (one querry for each row of the datafrane)

The script will iterate over:
- each row of the dataframe for the parameters (or use a single set of default values if no csv provided)
- each page of the query results
- each provided API keys when a key daily allotment has beem totally consumed

For each querry, a .json file named according to the parameters of the querry and page number will be produced in the /data folder. Upon ulterior runs of this script, it will skip any API calls for which there is already a corresponding .json file (page nb and call parameter) 

!!! define max_page_to_fetch to limit the number of pages to retrieve
!!! Always define dates otherwise it will get the current date and tracking of what has already been retrived will not be ensured!!!!


'''

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
output_format = ["gzip"] # ["json","gzip"]
skip_previously_failed = False
api_key_list_file = "./afklm_api_keys.txt"
api_key_list_folder = "api_keys"



max_page_to_fetch = 100000
pageNumberStart = 0
page_max = 100000 # Will automatically be adjusted after having retrived the fist page 

refresh_stats = False
time_delay_query = 1.5 # API limited to 1 call / s, 100 / day
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


call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)


for call_parameter_csv in call_parameter_csv_list :
    
    print(call_parameter_csv)
    
    

    ### Import query parameters
    
    df_call_parameters = pd.read_csv(path_call_parameter_file_folder +"/"+call_parameter_csv).fillna('')
    
   
    
    call_parameters_list = []

    
    for i in range(len(df_call_parameters)):
     
        df_subset_parameter = df_call_parameters.iloc[[i]].to_dict(orient="list")

        call_parameters_url = "&".join([key + "=" + str(val[0]) for key, val in df_subset_parameter.items()
                            if val[0] != '' and val[0] != '[nan]'])
        
        call_parameters_list.append(call_parameters_url)
        
    df_call_parameters['call_parameters'] = call_parameters_list


    ### Loading API keys to use

    API_key_list_cleaned = []

    for file in os.listdir(api_key_list_folder) :

        with open(api_key_list_folder+"/"+file, "r") as f:
            API_key_list =  f.read().split("\n")
            
            for api in API_key_list:
                
                api_cleaned = re.sub(" #.*","",api)
                API_key_list_cleaned.append(api_cleaned)
                
        


    API_key_list_length = len(API_key_list_cleaned)

    ### Definition of base urls for API call

    base_url ="https://api.airfranceklm.com/opendata/flightstatus/?"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}


    ### Definition of default parameters for API call


    dict_call_parameters = {
    "aircraftRegistration": ''	, #	string	Registration code of the aircraft		PHBEF	
    "aircraftType": ''	, #	string	Filter by a type of aircraft		737	
    "arrivalCity": ''	, #	string	Filter by airport code of arrival city		DXB	
    "carrierCode": []	, #	array[string]	Airline code (2-pos IATA and 3-pos ICAO)		KL, AF	
    "consumerHost": ''	, #	string	The information about the system from which the request is launched		KL	
    "departureCity": ''	, #	string	IATA departure city code		DXB	
    "destination": '' , #	string	Destination airport		AMS	
    "flightNumber": ''	, #	string	Filter by flight number		202	
    "movementType": ''	, #	string	Focus (Departure or Arrival) for the flights to be found; used for selection of departure or arrival time within range **Sorting is based on movementType A": Arrival, D": Departure If D- flights will be sorted by scheduleDeparture time If A- flights will be sorted by scheduleArrivaltime	AD		
    "operatingAirlineCode": []	, #	array[string]	Operating airline code (2-pos IATA and 3-pos ICAO)		KL	
    "operationalSuffix": ''	, #	string	Operational suffix, indicates if a flight has been advanced or delayed to the previous or next day	AD	D,A,R,S,T,U,V,W	
    "origin": ''	, #	string	Departure airport		AMS	

    "serviceType": []	, #	array[string]	IATA service type code		J	
    "timeOriginType": ''	, #	string	S": Scheduled, M": Modified, I": Internal, P": Public	SMIP	S	
    "timeType": ''	, #	string	Type of time used in startRange and endRange U": UTC time, L": Local Time	UL	U	
    "endRange": '2025-07-23T23:59:59Z'	, #	string<date-time>	End on this date time		2023-12-31T23":59":59.000Z	required
    "startRange": '2025-07-21T09:00:00Z', #	string<date-time>	Start from this date time		2023-12-31T09":00":00.000Z	required

    "call_parameters": '', # repopulated after request
    'response': '', # repopulated after request
    'message': '', # repopulated after request
    'timestamp': '', # repopulated after request
    'nb_of_pages_already_retrieved': '', # repopulated after request
    'totalPages': '', # repopulated after request
    'completion': '' # repopulated after request

    }



    dict_call_parameters["carrierCode"] =",".join(dict_call_parameters['carrierCode'])
    dict_call_parameters["operatingAirlineCode"] =",".join(dict_call_parameters['operatingAirlineCode'])
    dict_call_parameters["serviceType"] =",".join(dict_call_parameters['serviceType'])



    df_call_parameters = pd.DataFrame(dict_call_parameters, index = [0]) # from defaults



    if os.path.isfile(path_call_parameter_file_folder+"/"+call_parameter_csv):
        df_call_parameters = pd.read_csv(path_call_parameter_file_folder+"/"+call_parameter_csv).fillna('')

    i = 0
    API_key_counter = 0 # To use the first API key from the list

    no_more_api_key = False

    df_call_parameters_new = df_call_parameters.copy(deep=True) # to update the df_call_parameters csv after each querry




    ### loop over the csv file containing the parameter list to send to the API

    for i in range(0, len(df_call_parameters)): 
        
        print("")

        
        
        df_subset = df_call_parameters.iloc[[i]].copy(deep = True).reset_index().drop(['index'], axis = 1) # parameters for the current querry

        
        pageNumber = pageNumberStart	 #	integer<int32>	Indicates the page number you are requesting, the first real page is page 1. Page 0 gets the same results than page 1. If it's not provided first page will be returned		1	


        ### Check if query parameter already tested and skip previous failed if chosen in the script options
        
        text_response = str(df_subset['response'])
        match_error  = re.search("\\d\\d\\d",text_response)
        if match_error is None:
            match_error = "000"
        else:
            match_error = match_error[0]
            


        ### Cleaning of empty parameter calls
        

        parameter_list = df_subset.drop(non_parameters,axis=1,errors='ignore').columns.to_list()
        
        dict_call_parameters = df_subset.drop(non_parameters,axis=1,errors='ignore').to_dict(orient="list")

        call_parameters_url = "&".join([key + "=" + str(val[0]) for key, val in dict_call_parameters.items()
                        if val[0] != '' and val[0] != '[nan]'])


        print(Fore.RESET + f"{call_parameters_url}")
        
        if skip_previously_failed & ( int(match_error)> 200):
            
            
            print(Fore.MAGENTA + f"skipped because previously failed")
            continue

        
        url = base_url + call_parameters_url
        url = url.replace(" ","")


        ### Check date query coherence

        if df_subset['endRange'].item() < df_subset['startRange'].item():
            print("ERROR: endRange < startRange")
            break


        ### Loop until reached desired number of pages or max nb of pages to fetch for the query
        
        while (pageNumber + 1 <= page_max) & (pageNumber + 1 <= max_page_to_fetch): 
            
            json_to_make_root = f"afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}"
            json_to_make = json_to_make_root + f"_{pageNumber}.json"
            
            
            
            df_totalPages = df_subset['totalPages']
            df_item = df_subset['totalPages'].item()
            df_values = df_subset['totalPages'].values
            
            
            if (json_to_make in json_list)|(json_to_make+'.gz' in json_list)  : # skip current query if corresponding json already present
                if(df_item == ''):
                    file_to_open = [file for file in json_list if json_to_make in file][0]
                    
                    if '.gz' in file_to_open:
                        with  gzip.open(f"{path_data_storage}/" + file_to_open) as json_file:
                            data = json.load(json_file)
                        
                    else:
                        with  open(f"{path_data_storage}/" + file_to_open) as json_file:
                            data = json.load(json_file)
                    page_max =  data['page']['totalPages']
                    df_subset.loc[0,['totalPages']] = page_max
                    

                    
            
            if (json_to_make in json_list)|(json_to_make+'.gz' in json_list)  : # skip current query if corresponding json already present
                
                
                print(Fore.BLUE +f"Page {pageNumber} : skipped because already retrieved")
                print(page_max)
                print(pageNumber)
                
                
                if (page_max == pageNumber + 1) :
                
                
                    print(Fore.BLUE +f"All pages already retrieved")
                    
                    df_subset.loc[0,['nb_of_pages_already_retrieved']] = df_subset.loc[0,['totalPages']].item()
                    df_subset.loc[0,['completion']] = 100
                    df_call_parameters_new = pd.concat([df_call_parameters_new,df_subset],ignore_index=True)
                    df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
                    df_call_parameters_new.fillna('').to_csv(path_call_parameter_file_folder+"/"+call_parameter_csv, index=0)
                pageNumber = pageNumber + 1
                
           
                
                continue
            
            df_totalPages = df_subset['totalPages']
            df_item = df_subset['totalPages'].item()
            df_values = df_subset['totalPages'].values
            
            current_total_pages = df_item

            

            

                
                


            API_key = API_key_list_cleaned[API_key_counter]
            

            headers['API-Key'] = API_key # API key is send in the request header
            

        

            url_page = (url + f"&{pageNumber=}").replace("?&","?") # Cleaning url from empty fileds

            response = requests.get(url_page, headers=headers)
            
            time.sleep(time_delay_query) # API limited to 1 call / s, 100 / day
                
            
            
            
            
            # print(f"Page found: {response.__bool__()}")
            
            no_more_api_key = ("Developer" in response.text) & (API_key_list_length == API_key_counter + 1)
            
            time_analysis = datetime.datetime.now().isoformat()
            df_subset.loc[0,['timestamp']] = time_analysis
            df_subset.loc[0,['call_parameters']] = call_parameters_url
            

            if  response.__bool__() : # True if response < 400
                
                data = response.json()
                
                page_max =  data['page']['totalPages'] # Update total number of pages
                fullCount =  data['page']['fullCount'] 
                
                if "json" in output_format:
                    with open(f"{path_data_storage}/afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json", 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)
                    
                if "gzip" in output_format:    
                    with gzip.open(f"{path_data_storage}/afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json.gz", 'wt', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)
                    

                
                print(Fore.GREEN +f"Page {pageNumber} : retrieval OK"+ Fore.RESET +f"    Total: {page_max} , Max: {max_page_to_fetch} ")
                

                if  df_subset['nb_of_pages_already_retrieved'].item() == '': # Check info already present in df_call_parameters.csv  

                    df_subset.loc[0,['nb_of_pages_already_retrieved']] = 0
                    
                if int(pageNumber +1) > int(df_subset['nb_of_pages_already_retrieved'].item()) : # Check info already present in df_call_parameters.csv  

                    
                    df_subset.loc[0,['nb_of_pages_already_retrieved']] = f"{(pageNumber+1):.0f}"
                    
                    
                    
                df_subset.loc[0,['response']] = str(response)
                df_subset.loc[0,['totalPages']] = f"{(page_max):.0f}" 
                df_subset.loc[0,['totalFlights']] = f"{(fullCount):.0f}" 
                
                df_subset.loc[0,['completion']] = f"{100*(pageNumber+1)/page_max:.0f}"
                df_subset.loc[0,['message']] = ""
                
                
                
                df_call_parameters_new = pd.concat([df_call_parameters_new,df_subset],ignore_index=True)
                df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=parameter_list, keep='last')
                df_call_parameters_new.fillna('').to_csv(path_call_parameter_file_folder+"/"+call_parameter_csv, index=0)

                
                pageNumber = pageNumber + 1
            
        
            elif ("Developer" in response.text)  & (API_key_list_length > API_key_counter + 1) : # Iterate of API key list to test the next one
                
                
                print(Fore.YELLOW + f"Trying next API key previous: {API_key}, next: {API_key_list_cleaned[API_key_counter+1]}")
                API_key_counter = API_key_counter + 1
                
                
            elif ("Developer" in response.text) :
                break
                
            else:
                
                print(Fore.RED + f"Issues with the call: {response} {response.text}")
            
                df_subset.loc[0,['response']] = str(response)
                df_subset.loc[0,['message']] = str(response.text)            
                
            
                df_call_parameters_new =pd.concat([df_call_parameters_new,df_subset],ignore_index=True)
                df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=['call_parameters'], keep='last')
                
                df_call_parameters_new.fillna('').to_csv(path_call_parameter_file_folder+"/"+call_parameter_csv, index=0)

                break
            
            
        if no_more_api_key:
            print(Fore.RED + "API keys all consumed")
            print(Style.RESET_ALL)
            break
        
        
'''



        
        ### Update with new dates when all pages of current parameter file retrived or failed
            
        df_call_parameters = pd.read_csv(path_call_parameter_file_folder+"/"+call_parameter_csv).fillna('')

        df_call_parameters_date_update = df_call_parameters.query('response == "<Response [200]>"').sort_values(['endRange'],ascending=False).groupby('call_parameters').head(1)
            
        if len(df_call_parameters_date_update.query('completion != "100"')) == 0:

            df_call_parameters_date_update['startRange']= df_call_parameters_date_update['endRange'].map(lambda x: ((datetime.datetime.fromisoformat(x) +datetime.timedelta(0,1)).isoformat(timespec='seconds') + "Z").replace("+00:00","") )
            df_call_parameters_date_update['endRange']= datetime.datetime.now().isoformat(timespec='seconds') + "Z"

            df_call_parameters_date_update = df_call_parameters_date_update.drop(non_parameters, axis=1)

            df_call_parameters_new = pd.concat([df_call_parameters, df_call_parameters_date_update],ignore_index=True).fillna('').sort_values(['startRange','endRange','response'])
            df_call_parameters_new.fillna('').to_csv(path_call_parameter_file_folder+"/"+call_parameter_csv, index=0)

'''                    
            
