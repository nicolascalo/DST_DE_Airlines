'''
Written by Nicolas Calo

Script for data mining on the AIr France KLM ""https://api.airfranceklm.com/opendata/flightstatus/" API.

This script requires an afklm_api_keys.txt file containing the personal API keys (one on each line) to use to querry the API.

An additional df_call_parameters.csv file can be provided to set up the parameters to querry. This csv should contain a dataframe precising all the combinations of values needed to be retrived (one querry for each row of the datafrane)

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
from colorama import Fore, Back, Style
import gzip


### Script parameters

max_page_to_fetch = 1
pageNumberStart = 0
skip_previously_failed = False
page_max = 100000 # Will automatically be adjusted after having retrived the fist page 
refresh_stats = False
time_delay_query = 1.5 # API limited to 1 call / s, 100 / day
output_format = ["json","gzip"] # ["json","gzip"]

### List of already retrieved data

json_list = os.listdir("data")




### Setting up working directory

if  bool(re.search("afklm_api_collection",os.getcwd())) == False:
    os.chdir("1_data_collection/afklm_api_collection") 


if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


### Creation of folders for retrieved data

if not os.path.isdir("data"):
    os.mkdir("data")





### Import query parameters
 
df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')



### Refresh stats completion

if refresh_stats:
    
    json_list_no_gzip = [val for val in json_list if 'gzip' not in val]
    
    df_json_list_no_gzip = pd.DataFrame({"index":range(0,len(json_list_no_gzip)), 'json_file':json_list_no_gzip})
    
    
    json_param_list = [re.sub("afklm_api_data_collection_","",json) for json in json_list_no_gzip]
    json_param_list = [re.sub("Z_.*","Z",json) for json in json_param_list]
    json_param_list = [re.sub("_",":",json) for json in json_param_list]
    
    df_json_list_no_gzip['call_parameters'] = json_param_list
    
    
    json_pages = [re.sub(".*_","",json) for json in json_list_no_gzip]
    json_pages = [re.sub("\\..*","",json) for json in json_pages]
    df_json_list_no_gzip['page'] = json_pages
    
    df_retrived_nb = df_json_list_no_gzip['call_parameters'].value_counts().rename_axis('call_parameters').reset_index(name='nb_of_pages_already_retrieved')
    df_retrived_nb['nb_of_pages_already_retrieved'] = df_retrived_nb['nb_of_pages_already_retrieved'].astype('int')
    
    max_page_list = []
    totalFlights_list = []
    
    df_single_file_list = df_json_list_no_gzip.groupby('call_parameters').head(1).copy(deep= True)
    
    for json_file in df_single_file_list['json_file'].values:
        
        
         with open("data/" + json_file) as json_file:
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
    
    df_call_parameters_updated['completion'] = (100 * df_call_parameters_updated['nb_of_pages_already_retrieved'].values  / df_call_parameters_updated['totalPages'].values ).round(0)
    df_call_parameters_updated['completion'] = df_call_parameters_updated['completion'].astype('int').astype('str').replace(".0","")+"%"
    
    
    df_call_parameters_updated['response'] = '<Response [200]>'
    
    pd.concat([df_call_parameters, df_call_parameters_updated]).drop_duplicates(keep='last',subset='call_parameters').fillna('').to_csv("df_call_parameters.csv")
    df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')





### Loading API keys to use

with open(f"./afklm_api_keys.txt", "r") as f:
    API_key_list =  f.read().split("\n")
    
API_key_list_cleaned = []

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



if os.path.isfile("df_call_parameters.csv"):
    df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')

i = 0
API_key_counter = 0 # To use the first API key from the list

no_more_api_key = False

df_call_parameters_new = df_call_parameters.copy(deep=True) # to update the df_call_parameters csv after each querry












### loop over the csv file containing the parameter list to send to the API

for i in range(0, len(df_call_parameters)): 
    
    print("")
    
    df_subset = df_call_parameters.iloc[[i]].copy(deep = True).reset_index().drop(['index'], axis = 1) # parameters for the current querry
    print(Fore.RESET + f"{df_subset['call_parameters'].item()}")
    
    pageNumber = pageNumberStart	 #	integer<int32>	Indicates the page number you are requesting, the first real page is page 1. Page 0 gets the same results than page 1. If it's not provided first page will be returned		1	


    ### Check if query parameter already tested and skip previous failed if chosen in the script options
    
    text_response = str(df_subset['response'])
    match_error  = re.search("\\d\\d\\d",text_response)
    if match_error is None:
        match_error = "000"
    else:
        match_error = match_error[0]
        
    if skip_previously_failed & ( int(match_error)> 200):
        
        
        print(Fore.MAGENTA + f"skipped because previously failed")
        continue


    ### Cleaning of empty parameter calls
    
    non_parameters = ["call_parameters",'response', 'message', 'timestamp', 'nb_of_pages_already_retrieved', 'totalPages', 'completion']
    
    dict_call_parameters = df_subset.drop(non_parameters,axis=1).to_dict(orient="list")

    call_parameters_url = "&".join([key + "=" + str(val[0]) for key, val in dict_call_parameters.items()
                    if val[0] != '' and val[0] != '[nan]'])


    url = base_url + call_parameters_url
    url = url.replace(" ","")


    ### Check date query coherence

    if df_subset['endRange'].item() < df_subset['startRange'].item():
        print("ERROR: endRange < startRange")
        break


    


    ### Loop until reached desired number of pages or max nb of pages to fetch for the query
    
    while (pageNumber + 1 <= page_max) & (pageNumber + 1 <= max_page_to_fetch): 
        
        

        
        json_to_make = f"afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json"

        
        
        
        if (json_to_make in json_list)| (json_to_make in [file + ".gzip" for file in json_list] ) : # skip current query if corresponding json already present
            print(Fore.BLUE +f"Page {pageNumber} : skipped because already retrieved")
            
            
            

            pageNumber = pageNumber + 1
            continue

            

        API_key = API_key_list_cleaned[API_key_counter]
        

        headers['API-Key'] = API_key # API key is send in the request header
        

       

        url_page = (url + f"&{pageNumber=}").replace("?&","?") # Cleaning url from empty fileds

        response = requests.get(url_page, headers=headers)
        
        time.sleep(time_delay_query) # API limited to 1 call / s, 100 / day
               
        
        
        
        
        # print(f"Page found: {response.__bool__()}")
        
        no_more_api_key = (response.text == '<h1>Developer Over Rate</h1>') & (API_key_list_length == API_key_counter + 1)
        
        time_analysis = datetime.datetime.now().isoformat()
        df_subset.loc[0,['timestamp']] = time_analysis
        df_subset.loc[0,['call_parameters']] = call_parameters_url
        

        if  response.__bool__() : # True if response < 400
            
            data = response.json()
            
            page_max =  data['page']['totalPages'] # Update total number of pages
            fullCount =  data['page']['fullCount'] 
            
            if "json" in output_format:
                with open(f"data/afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json", 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                
            if "json" in output_format:    
                with gzip.open(f"data/afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json.gzip", 'wt', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                

            
            print(Fore.GREEN +f"Page {pageNumber} : retrieval OK"+ Fore.RESET +f"    Total: {page_max} , Max: {max_page_to_fetch} ")

            if  df_subset['nb_of_pages_already_retrieved'].item() == '': # Check info already present in df_call_parameters.csv  

                df_subset.loc[0,['nb_of_pages_already_retrieved']] = 0
                
            if int(pageNumber) > df_subset['nb_of_pages_already_retrieved'].item() : # Check info already present in df_call_parameters.csv  

                df_subset.loc[0,['nb_of_pages_already_retrieved']] =  pageNumber + 1
                
            df_subset.loc[0,['response']] = str(response)
            df_subset.loc[0,['totalPages']] = f"{(page_max):.0f}" 
            df_subset.loc[0,['totalFlights']] = f"{(fullCount):.0f}" 
            df_subset.loc[0,['nb_of_pages_already_retrieved']] = f"{(pageNumber+1):.0f}"
            df_subset.loc[0,['completion']] = f"{100*(pageNumber+1)/page_max:.0f}%"
            df_subset.loc[0,['message']] = ""
            
            
            df_call_parameters_new = pd.concat([df_call_parameters_new,df_subset],ignore_index=True)
            df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=['call_parameters'], keep='last')
            df_call_parameters_new.fillna('').to_csv("df_call_parameters.csv", index=0)

            
            pageNumber = pageNumber + 1
        
    
        elif (response.text == '<h1>Developer Over Rate</h1>') & (API_key_list_length > API_key_counter + 1) : # Iterate of API key list to test the next one
            
            
            print(Fore.YELLOW + f"Trying next API key previous: {API_key}, next: {API_key_list_cleaned[API_key_counter+1]}")
            API_key_counter = API_key_counter + 1
            
            
        elif (response.text == '<h1>Developer Over Rate</h1>'):
            break
            
        else:
            
            print(Fore.RED + f"Issues with the call: {response} {response.text}")
         
            df_subset.loc[0,['response']] = str(response)
            df_subset.loc[0,['message']] = str(response.text)            
            
           
            df_call_parameters_new =pd.concat([df_call_parameters_new,df_subset],ignore_index=True)
            df_call_parameters_new = df_call_parameters_new.drop_duplicates(subset=['call_parameters'], keep='last')
            
            df_call_parameters_new.fillna('').to_csv("df_call_parameters.csv", index=0)

            break
        
        
    if no_more_api_key:
        print(Fore.RED + "API keys all consumed")
        print(Style.RESET_ALL)
        break
    
    
### Update with new dates when all pages of current file retrived or failed
    
df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')

df_call_parameters_date_update = df_call_parameters.query('response == "<Response [200]>"').sort_values(['endRange'],ascending=False).groupby('call_parameters').head(1)
    
if len(df_call_parameters_date_update.query('completion != "100%"')) == 0:

    df_call_parameters_date_update['startRange']= df_call_parameters_date_update['endRange'].map(lambda x: ((datetime.datetime.fromisoformat(x) +datetime.timedelta(0,1)).isoformat(timespec='seconds') + "Z").replace("+00:00","") )
    df_call_parameters_date_update['endRange']= datetime.datetime.now().isoformat(timespec='seconds') + "Z"

    df_call_parameters_date_update = df_call_parameters_date_update.drop(non_parameters, axis=1)

    df_call_parameters_new = pd.concat([df_call_parameters, df_call_parameters_date_update],ignore_index=True).fillna('').sort_values(['startRange','endRange','response'])
    df_call_parameters_new.fillna('').to_csv("df_call_parameters.csv", index=0)


'''

### json to json.gzip

import shutil
for file in json_list:

    with open('data/'+file, 'rb') as f_in:
        with gzip.open("data/"+file+".gzip", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)



'''    
    
'''

        
### Stats pages to retrieve

### Setup 

df_json_pages_all = pd.DataFrame() # Initialize empty dataframe to store final values

### json file import

json_list = os.listdir("data") # Listing of json files to process

print("Starting json import")
for json_file_name in json_list:


    with open("data/" + json_file_name) as json_file:
        json_file = json.load(json_file)
        
    
    df = pd.json_normalize(json_file)
    df['request'] = re.sub("_\\d+\\.json","",json_file_name)
    df = df.drop('operationalFlights',axis = 1)
    
    df_json_pages_all = pd.concat([df_json_pages_all, df],ignore_index=True)
        

print("json import over")

df_json_pages_all_filtered = pd.merge(df_json_pages_all.groupby(['request']).agg({'page.pageNumber':'max'}).reset_index(), 
               df_json_pages_all, 
               on=['request']).drop(['page.pageNumber_y','page.pageCount','page.pageSize'], axis=1).drop_duplicates()


df_json_pages_all_filtered = df_json_pages_all_filtered.rename(columns={'page.pageNumber_x':'nb_of_pages_already_retrieved',
                                                                        'page.fullCount':'total_flights'})

df_json_pages_all_filtered.to_csv("afklm_api_data_collection_retrieval_count.csv")



'''
            