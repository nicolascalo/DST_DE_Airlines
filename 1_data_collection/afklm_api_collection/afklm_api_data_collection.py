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

!!! Script limited to 3 pages / requests to limit API consumption
!!! Always define dates !!!!


'''
### Library import

import pandas as pd
import requests
import re
import time
import json
import os

### Creation of folders for retrieved data

if not os.path.isdir("data"):
    os.mkdir("data")


### Loading API keys to use

with open(f"afklm_api_keys.txt", "r") as f:
    API_key_list =  f.read().split("\n")

API_key_list_length = len(API_key_list)

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
"startRange": '2025-07-21T09:00:00Z'	#	string<date-time>	Start from this date time		2023-12-31T09":00":00.000Z	required
}



dict_call_parameters["carrierCode"] =",".join(dict_call_parameters['carrierCode'])
dict_call_parameters["operatingAirlineCode"] =",".join(dict_call_parameters['operatingAirlineCode'])
dict_call_parameters["serviceType"] =",".join(dict_call_parameters['serviceType'])


pageNumber = 1	 #	integer<int32>	Indicates the page number you are requesting, the first page is page 0. If it's not provided first page will be returned		1	
pageSize = ''	 #	integer<int32>	Indicates the number of items per page	null	100	-> Does not seem to do anything





df_call_parameters = pd.DataFrame(dict_call_parameters, index = [0])


if os.path.isfile("df_call_parameters.csv"):
    df_call_parameters = pd.read_csv("df_call_parameters.csv").fillna('')

i = 0


for i in range(0, len(df_call_parameters)): ### loop over the csv file containing the parameter list to send to the API
    
    API_key_counter = 0
    
    df_subset = df_call_parameters.iloc[[i]]

    ### Cleaning of empty parameter calls

    dict_call_parameters = df_subset.to_dict(orient="list")

    call_parameters_url = "&".join([key + "=" + str(val[0]) for key, val in dict_call_parameters.items()
                    if val[0] != '' and val[0] != '[nan]'])


    url = base_url + call_parameters_url
    url = url.replace(" ","")

    if df_subset['endRange'] < df_subset['startRange']:
        print("ERROR: endRange < startRange")
        break

    ### Checking of presence of already downloaded pages for the current parameter set and adjustment of the page number to fetch

    pageNumber = 0
    
    while os.path.isfile(f"data/afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json"):
        pageNumber = pageNumber + 1

    url_page = (url + f"&{pageNumber=}").replace("?&","?")

    page_max = 10000 # Temporary number of pages in the collection total until update after 1st API call

    print("")
    print(f"API call parameters: {call_parameters_url}")


    
    # print(f"{API_key_counter}")


    while pageNumber < page_max and pageNumber < 4: # pageNumber < 4 for testing purposes only and not to consume to quickly the API call daily limit
        
        print(f"Querrying page {pageNumber} / {page_max}")
        print(url_page)
        
        API_key = API_key_list[API_key_counter]
        headers['API-Key'] = API_key
        print(f"{API_key = }")
        
        response = requests.get(url_page, headers=headers)

        time.sleep(1.5) # API limited to 1 call / s, 100 / day
        
        
        url_page = (url + f"&{pageNumber=}").replace("?&","?")
        print(f"Page found: {response.__bool__()}")

        if  response.__bool__() : # True if response < 400

            data = response.json()
            
            page_max =  data['page']['totalPages'] # Update total number of pages
            
            json_str = json.dumps(data, indent=4)
            
            with open(f"data/afklm_api_data_collection_{re.sub(":","_",call_parameters_url)}_{pageNumber}.json", "w") as f:
                f.write(json_str)
                
            pageNumber = pageNumber + 1
        
        elif API_key_list_length > API_key_counter + 1 : # Iterate of API key list to test the next one
            
            API_key_counter = API_key_counter + 1
            print(response)
            print("Trying next API key\n")
            
        else:
            
            print(f"Provided API keys could not retrieve anymore any pages / issues in the parameters of the call")
            print(response)
            break
           

            
        
        
      
            

