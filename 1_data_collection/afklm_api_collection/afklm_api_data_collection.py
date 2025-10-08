import pandas as pd
import requests

import pprint
import re
import time
import json
import datetime
import os

if not os.path.isdir("data"):
    os.mkdir("data")

API_key = "ect462qphynr9znnq4zn9fy5"
API_secret = "9xay0N3nuK"


base_url ="https://api.airfranceklm.com/opendata/flightstatus/?"
headers = {"API-Key": API_key,'Content-Type': 'application/x-www-form-urlencoded'}



aircraftRegistration  = ''	#	string	Registration code of the aircraft		PHBEF	
aircraftType = ''	#	string	Filter by a type of aircraft		737	
arrivalCity = ''	#	string	Filter by airport code of arrival city		DXB	
carrierCode = []	#	array[string]	Airline code (2-pos IATA and 3-pos ICAO)		KL, AF	
consumerHost = ''	#	string	The information about the system from which the request is launched		KL	
departureCity = ''	#	string	IATA departure city code		DXB	
destination	= '' #	string	Destination airport		AMS	
flightNumber = ''	#	string	Filter by flight number		202	
movementType = ''	#	string	Focus (Departure or Arrival) for the flights to be found; used for selection of departure or arrival time within range **Sorting is based on movementType A = Arrival, D = Departure If D- flights will be sorted by scheduleDeparture time If A- flights will be sorted by scheduleArrivaltime	AD		
operatingAirlineCode = []	#	array[string]	Operating airline code (2-pos IATA and 3-pos ICAO)		KL	
operationalSuffix = ''	#	string	Operational suffix, indicates if a flight has been advanced or delayed to the previous or next day	AD	D,A,R,S,T,U,V,W	
origin = ''	#	string	Departure airport		AMS	

serviceType = []	#	array[string]	IATA service type code		J	
timeOriginType = ''	#	string	S = Scheduled, M = Modified, I = Internal, P = Public	SMIP	S	
timeType = ''	#	string	Type of time used in startRange and endRange U = UTC time, L = Local Time	UL	U	
endRange = '2025-07-23T23:59:59Z'	#	string<date-time>	End on this date time		2023-12-31T23:59:59.000Z	required
startRange = '2025-07-21T09:00:00Z'	#	string<date-time>	Start from this date time		2023-12-31T09:00:00.000Z	required
carrierCode = ",".join(carrierCode)
operatingAirlineCode = ",".join(operatingAirlineCode)
serviceType = ",".join(serviceType)




pageNumber = 1	#	integer<int32>	Indicates the page number you are requesting, the first page is page 0. If it's not provided first page will be returned		1	
pageSize = ''	#	integer<int32>	Indicates the number of items per page	null	100	


params = [f"{aircraftRegistration=}", f"{aircraftType=}", f"{arrivalCity=}", f"{carrierCode=}", f"{consumerHost=}", f"{departureCity=}", f"{destination=}", f"{flightNumber=}", f"{movementType=}", f"{operatingAirlineCode=}", f"{operationalSuffix=}", f"{origin=}",  f"{pageSize=}" , f"{serviceType=}", f"{timeOriginType=}", f"{timeType=}", f"{endRange=}", f"{startRange=}"]

params_not_null = [re.sub("\'","",param) for param in params if not (re.search("\'\'",param)) ]

call_parameters = "&".join(params_not_null)

url = base_url + call_parameters




while os.path.isfile(f"data/afklm_api_data_collection_{re.sub(":","_",call_parameters)}_{pageNumber}.json"):
    pageNumber = pageNumber + 1
    
    
    

url_page = (url + f"&{pageNumber=}").replace("?&","?")

page_max = 1000


print(f"API call parameters = {call_parameters}")


while pageNumber < page_max:
    
    print(f"Querrying page {pageNumber} / {page_max}")
    print(url_page)
    

    response = requests.get(url_page, headers=headers)



    time.sleep(1.5) # API limited to 1 call / s, 100 / day
    
    
    print(f"Page found: {response.__bool__()}")


    if  response.__bool__() : # True if response < 400

        data = response.json()
        
        page_max =  data['page']['totalPages']
        
        json_str = json.dumps(data, indent=4)
        
        # current_time = datetime.datetime.now()
        # current_time = f"{current_time.year}-{current_time.month}-{current_time.day}-{current_time.hour}h{current_time.minute}m{current_time.second}s"
        
        with open(f"data/afklm_api_data_collection_{re.sub(":","_",call_parameters)}_{pageNumber}.json", "w") as f:
            f.write(json_str)
        
        
        
    else:
        print(f"Page nb {pageNumber} not found")

        print("Aborting the API call")
        
        break
    
    pageNumber = pageNumber + 1
    url_page = (url + f"&{pageNumber=}").replace("?&","?")
    print("")
        

