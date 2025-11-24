from pprint import pprint
from pymongo import MongoClient
import re
import pandas as pd
import numpy as np



### MongoDB settings

client = MongoClient(
    host="127.0.0.1",
    port = 27017,
    username = "airlines",
    password = "airlines"
)


database_name = "airlines"
collection_name = "flights"

### query setting

flight_nb_limit = 5000



### DB check

'''print(client.list_database_names())




pprint(flights.find_one())

pprint(flights.count_documents({}))

pprint(flights.find_one())

query_results =     list(
   flights.find({"id":'20250821+AF+7313'})
)

pprint(
    query_results
)'''


db = client[database_name]

'''db.list_collection_names()'''
flights = db[collection_name]

### Formating for tabular data


query_results =     list(
    flights.aggregate([
#     {
#         "$match": {"id":'20250821+AF+7467'} 

#       },
                {
           "$unwind":"$flightLegs"
        },
        {
          "$project": {
              
"id":"$id",
"airline-code":"$airline.code",
"airline-name":"$airline.name",
"flightLegs-aircraft-ownerAirlineCode":"$flightLegs.aircraft.ownerAirlineCode",
"flightLegs-aircraft-typeCode":"$flightLegs.aircraft.typeCode",
"flightLegs-arrivalInformation-airport-city-country-areaCode":"$flightLegs.arrivalInformation.airport.city.country.areaCode",
"flightLegs-arrivalInformation-airport-city-country-code":"$flightLegs.arrivalInformation.airport.city.country.code",
"flightLegs-arrivalInformation-airport-city-country-name":"$flightLegs.arrivalInformation.airport.city.country.name",
"flightLegs-arrivalInformation-airport-code":"$flightLegs.arrivalInformation.airport.code",
"flightLegs-arrivalInformation-airport-location-latitude":"$flightLegs.arrivalInformation.airport.location.latitude",
"flightLegs-arrivalInformation-airport-location-longitude":"$flightLegs.arrivalInformation.airport.location.longitude",
"flightLegs-arrivalInformation-airport-places-arrivalPositionTerminal":"$flightLegs.arrivalInformation.airport.places.arrivalPositionTerminal",
"flightLegs-arrivalInformation-times-actual":"$flightLegs.arrivalInformation.times.actual",
"flightLegs-arrivalInformation-times-actualTouchDownTime":"$flightLegs.arrivalInformation.times.actualTouchDownTime",
"flightLegs-arrivalInformation-times-estimated-value":"$flightLegs.arrivalInformation.times.estimated.value",
"flightLegs-arrivalInformation-times-latestPublished":"$flightLegs.arrivalInformation.times.latestPublished",
"flightLegs-arrivalInformation-times-scheduled":"$flightLegs.arrivalInformation.times.scheduled",
"flightLegs-departureInformation-airport-city-country-areaCode":"$flightLegs.departureInformation.airport.city.country.areaCode",
"flightLegs-departureInformation-airport-city-country-code":"$flightLegs.departureInformation.airport.city.country.code",
"flightLegs-departureInformation-airport-city-country-name":"$flightLegs.departureInformation.airport.city.country.name",
"flightLegs-departureInformation-airport-code":"$flightLegs.departureInformation.airport.code",
"flightLegs-departureInformation-airport-location-latitude":"$flightLegs.departureInformation.airport.location.latitude",
"flightLegs-departureInformation-airport-location-longitude":"$flightLegs.departureInformation.airport.location.longitude",
"flightLegs-departureInformation-airport-places-departurePositionTerminal-boardingTerminal":"$flightLegs.departureInformation.airport.places.boardingTerminal",
"flightLegs-departureInformation-airport-places-departurePositionTerminal-gateNumber":"$flightLegs.departureInformation.airport.places.gateNumber",
"flightLegs-departureInformation-times-actual":"$flightLegs.departureInformation.times.actual",
"flightLegs-departureInformation-times-actualTakeOffTime":"$flightLegs.departureInformation.times.actualTakeOffTime",
"flightLegs-departureInformation-times-latestPublished":"$flightLegs.departureInformation.times.latestPublished",
"flightLegs-departureInformation-times-scheduled":"$flightLegs.departureInformation.times.scheduled",
"flightLegs-irregularity-delayDuration":"$flightLegs.irregularity.delayDuration",
"flightLegs-irregularity-delayInformation-delayReasonPublicLong":"$flightLegs.irregularity.delayInformation.delayReasonPublicLong",
"flightLegs-irregularity-delayInformation-delayReasonPublicShort":"$flightLegs.irregularity.delayInformation.delayReasonPublicShort",
"flightLegs-irregularity-delayReason":"$flightLegs.irregularity.delayReason",
"flightLegs-scheduledFlightDuration":"$flightLegs.scheduledFlightDuration",
"flightLegs-serviceType":"$flightLegs.serviceType",
"flightLegs-status":"$flightLegs.status",
"flightNumber":"$flightNumber",
"flightStatusPublic":"$flightStatusPublic"

              }
        },
        {"$limit":flight_nb_limit}         
        ]
    )
)

'''pprint(
    query_results
)'''





### Formating for tabular data


df = pd.json_normalize(query_results)
df = df.map(lambda x: ', '.join(x) if isinstance(x, list) and x
                 else (np.nan if isinstance(x, list) else x))

delayDuration_total_sum = []
print(df[['flightLegs-irregularity-delayDuration']].to_numpy())
for item in  df[['flightLegs-irregularity-delayDuration']].to_numpy():
    sum_values = 0
    print(item)
    item = item[0]
    print(item)


    if type(item) is str:

        if "," not in item:
            print("not in virgul")
            sum_values = float(item)
    
    
        else:
            print("split")
            list = item.split(", ")
            print(list)
            for value in list:
                value = float(value)
                sum_values = sum_values + value
    
    else:
        print("item is None")
        sum_values = None



    delayDuration_total_sum.append(sum_values)



df['flightLegs-irregularity-delayDuration-total']  = delayDuration_total_sum
df.to_csv("afklm_flight_from_mongo_filtered.csv.gz", index = 0,na_rep = "",compression='gzip')