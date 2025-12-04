from CONNECTION.db_context import mongo_db_connect, client
from CONNECTION.check_database_connection import check_db_connection
from datetime import datetime
import gc

collection = "operation_flights"
flight_colleciton = "flights"

def insert_many(operation_flights, collection_name):
    check_db_connection() 
    
    mongo_db_connect[collection_name].insert_many(operation_flights, ordered=False)
    gc.collect()



def insert_one(operation_flights, collection_name):
    check_db_connection() 
    
    mongo_db_connect[collection_name].insert_one(operation_flights)
    gc.collect


def delete_duplicates(collection_name):
    check_db_connection()
    print("delete duplicates")
    
    seen_flight_ids = set()
    
    for op_flights in mongo_db_connect[collection_name].find():
        operational_flights = op_flights.get('operationalFlights', [])
        
        if not operational_flights:
            continue
 
        unic_flights = []
        
        for flight in operational_flights:
            flight_id = flight.get('id')
            
            if flight_id and flight_id not in seen_flight_ids:
                seen_flight_ids.add(flight_id)
                unic_flights.append(flight)
        
        if len(unic_flights) < len(operational_flights):
            mongo_db_connect[collection_name].update_one(
                {'_id': op_flights['_id']},
                {'$set': {'operationalFlights': unic_flights}}
            ) 
    gc.collect()
        


def move_to_dst_collection(org_collection, dst_collection):
    check_db_connection() 
    print("move to collection "+dst_collection)
    mongo_db_connect[org_collection].aggregate([
            {"$unwind": "$operationalFlights"},  
            {"$replaceRoot": {"newRoot": "$operationalFlights"}},
            {
                "$merge": {
                    "into": dst_collection,
                    "whenMatched": "replace", 
                    "whenNotMatched": "insert"  
                }
            }
        ])
    gc.collect()
    
def delete_all_opreation_flights_collection(collection_name):
    check_db_connection() 
    print("drop")
    mongo_db_connect[collection_name].drop()
    gc.collect()



def remove_past_flights_on_d1_collection():
    check_db_connection() 
    query = {
        "$expr": {
            "$lt": [
                {
                    "$toDate": {
                        "$arrayElemAt": ["$flightLegs.arrivalInformation.times.latestPublished", 0]
                    }
                },
                datetime.now()
            ]
        }
    }
        
    flights_to_remove = list(mongo_db_connect['update_scheduled_d1_flights'].find(query))

    if flights_to_remove:
         for flight in flights_to_remove:
                mongo_db_connect['removed_update_scheduled_d1_flights'].update_one(
                {"id": flight["id"]},
                {"$setOnInsert": flight},
                upsert=True
            )
   
    deleting = mongo_db_connect['update_scheduled_d1_flights'].delete_many(query)

    print(f"nb d1 flights deleted : {deleting.deleted_count}")
    gc.collect()
    return deleting.deleted_count

def remove_past_flights_on_scheduled_collection():
    check_db_connection() 
    query = {
   
          "$expr": {
            "$lt": [
                {
                    "$toDate": {
                        "$arrayElemAt": ["$flightLegs.arrivalInformation.times.latestPublished", 0]
                    }
                },
                datetime.now()
            ]
        }
    }
    flights_to_remove = list(mongo_db_connect['scheduled_flights'].find(query))
    if flights_to_remove:
        for flight in flights_to_remove:

            mongo_db_connect['removed_scheduled_flights'].update_one(
                {"id": flight["id"]},
                {"$setOnInsert": flight},
                upsert=True
            )
      
    deleting = mongo_db_connect['scheduled_flights'].delete_many(query)
    print(f"nb scheduled flights deleted : {deleting.deleted_count}")
    gc.collect()
    return deleting.deleted_count
    
def remove_duplicate_flights_from_scheduled():
    check_db_connection() 
    
 
    ids_to_remove = mongo_db_connect['update_scheduled_d1_flights'].distinct("id")
    

    result = mongo_db_connect['scheduled_flights'].delete_many({
        "id": {"$in": ids_to_remove}
    })
    print(f"nb duplicate scheduled_flights deleted : {result.deleted_count}")
    gc.collect()
    return result.deleted_count


   


