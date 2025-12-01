from CONNECTION.db_context import mongo_db_connect, client
from CONNECTION.check_database_connection import check_db_connection
from datetime import datetime
import gc
from pymongo import UpdateOne

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
    
    print("deleting duplicates")
    pipeline = [
        {"$group": {"_id": "$id", "count": {"$sum": 1}, "docs": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(mongo_db_connect[collection_name].aggregate(pipeline))
        
    total_deleted = 0
    total_docs_to_delete = []
    for dup in duplicates:
    
        docs_to_delete = dup["docs"][1:] 
        total_docs_to_delete.extend(docs_to_delete)
    batch_size = 10000 
    for i in range(0, len(total_docs_to_delete), batch_size):
        batch = total_docs_to_delete[i:i + batch_size]
        result = mongo_db_connect[collection_name].delete_many(
            {"_id": {"$in": batch}}
        )
        total_deleted += result.deleted_count
        print(f"nb duplicates deleted : {total_deleted}")
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

    batch_size = 1000

    if flights_to_remove:
        for i in range(0, len(flights_to_remove), batch_size):
            batch = flights_to_remove[i:i + batch_size]
            operations = [
                UpdateOne(
                    {"id": flight["id"]},
                    {"$setOnInsert": flight},
                    upsert=True
                )
                for flight in batch
            ]
            if operations:
                mongo_db_connect['removed_update_scheduled_d1_flights'].bulk_write(operations)
   
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
    batch_size = 1000

    if flights_to_remove:
        for i in range(0, len(flights_to_remove), batch_size):
            batch = flights_to_remove[i:i + batch_size]
            operations = [
                UpdateOne(
                    {"id": flight["id"]},
                    {"$setOnInsert": flight},
                    upsert=True
                )
                for flight in batch
            ]
            if operations:
                mongo_db_connect['removed_scheduled_flights'].bulk_write(operations)

    deleting = mongo_db_connect['scheduled_flights'].delete_many(query)
    print(f"nb scheduled flights deleted : {deleting.deleted_count}")

    # Nettoyage mémoire
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


   


