from mongo_db_interaction.DB_CONTEXT.db_context import mongo_db_connect
from datetime import datetime

collection = "operation_flights"
flight_colleciton = "flights"

def insert_one(operation_flights, collection_name):
    
    mongo_db_connect[collection_name].insert_one(operation_flights)



def delete_duplicates(collection_name):
    print("deleting duplicates")
    pipeline = [
        {"$group": {"_id": "$id", "count": {"$sum": 1}, "docs": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(mongo_db_connect[collection_name].aggregate(pipeline))
        
    total_deleted = 0
    for dup in duplicates:
    
        docs_to_delete = dup["docs"][1:] 
        result = mongo_db_connect[collection_name].delete_many(
            {"_id": {"$in": docs_to_delete}}
        )
        total_deleted += result.deleted_count
        


def move_to_dst_collection(org_collection, dst_collection):
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
    
def delete_all_opreation_flights_collection(collection_name):
    print("drop")
    mongo_db_connect[collection_name].drop()



def remove_past_flights_on_d1_collection():
    result = mongo_db_connect['update_scheduled_d1_flights'].delete_many({
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
    })
    print(f"nb d1 flights deleted : {result.deleted_count}")
    return result.deleted_count
    
def remove_duplicate_flights_from_scheduled():
    
 
    ids_to_remove = mongo_db_connect['update_scheduled_d1_flights'].distinct("id")
    

    result = mongo_db_connect['scheduled_flights'].delete_many({
        "id": {"$in": ids_to_remove}
    })
    print(f"nb scheduled_flights deleted : {result.deleted_count}")
    return result.deleted_count


   


