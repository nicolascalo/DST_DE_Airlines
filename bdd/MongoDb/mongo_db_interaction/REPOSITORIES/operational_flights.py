from DB_CONTEXT.db_context import mongo_db_connect


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
        


def move_to_flight_collection(org_collection, dst_collection):

 print("move to flights collection")
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
   


