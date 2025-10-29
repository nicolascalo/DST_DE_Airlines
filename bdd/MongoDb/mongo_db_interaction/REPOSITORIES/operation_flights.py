from DB_CONTEXT.db_context import mongo_db_connect


collection = "operation_flights"
flight_colleciton = "flights"

def insert_one(operation_flights):
    
    mongo_db_connect[collection].insert_one(operation_flights)



def delete_duplicates():
    print("deleting duplicates")
    pipeline = [
        {"$group": {"_id": "$id", "count": {"$sum": 1}, "docs": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(mongo_db_connect[flight_colleciton].aggregate(pipeline))
        
    total_deleted = 0
    for dup in duplicates:
    
        docs_to_delete = dup["docs"][1:] 
        result = mongo_db_connect[flight_colleciton].delete_many(
            {"_id": {"$in": docs_to_delete}}
        )
        total_deleted += result.deleted_count
        


def move_to_flight_collection():

 print("move to flights collection")
 mongo_db_connect[collection].aggregate([
        {"$unwind": "$operationalFlights"},  
        {"$replaceRoot": {"newRoot": "$operationalFlights"}},
        {
            "$merge": {
                "into": flight_colleciton,
                "whenMatched": "replace", 
                "whenNotMatched": "insert"  
            }
        }
    ])
    
def delete_all_opreation_flights_collection():
   mongo_db_connect[collection].drop()
   


