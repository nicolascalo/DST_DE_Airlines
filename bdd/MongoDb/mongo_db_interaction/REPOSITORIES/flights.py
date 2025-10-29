from DB_CONTEXT.db_context import mongo_db_connect

collection = "flights"

def insert_one(flight):
    mongo_db_connect[collection].insert_one(flight)



def get_by_id(id):
    try:
        return mongo_db_connect[collection].find_one(
            {"id": id}
        )
    except (TypeError, KeyError):
        return None
    except Exception as e: 
        print(f"critical error : {e}")
        raise 
    

  

def create_index():
    mongo_db_connect[collection].create_index([("id", 1)], unique=True)


def count_flight():
    return mongo_db_connect[collection].count_documents({})
