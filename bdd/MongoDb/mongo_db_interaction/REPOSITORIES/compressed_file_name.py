from DB_CONTEXT.db_context import mongo_db_connect


collection = 'compressed_file_names'

def insert_one(compressed_file_name):

    mongo_db_connect[collection].insert_one({"compressed_file_name":compressed_file_name})

def get_by_compressed_file_name(compressed_file_name):
    try:
        return mongo_db_connect[collection].find_one(
            {"compressed_file_name": compressed_file_name},
            projection={"compressed_file_name": 1, "_id": 0}
        )["compressed_file_name"]
    except (TypeError, KeyError):
        return None
    except Exception as e: 
        print(f"critical error : {e}")
        raise 




def count_compressed_file_name():
    return mongo_db_connect[collection].count_documents({})


