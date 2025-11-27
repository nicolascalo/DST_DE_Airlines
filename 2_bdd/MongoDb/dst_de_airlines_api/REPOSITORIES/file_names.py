from CONNECTION.db_context import mongo_db_connect
from CONNECTION.check_database_connection import check_db_connection


collection = 'file_names'

def insert_one(file_name):
    check_db_connection() 

    mongo_db_connect[collection].insert_one({"file_name":file_name})


def get_by_name(file_name):
    check_db_connection() 
    try:
        return mongo_db_connect[collection].find_one(
            {"file_name": file_name},
            projection={"file_name": 1, "_id": 0}
        )["file_name"]
    except (TypeError, KeyError):
        return None
    except Exception as e: 
        print(f"critical error : {e}")
        raise 
