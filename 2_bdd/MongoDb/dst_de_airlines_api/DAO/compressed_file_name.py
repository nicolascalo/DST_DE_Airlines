from CONNECTION.db_context import mongo_db_connect
from fastapi import HTTPException 
from CONNECTION.check_database_connection import check_db_connection
import gc


collection = 'compressed_file_names'

def insert_many(compressed_file_name):
    check_db_connection() 

    mongo_db_connect[collection].insert_many(compressed_file_name)
    gc.collect()

def insert_one(compressed_file_name):
    check_db_connection() 

    mongo_db_connect[collection].insert_one({"compressed_file_name":compressed_file_name})
    gc.collect

def get_by_compressed_file_name(compressed_file_name):
    check_db_connection() 
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


def get_all_compressed_file_names():
    check_db_connection()
    try:

        return mongo_db_connect[collection].find()
    
    except (TypeError, KeyError):
        return None
    except Exception as e: 
        print(f"critical error : {e}")
        raise 


        



def count_compressed_file_name():
    check_db_connection() 
    try:
        return mongo_db_connect[collection].count_documents({})
    except (TypeError, KeyError):
        return None
    except HTTPException:  
        raise
    except Exception as e:
        print(f"critical error : {e}")
        raise


