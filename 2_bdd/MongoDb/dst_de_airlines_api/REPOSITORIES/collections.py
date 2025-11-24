from DB_CONTEXT.db_context import mongo_db_connect
from DB_CONTEXT.check_database_connection import check_db_connection

def get_all_collection_name():
    check_db_connection()
    return mongo_db_connect.list_collection_names()