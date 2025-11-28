from CONNECTION.db_context import mongo_db_connect
from CONNECTION.check_database_connection import check_db_connection

collection = 'update_scheduled_d1_flights'
def get_all():
    check_db_connection() 
    mongo_db_connect[collection]
    