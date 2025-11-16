from DB_CONTEXT.db_context import mongo_db_connect

collection = 'update_scheduled_d1_flights'
def get_all():
    mongo_db_connect[collection]
    