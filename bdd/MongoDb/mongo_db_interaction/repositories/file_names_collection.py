from ..db_context.db_context import mongo_db_connect


collection = 'file_name_collections'

def insert_one(file_name_collection):

    mongo_db_connect[collection].insert_one(file_name_collection)

def get_by_name(file_name):
    result =  mongo_db_connect[collection].find_one({"file_name":file_name})

    return result