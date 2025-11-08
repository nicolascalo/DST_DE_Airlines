from mongo_db_interaction.DB_CONTEXT.db_context import mongo_db_connect

def get_all_collection_name():
    return mongo_db_connect.list_collection_names()