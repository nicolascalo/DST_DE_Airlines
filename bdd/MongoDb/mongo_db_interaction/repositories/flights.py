from ..db_context.db_context import mongo_db_connect

collection = "flights"

def insert_one(flight):
    mongo_db_connect[collection].insert_one(flight)



def get_by_id(id):
    mongo_db_connect[collection].find_one({"operationalFlights.id":id})

def create_index():
    mongo_db_connect[collection].create_index([("operationalFlights.id", 1)], unique=True)