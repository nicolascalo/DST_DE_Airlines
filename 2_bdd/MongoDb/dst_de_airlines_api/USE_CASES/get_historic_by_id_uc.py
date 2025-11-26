from REPOSITORIES.flights import get_by_id 

def get_historic_by_id(id):
    collection_name = "historic_flights"
    return get_by_id(id, collection_name)