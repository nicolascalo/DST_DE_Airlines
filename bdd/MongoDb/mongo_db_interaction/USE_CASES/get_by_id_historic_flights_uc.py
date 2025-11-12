from mongo_db_interaction.REPOSITORIES.flights import get_by_id 

def get_by_id_historic_flight(id):
    collection_name = "historic_flights"
    return get_by_id(id, collection_name)