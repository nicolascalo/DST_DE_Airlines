from mongo_db_interaction.REPOSITORIES.flights import get_all

def get_historic_flights(nb_flight_limit):
    collection_name = "historic_flights"
    historic_flight = get_all(nb_flight_limit, collection_name)
    return historic_flight


