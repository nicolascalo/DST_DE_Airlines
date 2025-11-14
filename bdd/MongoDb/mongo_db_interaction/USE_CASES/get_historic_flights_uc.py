from mongo_db_interaction.REPOSITORIES.flights import get_all


def get_historic_flights(nb_flight_limit, date = None):
    collection_name = "historic_flights"

    historic_flight = get_all(nb_flight_limit, collection_name, date)
    return historic_flight



