from REPOSITORIES.flights import get_all



def get_d1_removed(nb_flight_limit, date = None):
    collection_name = "update_scheduled_d1_flights"

    get_removed_d1_flights = get_all(nb_flight_limit, collection_name, date)
    return get_removed_d1_flights



