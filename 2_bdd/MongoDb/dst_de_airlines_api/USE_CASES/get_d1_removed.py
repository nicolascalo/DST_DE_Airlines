from REPOSITORIES.flights import get_flights_by_id



def get_d1_removed(nb_flights, id=None, date = None):
    collection_name = "update_scheduled_d1_flights"

    get_removed_d1_flights = get_flights_by_id(nb_flights, collection_name, id, date)
    return get_removed_d1_flights



