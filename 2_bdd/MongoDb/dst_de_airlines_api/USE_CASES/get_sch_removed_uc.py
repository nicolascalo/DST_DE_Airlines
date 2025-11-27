from REPOSITORIES.flights import get_flights_by_id



def get_removed_sch(nb_flights, id= None, date = None):
    collection_name = "removed_scheduled_flights"

    get_removed_sch_flights = get_flights_by_id(nb_flights, collection_name, id, date)
    return get_removed_sch_flights



