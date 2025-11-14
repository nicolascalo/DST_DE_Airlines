from mongo_db_interaction.REPOSITORIES.flights import get_all



def get_removed_sch_flights(nb_flight_limit, date = None):
    collection_name = "removed_scheduled_flights"

    get_removed_sch_flights = get_all(nb_flight_limit, collection_name, date)
    return get_removed_sch_flights



