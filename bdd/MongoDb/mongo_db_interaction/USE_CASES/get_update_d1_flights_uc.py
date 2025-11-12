from mongo_db_interaction.REPOSITORIES.flights import get_all


def get_update_scheduled_d1_flights(nb_flight_limit):
    collection_name = "update_scheduled_d1_flights"
    update_scheduled_d1_flights = get_all(nb_flight_limit, collection_name)
    return update_scheduled_d1_flights
