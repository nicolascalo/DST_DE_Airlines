from REPOSITORIES.flights import get_flights_by_id



def get_update_scheduled_d1(nb_flights, id, date = None):
    collection_name = "update_scheduled_d1_flights"

    if id != None:
        nb_flights = nb_flights + 1
  
    update_scheduled_d1_flights = get_flights_by_id(nb_flights, collection_name, id, date)
    if id is not None and update_scheduled_d1_flights:
        update_scheduled_d1_flights = update_scheduled_d1_flights[1:]
    return update_scheduled_d1_flights

        



