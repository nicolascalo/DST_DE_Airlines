from REPOSITORIES.flights import get_flights_by_id



def get_scheduled_flights(nb_flights, id, date = None):
    collection_name = "scheduled_flights"

    if id != None:
        nb_flights = nb_flights + 1
 
    scheduled_flights = get_flights_by_id(nb_flights, collection_name, id, date)
    if id is not None and scheduled_flights:
        scheduled_flights = scheduled_flights[1:]
    return scheduled_flights
  






