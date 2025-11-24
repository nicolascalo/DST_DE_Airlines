from REPOSITORIES.flights import get_all



def get_scheduled_flights(nb_flight_limit, date = None):
    collection_name = "scheduled_flights"
 
    scheduled_flights = get_all(nb_flight_limit, collection_name, date)
    return scheduled_flights
  
