from DAO.flights import get_flights_by_id



def get_flights(collection_name, date = None, id=None, nb_flights=None):

    if id != None:
        nb_flights = nb_flights + 1

    flights = get_flights_by_id(collection_name, date, id, nb_flights)
    if id is not None and flights:
        flights = flights[1:]

    return flights



