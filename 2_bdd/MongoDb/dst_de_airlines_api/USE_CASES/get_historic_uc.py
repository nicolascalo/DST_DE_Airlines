from REPOSITORIES.flights import get_flights_by_id


def get_historic_flights(nb_flights, id, date):
    collection_name = "historic_flights"

    if id != None:
        nb_flights = nb_flights + 1

    historic_flights = get_flights_by_id(nb_flights, collection_name, id, date)
    if id is not None and historic_flights:
        historic_flights = historic_flights[1:]
    return historic_flights



