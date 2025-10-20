from ..repositories.flights import insert_one, get_by_id

def insert_flight(flight):
    id = flight['id']
    print(id)
    if is_flight_exit(id) == False:
        insert_one(flight)


def is_flight_exit(id):
    flight_id = get_by_id(id)
    if flight_id == None:
        return False
    else:
        return True






