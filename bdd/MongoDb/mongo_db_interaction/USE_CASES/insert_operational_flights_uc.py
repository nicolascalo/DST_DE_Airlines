from .insert_flight_uc import insert_flight


def insert_operation_fly(json_file):

    for flight in json_file['operationalFlights']:
        insert_flight(flight)







