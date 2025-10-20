from .insert_flight_uc import insert_flight
from ..services.folder_exploration import get_folder_path_in_env




def insert_operation_fly(json_file):

    for flight in json_file['operationalFlights']:
        insert_flight(flight)







