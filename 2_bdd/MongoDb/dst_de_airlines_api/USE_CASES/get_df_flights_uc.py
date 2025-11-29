from datetime import datetime
from USE_CASES.get_flights_uc import get_flights
from USE_CASES.get_by_id_uc import get_flight_by_id
from SERVICES.formater_service import format_json_flight_to_df



def get_df_flights(collection_name, date = None, id= None, nb_flights = None):
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_{collection_name}_from_mongo_filtered_{date_time}.csv"
    if id != None:
        get_flight_by_id(collection_name, id)

    flights = get_flights(collection_name, date, id, nb_flights)
    df = format_json_flight_to_df(flights)
    
    return df, filename
    

    