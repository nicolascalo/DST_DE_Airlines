from datetime import datetime
from USE_CASES.get_all_flights import get_flights
from SERVICES.formater_service import format_json_flight_to_csv



def get_flights_to_csv(date = None):
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_flight_from_mongo_filtered_{date_time}.csv.gz"
    flights = get_flights(date)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    

    
