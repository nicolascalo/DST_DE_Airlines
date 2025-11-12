from datetime import datetime
from mongo_db_interaction.USE_CASES.get_all_flights import get_all_flights
from mongo_db_interaction.SERVICES.formater_service import format_json_flight_to_csv



def get_all_flights_to_csv():
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_flight_from_mongo_filtered_{date_time}.csv.gz"
    flights = get_all_flights()
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    

    
