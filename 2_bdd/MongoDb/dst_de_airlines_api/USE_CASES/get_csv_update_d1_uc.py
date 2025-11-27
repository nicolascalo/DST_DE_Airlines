from datetime import datetime
from USE_CASES.get_update_d1_uc import get_update_scheduled_d1
from SERVICES.formater_service import format_json_flight_to_csv




def get_csv_update_d1_by_id(nb_flights, id, date):
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")
    filename = f"afklm_update_d1_flight_from_mongo_filtered_{date_time}_{nb_flights}.csv.gz"
    flights = get_update_scheduled_d1(nb_flights, id, date)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    