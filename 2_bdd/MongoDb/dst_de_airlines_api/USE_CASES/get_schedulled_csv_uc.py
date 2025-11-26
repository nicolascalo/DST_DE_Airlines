from datetime import datetime
from USE_CASES.get_scheduled_uc import get_scheduled_flights
from SERVICES.formater_service import format_json_flight_to_csv




def get_csv_schedulled_by_id(nb_flights, id, date):
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_sched_flight_from_mongo_filtered_{date_time}_{nb_flights}.csv.gz"
    flights = get_scheduled_flights(nb_flights, id, date)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    