from datetime import datetime
from mongo_db_interaction.USE_CASES.get_scheduled_flights_uc import get_scheduled_flights
from mongo_db_interaction.SERVICES.formater_service import format_json_flight_to_csv




def get_schedulled_flights_to_csv(nb_limit_flights):
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_sched_flight_from_mongo_filtered_{date_time}_{nb_limit_flights}.csv.gz"
    flights = get_scheduled_flights(nb_limit_flights)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    