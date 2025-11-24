from datetime import datetime
from USE_CASES.get_sch_removed_uc import get_removed_sch_flights
from SERVICES.formater_service import format_json_flight_to_csv



def get_removed_sch_flights_to_csv(date = None):
    print("grosse pute")
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_removed_sch_flight_from_mongo_filtered_{date_time}.csv.gz"
    flights = get_removed_sch_flights(None, date)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    

    