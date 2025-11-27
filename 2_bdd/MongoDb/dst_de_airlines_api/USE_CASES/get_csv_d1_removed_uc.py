from datetime import datetime
from USE_CASES.get_d1_removed import get_d1_removed
from SERVICES.formater_service import format_json_flight_to_csv



def get_csv_d1_removed(date = None):
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_removed_sch_flight_from_mongo_filtered_{date_time}.csv.gz"
    flights = get_d1_removed(None, date)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    

    