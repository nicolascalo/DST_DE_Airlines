from datetime import datetime
from mongo_db_interaction.USE_CASES.get_d1_removed import get_d1_removed
from mongo_db_interaction.SERVICES.formater_service import format_json_flight_to_csv



def get_d1_removed_to_csv(date = None):
    print("grosse pute")
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    filename = f"afklm_removed_sch_flight_from_mongo_filtered_{date_time}.csv.gz"
    flights = get_d1_removed(None, date)
    df = format_json_flight_to_csv(flights)
    
    return df, filename
    

    