from mongo_db_interaction.USE_CASES.get_historic_uc import get_historic_flights
from mongo_db_interaction.USE_CASES.get_scheduled_uc import get_scheduled_flights
from mongo_db_interaction.USE_CASES.get_update_d1_uc import get_update_scheduled_d1_flights



def get_flights(date = None):
    historic_flights = get_historic_flights(None, date)
    scheduled_flights = get_scheduled_flights(None, date)
    update_scheduled_d1_flights = get_update_scheduled_d1_flights(None, date)

    flights = historic_flights + scheduled_flights + update_scheduled_d1_flights
    return flights
