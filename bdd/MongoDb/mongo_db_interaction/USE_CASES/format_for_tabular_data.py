from REPOSITORIES.flights import get_all
import pandas as pd
from datetime import datetime


def format_for_tabular_data(nb_flight_limit):

    timestamp = datetime.now().strftime("%Y%m%d-%H-%M-%S")
    filename = f"afklm_flight_from_mongo_filtered_{timestamp}.csv"

    flights = get_all(nb_flight_limit)
    df = pd.json_normalize(flights)
    df.to_csv(filename)
    return df, filename
    