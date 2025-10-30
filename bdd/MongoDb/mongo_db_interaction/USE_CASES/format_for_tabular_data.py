from REPOSITORIES.flights import get_all
import pandas as pd
from datetime import datetime
import numpy as np


def format_for_tabular_data(nb_flight_limit):
    

    flights = get_all(nb_flight_limit)
    df = pd.json_normalize(flights)
    df = df.applymap(lambda x: ', '.join(x) if isinstance(x, list) and x
        else (np.nan if isinstance(x, list) else x))
    delayDuration_total = df['flightLegs_irregularity_delayDuration'].str.split(", ").to_list()
    delayDuration_total_sum = []
    for item in delayDuration_total:
        if type(item) is list:
            sum_values = 0
        for value in item:
            value = float(value)
            sum_values = sum_values + value
        else:
            sum_values = None
        delayDuration_total_sum.append(sum_values)
    df['flightLegs_irregularity_delayDuration_total']  = delayDuration_total_sum
    df.to_csv("afklm_flight_from_mongo_filtered.csv.gz", index = 0,na_rep = "",compression='gzip')
        


    