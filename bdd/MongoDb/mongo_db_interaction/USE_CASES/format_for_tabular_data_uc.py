from REPOSITORIES.flights import get_all
import pandas as pd
from datetime import datetime
import gzip
import numpy as np
import io


def format_for_tabular_data(nb_flight_limit):
  
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")
    flights = get_all(nb_flight_limit)
    filename = f"afklm_flight_from_mongo_filtered_{date_time}_{nb_flight_limit}.csv.gz"


    df = pd.json_normalize(flights)
    df = df.map(lambda x: ', '.join(x) if isinstance(x, list) and x
                 else (np.nan if isinstance(x, list) else x))
    
    delayDuration_total_sum = []

    for item in  df[['flightLegs_irregularity_delayDuration']].to_numpy():
        sum_values = 0
      
        item = item[0]
        


        if type(item) is str:

            if "," not in item:
                sum_values = float(item)
        
        
            else:
                List = item.split(", ")
                for value in List:
                    value = float(value)
                    sum_values = sum_values + value
        
        else:
      
            sum_values = None



        delayDuration_total_sum.append(sum_values)



    df['flightLegs_irregularity_delayDuration_total']  = delayDuration_total_sum
    # for the api calling------------------------------------------
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    #--------------------------------------------------------------

    df.to_csv(filename, index = 0,na_rep = "",compression='gzip')
 
    # for the api calling------------------------------------------
    return csv_content, filename
    #--------------------------------------------------------------