import pandas as pd
import numpy as np



def format_json_flight_to_csv(flights):

    
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

    #--------------------------------------------------------------
   

    return df
