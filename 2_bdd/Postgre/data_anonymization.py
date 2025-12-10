import pandas as pd
import os
import re

PATH_DATA = "2_bdd/Postgre/data_input"

def shuffle_cols(folder_path:str):



    for file in  os.listdir(PATH_DATA):

        print(file)

        data = pd.read_csv(f"{PATH_DATA}/{file}")

        col_groups = ['flightNumber',
                      'arrivalInformation_airport',
                      'departureInformation_airport',
                      'aircraft',
                      'airline']

        for group in col_groups:

            cols = [col for col in data.columns if group in col]

            data_shuffled = data[cols].sample(frac=1).reset_index(drop=True)
            data = data.drop(cols, axis=1)
            data = pd.concat([data, data_shuffled], axis=1)

    

    
    
    data.apply(
    lambda row: re.sub("T.*","",(row['flightLegs_departureInformation_times_scheduled'])) .replace("-","")  +"+"   + row['airline_code']+ "+"+ str(row['flightNumber']) ,            axis=1)
                                                                                 


    data.to_csv(f"{PATH_DATA}/{file}")



if __name__ == "data_anonymization.py":

    shuffle_cols(PATH_DATA)