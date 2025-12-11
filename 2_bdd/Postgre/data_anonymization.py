import pandas as pd
import os
import re

PATH_DATA = "2_bdd/Postgre/data_input"

def shuffle_cols(folder_path:str):

    csv = os.listdir(PATH_DATA)
    csv = [file for file in csv if "from_mongo.csv" in file]


    for file in  csv:



        print(file)

        data = pd.read_csv(f"{PATH_DATA}/{file}",low_memory=False)
        cols_og = data.columns

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
                                                                                    
        data = data.loc[:, cols_og]

        data.to_csv(f"{PATH_DATA}/dummy_{file}", index=0)



if __name__ == "data_anonymization.py":

    shuffle_cols(PATH_DATA)