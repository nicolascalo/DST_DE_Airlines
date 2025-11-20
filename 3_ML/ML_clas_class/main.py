from fastapi import FastAPI
import os, re, datetime, logging
import pandas as pd
from typing import Optional
import datetime
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder, MinMaxScaler
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score, accuracy_score
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.tree import plot_tree
from xgboost import XGBRegressor
import pickle
import json
import re
from pydantic import BaseModel, create_model
from typing import Any
from fastapi.responses import JSONResponse

OUTPUT_DIR = "outputs"
BEST_MODEL_DIR =  f'{OUTPUT_DIR}/best_models'




try:
    with open('./config/afklm_ml_training_settings.json') as json_file:
        ml_training_settings = json.load(json_file)
    params_from_json = True
    
except:
    ml_training_settings = {
    "DATA_DIR" : 'data',
    "OUTPUT_DIR": "outputs"

    }



season_dictionary = {1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall',12:'winter'}

def get_dayPeriod(x):
    if (x >= 6) & (x < 12):
        return "morning"
    elif (x >= 12) & (x < 18):
        return "afternoon"
    elif (x >= 18) & (x < 24):
        return "evening"
    else:
        return 'night'

best_model_regression_file =[model for model in list(os.listdir(BEST_MODEL_DIR)) if ('.pkl' in model) & ("regression" in model) ][0]

with open(f'{OUTPUT_DIR}/best_models/{best_model_regression_file}', 'rb') as f:
    best_model_regression = pickle.load(f)

best_model_classification_file =[model for model in list(os.listdir(BEST_MODEL_DIR)) if ('.pkl' in model) & ("classification" in model) ][0]

with open(f'{OUTPUT_DIR}/best_models/{best_model_classification_file}', 'rb') as f:
    best_model_classification = pickle.load(f)

best_models_metrics = pd.read_csv(f'{OUTPUT_DIR}/best_models/best_models.csv')
best_models_metrics_json = ([row.dropna().to_dict() for index,row in best_models_metrics.iterrows()])



api = FastAPI()

class Payload_flight(BaseModel):
    model_config = {
    "extra": "allow",
    "json_schema_extra": {
            "examples": [
                {"_id":"690c974f228ea0580c98a8be","id":"20250517+G3+7612","airline_code":"G3","airline_name":"GOL LINHAS AEREAS S.A.","flightLegs_aircraft_ownerAirlineCode":"G3","flightLegs_aircraft_typeCode":"7M8","flightLegs_arrivalInformation_airport_city_country_areaCode":"I","flightLegs_arrivalInformation_airport_city_country_code":"AR","flightLegs_arrivalInformation_airport_city_country_name":"ARGENTINA","flightLegs_arrivalInformation_airport_code":"COR","flightLegs_arrivalInformation_airport_location_latitude":-31.3131,"flightLegs_arrivalInformation_airport_location_longitude":-64.1994,"flightLegs_arrivalInformation_times_scheduled":"2025-05-17T02:20:00.000-03:00","flightLegs_departureInformation_airport_city_country_areaCode":"I","flightLegs_departureInformation_airport_city_country_code":"BR","flightLegs_departureInformation_airport_city_country_name":"BRAZIL","flightLegs_departureInformation_airport_code":"GIG","flightLegs_departureInformation_airport_location_latitude":-22.8214,"flightLegs_departureInformation_airport_location_longitude":-43.2494,"flightLegs_departureInformation_airport_places_departurePositionTerminal_gateNumber":"","flightLegs_departureInformation_times_scheduled":"2025-05-16T22:30:00.000-03:00",
                "flightLegs_scheduledFlightDuration":"PT3H50M",
                 "flightLegs_serviceType":"J","flightLegs_serviceTypeName":"Normal Service","flightNumber":7612,"flightLegs_departureInformation_airport_places_departurePositionTerminal_boardingTerminal":"",
                 "flightLegs_arrivalInformation_airport_places_arrivalPositionTerminal":""}
            ]
        }
    
    }

    """"Parameters of the flight for which to predict delay"""
    id : Optional[str]
    airline_code : Optional[str]
    airline_name : Optional[str]
    flightlegs_aircraft_ownerairlinecode : Optional[str]
    flightlegs_aircraft_typecode : Optional[str]
    flightlegs_arrivalinformation_airport_city_country_areacode : Optional[str]
    flightlegs_arrivalinformation_airport_city_country_code : Optional[str]
    flightlegs_arrivalinformation_airport_city_country_name : Optional[str]
    flightlegs_arrivalinformation_airport_code : str
    flightlegs_arrivalinformation_airport_location_latitude : Optional[float]
    flightlegs_arrivalinformation_airport_location_longitude : Optional[float]
    flightlegs_arrivalinformation_airport_places_arrivalpositionterminal : Optional[str]
    flightlegs_arrivalinformation_times_scheduled : str
    flightlegs_departureinformation_airport_city_country_areacode : Optional[str]
    flightlegs_departureinformation_airport_city_country_code : Optional[str]
    flightlegs_departureinformation_airport_city_country_name : Optional[str]
    flightlegs_departureinformation_airport_code : str
    flightlegs_departureinformation_airport_location_latitude : Optional[float]
    flightlegs_departureinformation_airport_location_longitude : Optional[float]
    flightlegs_departureinformation_airport_places_depposterm_boardingterminal : Optional[str]
    flightlegs_departureinformation_airport_places_depposterm_gatenumber : Optional[str]
    flightlegs_departureinformation_times_scheduled : str
    flightlegs_servicetype : Optional[str]
    flightlegs_servicetypename : Optional[str]
    flightnumber : Optional[int]
    _id : Optional[str]
    airline_code : Optional[str]
    airline_name : Optional[str]
    flightlegs_aircraft_ownerairlinecode : Optional[str]
    flightlegs_aircraft_typecode : Optional[str]
    flightlegs_arrinfo_airport_city_country_areacode : Optional[str]
    flightlegs_arrinfo_airport_city_country_code : Optional[str]
    flightlegs_arrinfo_airport_city_country_name : Optional[str]
    flightlegs_arrinfo_airport_code : str
    flightlegs_arrinfo_airport_location_latitude : Optional[float]
    flightlegs_arrinfo_airport_location_longitude : Optional[float]
    flightlegs_arrinfo_times_scheduled : str
    flightlegs_depinfo_airport_city_country_areacode : Optional[str]
    flightlegs_depinfo_airport_city_country_code : Optional[str]
    flightlegs_depinfo_airport_city_country_name : Optional[str]
    flightlegs_depinfo_airport_code : str
    flightlegs_depinfo_airport_location_latitude : Optional[str]
    flightlegs_depinfo_airport_location_longitude : Optional[str]
    flightlegs_depinfo_airport_places_depposterm_gatenumber : Optional[str]
    flightlegs_depinfo_times_scheduled : str
    flightlegs_irregularity_delayduration : Optional[str]
    flightlegs_irregularity_delayinfo_delayreasonpubliclong : Optional[str]
    flightlegs_irregularity_delayinformation_delaycode : Optional[str]
    flightlegs_irregularity_delayinfo_delayreasonpublicshort : Optional[str]
    flightlegs_irregularity_delayreason : Optional[str]
    flightlegs_scheduledflightduration : Optional[str]
    flightlegs_servicetype : Optional[str]
    flightlegs_servicetypename : Optional[str]
    flightlegs_status : Optional[str]
    flightlegs_publishedstatus : str
    flightlegs_legstatuspublic : Optional[str]
    flightlegs_statusname : Optional[str]
    flightstatuspublic : Optional[str]
    flightlegs_arrinfo_times_estimated_value : Optional[str]
    flightlegs_arrinfo_times_latestpublished : Optional[str]
    flightlegs_depinfo_times_actual : Optional[str]
    flightlegs_depinfo_times_actualtakeofftime : Optional[str]
    flightlegs_depinfo_times_latestpublished : Optional[str]
    flightlegs_arrinfo_airport_places_arrivalpositionterminal : Optional[str]
    flightlegs_arrinfo_times_actual : Optional[str]
    flightlegs_arrinfo_times_actualtouchdowntime : Optional[str]
    flightlegs_depinfo_airport_places_depposterm_boardingterminal : Optional[str]
    flightlegs_irregularity_delayduration_total : str

    
    
    

@api.get('/verify', name="Check if the API is running")
def get_index():
    """Check if the API is running"""
    return {
        'message': "The API is running"
    }


@api.get('/model_parameters_and_metrics', name="Retrieve the prediction model training parameters and validation metrics")
def get_model_parameters():
    """Retrieve the prediction model training parameters and test metrics of the current best models for classification and regression"""
    return JSONResponse(content=best_models_metrics_json) 


@api.post('/get_delay_predictions', name = "Get flight delay predictions")
def post_users(parameters: Payload_flight):
    """"Interrogates the best classification (on_time, late, cancelled) and regression models (delay duration in minutes) based on the flight parameters parameters. Will only perform delay duration prediction if the flight is predicted to be late, and will otherwise output NA"""
    
    # Convert Pydantic model to dict
    data_dict = parameters.model_dump()

    # Convert dict to DataFrame
    entry = pd.DataFrame([data_dict])


    entry["company_flight"] = entry["id"].apply(lambda x: re.sub("^.*?\\+","", x))

    entry["scheduled"] = entry["flightLegs_arrivalInformation_times_scheduled"].apply(
        lambda x: datetime.datetime.fromisoformat(x).date() > datetime.datetime.now().date()
    ) # To change for filtering on flightleg_publicstatus


    entry["flightLegs_scheduledFlightDuration"] = entry.apply(
        lambda row: (datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled)
                    - datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled)).seconds / 60,
        axis=1
    )

    entry = entry[~entry["scheduled"]].copy()

    entry['flightLegs_season'] = entry.apply(
        lambda row: season_dictionary[(datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled).month)]
                    ,
        axis=1
    )

    entry['flightLegs_arrivalInformation_times_scheduled_isWeekend'] = entry.apply(
        lambda row: True if datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled).isoweekday() in [6,7] else False
                    ,
        axis=1
    )

    entry['flightLegs_arrivalInformation_times_scheduled_dayPeriod'] = entry.apply(
        lambda row: get_dayPeriod(datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled).hour + datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled).minute/60)
                    ,
        axis=1
    )
    entry['flightLegs_departureInformation_times_scheduled_dayPeriod'] = entry.apply(
        lambda row: get_dayPeriod(datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled).hour + datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled).minute/60)
                    ,
        axis=1
    )


    entry['flightLegs_departureInformation_times_scheduled_isWeekend'] = entry.apply(
        lambda row: True if datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled).isoweekday() in [6,7] else False
                    ,
        axis=1
    )

    prediction_classification = best_model_classification.predict(entry)
    prediction_classification = prediction_classification[0]
    

    if prediction_classification == "late":
        prediction_regression = best_model_regression.predict(entry)
        prediction_regression = prediction_regression[0]
    else:
        prediction_regression = "NA"
    
    return JSONResponse(content={"predicted_flightLeg_status": prediction_classification,
                                 "predicted_delay_min": prediction_regression,
    })


