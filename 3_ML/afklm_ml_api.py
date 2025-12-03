from fastapi import FastAPI, HTTPException
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
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
from sqlalchemy import create_engine
import traceback
import shutil
import glob
import subprocess
import os



OUTPUT_DIR = os.getenv('OUTPUT_DIR')
BEST_MODEL_DIR =  f'{OUTPUT_DIR}/best_models'




try:
    with open('./config/afklm_ml_training_settings.json') as json_file:
        ml_training_settings = json.load(json_file)
    params_from_json = True
    print("afklm_ml_training_settings_class.json loaded")

except:
    ml_training_settings = {
    "DATA_DIR" : 'data',
    "OUTPUT_DIR": "outputs"
    }
    print("afklm_ml_training_settings.json defaults loaded")

try :
    best_model_classification_delay_file =[model for model in list(os.listdir(BEST_MODEL_DIR)) if ('.pkl' in model) & ("classification_delay" in model) ][0]

    with open(f'{OUTPUT_DIR}/best_models/{best_model_classification_delay_file}', 'rb') as f:
        best_model_classification_delay = pickle.load(f)
except:
    print("No classification delay status model found")


try :
    best_model_classification_status_file =[model for model in list(os.listdir(BEST_MODEL_DIR)) if ('.pkl' in model) & ("classification_status" in model) ][0]
    with open(f'{OUTPUT_DIR}/best_models/{best_model_classification_status_file}', 'rb') as f:
        best_model_classification_status = pickle.load(f)
except:
    print("No classification delay model found")


try :
    best_model_regression_file =[model for model in list(os.listdir(BEST_MODEL_DIR)) if ('.pkl' in model) & ("regression" in model) ][0]

    with open(f'{OUTPUT_DIR}/best_models/{best_model_regression_file}', 'rb') as f:
        best_model_regression = pickle.load(f)
except:
    print("No regression delay model found")


try:
    best_models_metrics = pd.read_csv(f'{OUTPUT_DIR}/best_models/best_models.csv')
    best_models_metrics_json = ([row.dropna().to_dict() for index,row in best_models_metrics.iterrows()])
except:
    print("No model metrics found")


def get_dayPeriod(x):
    if (x >= 6) & (x < 12):
        return "morning"
    elif (x >= 12) & (x < 18):
        return "afternoon"
    elif (x >= 18) & (x < 24):
        return "evening"
    else:
        return 'night'

api = FastAPI(
        title = "Air France KLM - ML API",

    description = "REST API for managing, training and querying the machine learning pipelines that aim at prediction delay on flights from the Air France - KLM API ",
    docs_url = "/",

    openapi_tags=[
    {
        'name': 'tests',
        'description': 'Utility functions'
    },
    {
        'name': 'training',
        'description': 'functions regarding model training'
    },
    {
        'name': 'predictions',
        'description': 'functions regarding model training'
    }
])

class Payload_flight(BaseModel):
    model_config = {
    "extra": "allow",
    "json_schema_extra": 
    
    
    {
            "examples": []
               
            
        }
    
    }

    """"Parameters of the flight for which to predict delay"""
    flight_id	: Optional[str] = None
    flightnumber	: Optional[int] = None 
    airline_code	: Optional[str] = None
    airline_name	: Optional[str] = None
    flightlegs_aircraft_typecode	: Optional[str] = None
    flightlegs_servicetypename	: Optional[str] = None
    flightlegs_depinfo_airport_continent_name	: Optional[str] = None
    flightlegs_depinfo_airport_subcontinent_name	: Optional[str] = None
    flightlegs_depinfo_airport_country_code	: Optional[str] = None
    flightlegs_depinfo_airport_country_name	: Optional[str] = None
    flightlegs_depinfo_airport_airport_name	: Optional[str] = None
    flightlegs_depinfo_airport_code	: str
    flightlegs_depinfo_airport_places_depposterm_boardingterminal	: Optional[str] = None
    flightlegs_depinfo_airport_places_depposterm_gatenumber	: Optional[str] = None
    flightlegs_depinfo_times_scheduled_date	: str
    flightlegs_depinfo_times_scheduled_time	: str
    flightlegs_depinfo_times_scheduled_year	: Optional[int] = None 
    flightlegs_depinfo_times_scheduled_month	: Optional[int] = None 
    flightlegs_depinfo_times_scheduled_day	: Optional[int] = None 
    flightlegs_depinfo_times_scheduled_hour	: Optional[int] = None 
    flightlegs_depinfo_times_scheduled_minute	: Optional[int] = None 
    flightlegs_depinfo_times_scheduled_timezone	: str
    flightlegs_depinfo_times_number_week	: Optional[int] = None 
    flightlegs_arrinfo_airport_continent_name	: Optional[str] = None
    flightlegs_arrinfo_airport_subcontinent_name	: Optional[str] = None
    flightlegs_arrinfo_airport_country_code	: Optional[str] = None
    flightlegs_arrinfo_airport_country_name	: Optional[str] = None
    flightlegs_arrinfo_airport_airport_name	: Optional[str] = None
    flightlegs_arrinfo_airport_code	: str
    flightlegs_arrinfo_airport_places_arrivalpositionterminal	: Optional[str] = None
    flightlegs_arrinfo_times_scheduled_date	: str
    flightlegs_arrinfo_times_scheduled_time	: str
    flightlegs_arrinfo_times_scheduled_year	: Optional[int] = None 
    flightlegs_arrinfo_times_scheduled_month	: Optional[int] = None 
    flightlegs_arrinfo_times_scheduled_day	: Optional[int] = None 
    flightlegs_arrinfo_times_scheduled_hour	: Optional[int] = None 
    flightlegs_arrinfo_times_scheduled_minute	: Optional[int] = None 
    flightlegs_arrinfo_times_scheduled_timezone	: str
    flightlegs_arrinfo_times_number_week	: Optional[int] = None 




with open('./config/api_test_payload.json') as json_file:
    json_example_payload = json.load(json_file)
'''
try:
    with open('./config/api_test_payload.json') as json_file:
        json_example = json.load(json_file)

    
except:
    json_example =  {"_id":"690c974f228ea0580c98a8be","id":"20250517+G3+7612","airline_code":"G3","airline_name":"GOL LINHAS AEREAS S.A.","flightLegs_aircraft_ownerAirlineCode":"G3","flightLegs_aircraft_typeCode":"7M8","flightLegs_arrivalInformation_airport_city_country_areaCode":"I","flightLegs_arrivalInformation_airport_city_country_code":"AR","flightLegs_arrivalInformation_airport_city_country_name":"ARGENTINA","flightLegs_arrivalInformation_airport_code":"COR","flightLegs_arrivalInformation_airport_location_latitude":-31.3131,"flightLegs_arrivalInformation_airport_location_longitude":-64.1994,"flightLegs_arrivalInformation_times_scheduled":"2025-05-17T02:20:00.000-03:00","flightLegs_departureInformation_airport_city_country_areaCode":"I","flightLegs_departureInformation_airport_city_country_code":"BR","flightLegs_departureInformation_airport_city_country_name":"BRAZIL","flightLegs_departureInformation_airport_code":"GIG","flightLegs_departureInformation_airport_location_latitude":-22.8214,"flightLegs_departureInformation_airport_location_longitude":-43.2494,"flightLegs_departureInformation_airport_places_departurePositionTerminal_gateNumber":"","flightLegs_departureInformation_times_scheduled":"2025-05-16T22:30:00.000-03:00",
                "flightLegs_scheduledFlightDuration":"PT3H50M",
                 "flightLegs_serviceType":"J","flightLegs_serviceTypeName":"Normal Service","flightNumber":7612,"flightLegs_departureInformation_airport_places_departurePositionTerminal_boardingTerminal":"",
                 "flightLegs_arrivalInformation_airport_places_arrivalPositionTerminal":""}

'''


Payload_flight.model_config["json_schema_extra"]["examples"] = json_example_payload



class PayloadTrainingParameters(BaseModel):
    model_config = {
    "extra": "allow",
    "json_schema_extra": 
    
    
    {
            "examples": []
               
            
        }
    
    }

    """"Parameters for ML training"""


    DATA_FILE_ROOT_NAME : str
    RUN_MODE : str
    FILTER_AIRPORTS_OPTIONAL : str
    FILTER_AIRPORTS_MANDATORY : str
    TOP_K_FEATURES : int
    GRID_LEVEL : str
    PARALLEL_JOBS : int
    TEST_SIZE : float
    RANDOM_STATE : int
    RECORD_LIMIT : str
    MODEL_TO_KEEP : str
    TARGET_REGRESSION : str
    TARGET_CLASSIFICATION_STATUS : str
    TARGET_CLASSIFICATION_DELAY : str
    MODEL_LIST_TO_TEST : str
    CV_NB : int
    columnKeywordsToDrop_all : str
    columnKeywordsToKeep_classification_status : str
    columnKeywordsToDrop_classification_status : str
    columnKeywordsToDrop_classification_delay : str
    columnKeywordsToDrop_regression : str
    columnsToDrop_classification_status : str
    columnsToDrop_classification_delay : str
    columnsToDrop_regression : str


try:
    with open('./config/afklm_ml_training_settings_default.json') as json_file:
        json_example_training_params = json.load(json_file)
except:
    json_example_training_params = {"training parameters not found":""}


PayloadTrainingParameters.model_config["json_schema_extra"]["examples"] = [json_example_training_params]





@api.get('/health', name="Check if the API is running", tags=['tests'])
def get_index():
    """Check if the API is running"""
    return 1


@api.get('/model_parameters_and_metrics', name="Retrieve the prediction model training parameters and validation metrics", tags=['predictions'])
def get_model_parameters():
    """Retrieve the prediction model training parameters and test metrics of the current best models for status and delay"""
    return JSONResponse(content=best_models_metrics_json) 


@api.get('/retrain_models', name="Retrain the machine learning models with the current dataset", tags=['training'],response_class=PlainTextResponse)
def retrain_models():
    """Retrain the machine learning models with the current dataset"""
    subprocess.run(["python", "afklm_ml_training.py"])
    return "Model training over"

@api.get('/retrieve_latest_data_for_training', name="Retrieve the latest training dataset", tags=['training'],response_class=PlainTextResponse)
def retrieve_latest_training_dataset():

    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_URI')
    port = os.getenv('POSTGRES_PORT')
    database_name = os.getenv('POSTGRES_DB')

    DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"




    query = 'select v_past_flight.flight_id,  v_past_flight.flightNumber,  v_past_flight.airline_code,  v_past_flight.airline_name,  v_past_flight.flightStatusPublic,  v_past_flight.flightLegs_aircraft_typeCode,  v_past_flight.flightLegs_scheduledFlightDuration,  v_past_flight.flightLegs_serviceType,  v_past_flight.flightLegs_aircraft_ownerAirlineCode,  v_past_flight.flightLegs_status,  v_past_flight.delay_status,  v_past_flight.flightLegs_serviceTypeName, v_past_flight.flightLegs_publishedStatus, v_past_flight.flightLegs_legStatusPublic, v_past_flight.flightLegs_statusName, v_past_flight.flightLegs_irregularity_delayDuration, v_past_flight.flightlegs_irregularity_delayduration_total, v_past_flight.flightLegs_irregularity_delayInfo_delayReasonPublicLong, v_past_flight.flightLegs_irregularity_delayInfo_delayReasonPublicShort, v_geod.flightLegs_depInfo_airport_Continent_Name,  v_geod.flightLegs_depInfo_airport_Subcontinent_Name,  v_geod.flightLegs_depInfo_airport_Country_Code,  v_geod.flightLegs_depInfo_airport_Country_Name,  v_geod.flightLegs_depInfo_airport_Location_name,  v_geod.flightLegs_depInfo_airport_Airport_Name,  v_geod.flightLegs_depInfo_airport_Icao_Code,  v_geod.flightLegs_depInfo_airport_Latitude,  v_geod.flightLegs_depInfo_airport_Longitude, v_past_flight.flightLegs_depInfo_airport_code, v_past_flight.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, v_past_flight.flightLegs_depInfo_airport_places_depPosTerm_gateNumber, v_past_flight.flightLegs_depInfo_times_scheduled_date, v_past_flight.flightLegs_depInfo_times_scheduled_time, v_past_flight.flightLegs_depInfo_times_scheduled_year, v_past_flight.flightLegs_depInfo_times_scheduled_month, v_past_flight.flightLegs_depInfo_times_scheduled_day , v_past_flight.flightLegs_depInfo_times_scheduled_hour, v_past_flight.flightLegs_depInfo_times_scheduled_minute, v_past_flight.flightLegs_depInfo_times_scheduled_timezone, v_past_flight.flightLegs_depInfo_times_number_week, v_geoa.flightLegs_arrInfo_airport_Continent_Name,  v_geoa.flightLegs_arrInfo_airport_Subcontinent_Name,  v_geoa.flightLegs_arrInfo_airport_Country_Code,  v_geoa.flightLegs_arrInfo_airport_Country_Name,  v_geoa.flightLegs_arrInfo_airport_Location_name,  v_geoa.flightLegs_arrInfo_airport_Airport_Name,  v_geoa.flightLegs_arrInfo_airport_Icao_Code,  v_geoa.flightLegs_arrInfo_airport_Latitude,  v_geoa.flightLegs_arrInfo_airport_Longitude, v_past_flight.flightLegs_arrInfo_airport_code, v_past_flight.flightLegs_arrInfo_airport_places_arrivalPositionTerminal, v_past_flight.flightLegs_arrInfo_times_scheduled_date, v_past_flight.flightLegs_arrInfo_times_scheduled_time, v_past_flight.flightLegs_arrInfo_times_scheduled_year, v_past_flight.flightLegs_arrInfo_times_scheduled_month, v_past_flight.flightLegs_arrInfo_times_scheduled_day, v_past_flight.flightLegs_arrInfo_times_scheduled_hour, v_past_flight.flightLegs_arrInfo_times_scheduled_minute, v_past_flight.flightLegs_arrInfo_times_scheduled_timezone, v_past_flight.flightLegs_arrInfo_times_number_week from v_past_flight v_past_flight 	INNER JOIN v_geod v_geod ON v_geod.flightLegs_depInfo_airport_Iata_Code = v_past_flight.flightLegs_depInfo_airport_code 	INNER JOIN v_geoa v_geoa ON v_geoa.flightLegs_arrInfo_airport_Iata_Code = v_past_flight.flightLegs_arrInfo_airport_code ;'



    #df = pd.read_csv('afklm_flight_from_mongo_filtered_20251113-21-36-51_test.csv', low_memory=False)
    try:
        engine = create_engine(DATABASE_URL)

        def get_sql_data(query):
            df = pd.read_sql(query, engine)
            return df

        try:
            df = get_sql_data(query)

        except Exception as e:
            full_trace = traceback.format_exc()
            print(full_trace)  # shows full stack trace in Docker logs

            raise HTTPException(
                status_code=500,
                detail=f"Database query failed: {str(e)}"
            )

        df.to_csv('./data/afklm_past_flight.csv.zip', compression='zip', index=False)
        print("PostgreSQL data retrieved")

    except Exception as e:
        full_trace = traceback.format_exc()
        print(full_trace)

        raise HTTPException(
            status_code=500,
            detail=f"Database connection error: {str(e)}"
        )


@api.get('/training_parameters_show', name="", tags=['training'])
def get_training_parameters():
    """Get current parameters for model training"""
    return JSONResponse(ml_training_settings)


@api.post('/training_parameters_upload', name="", tags=['training'],response_class=PlainTextResponse)
def set_training_parameters(parameters: PayloadTrainingParameters):
    """Set current parameters for model training"""
    new_params = parameters.model_dump()
    
    with open(f"./config/afklm_ml_training_settings.json", 'w', encoding='utf-8') as f:
        params =json.dump(new_params, f, ensure_ascii=False, indent=4)

    return("Training parameters updated")

@api.get('/training_parameters_defaults', name="", tags=['training'],response_class=PlainTextResponse)
def set_training_parameters_to_default():
    """Reset model parameters to default"""
    shutil.copyfile("./config/afklm_ml_training_settings_default.json","./config/afklm_ml_training_settings.json")
    return "Training paramters reset to defaults"


@api.get('/display_training_run_list', name="", tags=['training_outputs'],response_class=JSONResponse)
def list_training_logs():
    """Display the list of training runs"""
    try :
        log_list = glob.glob('./*/*/*ML.log', recursive=True)
        log_list.sort(reverse=True)
        log_list = [re.sub(".*/","",file) for file in log_list]
        log_list = [re.sub("_ML\\..*","",file) for file in log_list]
        log_list=dict(enumerate(log_list))
        print(log_list)
        return log_list
    except:

        HTTPException(status_code=404, detail="No log found")


@api.get('/display_last_training_log', name="", tags=['training_outputs'],response_class=PlainTextResponse)
def display_training_log():
    """Display the log of the last training"""
    try :
        log_list = glob.glob('./*/*/*ML.log', recursive=True)
        log_list.sort(reverse=True)
        last_log_file = log_list[0]
        with open(last_log_file, 'r') as f:
            last_log = f.read()
        return last_log
    except:

        HTTPException(status_code=404, detail="No log found")

@api.get('/download_chosen_training_run', name="", tags=['training_outputs'])
def download_chosen_training_run(run:str):
    """Downloads the chosen training run. Will not include any model as only the best models are kept."""
    try :
        file_name = f"{run}"
        file_path = f"./outputs/{run}"
        shutil.make_archive(f"./outputs/{file_name}", 'zip', file_path)


        return FileResponse(path=f"./outputs/{file_name}.zip", filename=f"{file_name}"+'.zip', media_type='application/zip')
    except:

        HTTPException(status_code=404, detail="No run found for this name")




@api.get('/download_best_models', name="", tags=['training_outputs','predictions'])
def download_chosen_training_run():
    """Downloads the best overall models."""
    try :
        file_name = "afklm_ml_best_models"
        file_path = f"./outputs/best_models"
        shutil.make_archive(f"./outputs/{file_name}", 'zip', file_path)


        return FileResponse(path=f"./outputs/{file_name}.zip", filename=f"{file_name}"+'.zip', media_type='application/zip')
    except:

        HTTPException(status_code=404, detail="No run found for this name")





@api.post('/get_delay_predictions', name = "Get flight delay predictions", tags=['predictions'])
def post_users(parameters: Payload_flight):
    """"Interrogates the best status (ONTIME, LATE, CANCELLED) and delay models (delay duration in minutes) based on the flight parameters parameters. Will only perform delay duration prediction if the flight is predicted to be late, and will otherwise output NA"""
    
    # Convert Pydantic model to dict
    data_dict = parameters.model_dump()

    # Convert dict to DataFrame
    entry = pd.DataFrame([data_dict])

    # --------------------------------------
    # DATA CLEANING
    # --------------------------------------

    entry["company_flight"] = entry["flight_id"].apply(lambda x: re.sub("^.*?\\+","", x))


    entry_cleaned = entry
    entry_cleaned['flightlegs_arrinfo_times_scheduled'] = entry_cleaned.apply(
        lambda row:row.flightlegs_arrinfo_times_scheduled_date+"T"+row.flightlegs_depinfo_times_scheduled_time+".000"+row.flightlegs_arrinfo_times_scheduled_timezone
                    ,
        axis=1
    )

    entry_cleaned['flightlegs_depinfo_times_scheduled'] = entry_cleaned.apply(
        lambda row: row.flightlegs_depinfo_times_scheduled_date+"T"+row.flightlegs_depinfo_times_scheduled_time+".000"+row.flightlegs_depinfo_times_scheduled_timezone
                    ,
        axis=1
    )


    entry_cleaned["flightlegs_scheduledFlightDuration"] = entry_cleaned.apply(
        lambda row: (datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled)
                    - datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled)).seconds / 60,
        axis=1
    )


    # ----------------------
    # FEATURE ENGINEERING
    # ----------------------



    season_dictionary = {1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall',12:'winter'}




    entry_cleaned['flightlegs_season'] = entry_cleaned.apply(
        lambda row: season_dictionary[(datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).month)]
                    ,
        axis=1
    )

    entry_cleaned['flightlegs_arrinfo_times_scheduled_isWeekend'] = entry_cleaned.apply(
        lambda row: True if datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled).isoweekday() in [6,7] else False
                    ,
        axis=1
    )

    entry_cleaned['flightlegs_arrinfo_times_scheduled_dayPeriod'] = entry_cleaned.apply(
        lambda row: get_dayPeriod(datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled).hour + datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled).minute/60)
                    ,
        axis=1
    )


    entry_cleaned['flightlegs_depinfo_times_scheduled_isWeekend'] = entry_cleaned.apply(
        lambda row: True if datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).isoweekday() in [6,7] else False
                    ,
        axis=1
    )

    entry_cleaned['flightlegs_depinfo_times_scheduled_dayPeriod'] = entry_cleaned.apply(
        lambda row: get_dayPeriod(datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).hour + datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).minute/60)
                    ,
        axis=1
    )



    try:

        prediction_status = best_model_classification_status.predict(entry_cleaned)
        prediction_status = prediction_status[0]

    except Exception as e:
        prediction_status = "No model found for delay status"
        raise RuntimeError(f"STATUS MODEL ERROR: {e}")            

    if prediction_status == "LATE":

        try:
            prediction_delay_classification = best_model_classification_delay.predict(entry_cleaned)
            prediction_delay_classification = prediction_delay_classification[0]
        except Exception as e:
            prediction_delay_regresssion = "No model found for delay duration classification"
            raise RuntimeError(f"STATUS MODEL ERROR: {e}")            
        try:
            prediction_delay_regresssion = best_model_regression.predict(entry_cleaned)
            prediction_delay_regresssion = prediction_delay_regresssion[0].item()
        except Exception as e:
            prediction_delay_regresssion = "No model found for delay duration regression"
            raise RuntimeError(f"STATUS MODEL ERROR: {e}")            


    else:
        prediction_delay_regresssion = "NA"
        prediction_delay_classification = "NA"
    
    return JSONResponse(content={"predicted_flightLeg_status": prediction_status,
                                 "predicted_delay_min_classification": prediction_delay_classification,
                                 "predicted_delay_min_regression": prediction_delay_regresssion
    })


