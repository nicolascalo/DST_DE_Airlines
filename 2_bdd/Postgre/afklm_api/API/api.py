from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os
from USE_CASES.download_data import download_data
from fastapi import FastAPI, HTTPException
import os, re, datetime
import pandas as pd
from typing import Optional
import datetime
import json
import re
from typing import Any
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
import shutil
from dotenv import load_dotenv
import psycopg2
import subprocess

load_dotenv()


folder_path = os.getenv('DATA_INPUT')



app = FastAPI(
    title="Air France KLM - PostgreSQL API",
    description = "REST API for updating the afklm PostgreSQL database with the latest data from MongoDB",
    version="1.0.0",
    docs_url="/"
)


@app.get('/health', name="Check if the API is running", tags=['tests'],response_class=PlainTextResponse)
def get_index():
    """Check if the API is running"""
    return "The API is running"




@app.get("/download_historic_flights",
         description=f"Download a file named afklm_historic_from_mongo.csv.tar.gz in {folder_path}", tags=['training']
)
def download_historic_flights():
    

    route = "historic/export"

    file_name = "afklm_historic_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="historic flights not found")
    return {'message': file_name + 'saved in' + path}



@app.get("/download_update_scheduled_d1_flights",
         description=f"afklm_d1_from_mongo.csv.tar.gz in {folder_path}", tags=['training'])
def download_update_d1_flights():

    route = "/update_scheduled_d1/export"

    file_name = "afklm_d1_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="update scheduled d1 flights not found")
    return {'message': file_name + 'saved in' + path}





@app.get("/download_scheduled_flights",
         description=f"afklm_scheduled_from_mongo.csv.tar.gz in {folder_path}", tags=['training'])
def download_scheduled_flights():

    route = "/scheduled/export"

    file_name = "afklm_scheduled_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="update scheduled d1 flights not found")
    return {'message': file_name + 'saved in' + path}


@app.get('/load_mongodb_data_into_postgres', name="Loads the contents of the files in the data_input folder into the postreSQL database", tags=['training'],response_class=PlainTextResponse)
def retrieve_latest_training_dataset():
    """Check if the API is running"""



    sql_file_folder = os.getenv('SQL_FILE_FOLDER')
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_URI')
    port = os.getenv('POSTGRES_PORT')
    database_name = os.getenv('POSTGRES_DB')
    tmp_path = "/app/tmp"

    conn = psycopg2.connect(database=database_name,
                            host=host,
                            user=username,
                            password=password,
                            port=port)

    cur = conn.cursor()



    data_file_folder = os.getenv('DATA_FILE_FOLDER')  # /data_input

    os.makedirs(tmp_path, exist_ok=True)

    file_list_data = os.listdir(data_file_folder)

    file_list_data = [file for file in file_list_data if ".tar.gz" in file]

    file_list_data_expected = ['afklm_historic_flights_from_mongo.csv.tar.gz',
                               'afklm_scheduled_flights_from_mongo.csv.tar.gz',
                               'afklm_update_scheduled_d1_flights_from_mongo.csv.tar.gz']
    
    file_list_missing = list(set(file_list_data_expected) - set(file_list_data))

    if len(file_list_missing) != 0:
        
        raise HTTPException(status_code=444, detail=f"Missing data files {file_list_missing}")
    
    for file in file_list_data:
        print(f"Decompressing {file} into {tmp_path}")
        shutil.unpack_archive(f"{data_file_folder}/{file}", tmp_path)

    print("Extracted files:", os.listdir(tmp_path))




    file_list_sql = os.listdir(sql_file_folder)

    file_list_sql = [file for file in file_list_sql if ".sql" in file]
    file_list_sql.sort()


    os.makedirs(tmp_path, exist_ok=True)

    try:
        for file in file_list_data:

            print(f"Decompressing {file} into {tmp_path}")
            shutil.unpack_archive(f"{data_file_folder}/{file}", tmp_path)
        print("Extracted files:", os.listdir(tmp_path))
        print("Decompression over")
    except:
        return("issue with decompression")
    
    try:
        print(f"SQL scripts to run: {file_list_sql}")

        for file in file_list_sql:
            full_path = os.path.join(sql_file_folder, file)
            print(f"--- STARTING {file} ---")

            with open(full_path, 'r') as sql_file:
                sql_content = sql_file.read()

                # Enable autocommit for index scripts
                if "create_index" in file.lower():
                    conn.autocommit = True
                    cur.execute(sql_content)
                    conn.autocommit = False
                else:
                    cur.execute(sql_content)
                    conn.commit()  

        print(f"Updated PostgreSQL database with the contents of the files located in {data_file_folder}")

        try:
            for f in os.listdir(tmp_path):
                os.remove(os.path.join(tmp_path, f))
            return("Postgres database updated and temporary files in /tmp erased successfully.")

        except Exception as cleanup_error:
            print(f"Warning: failed to clean /tmp: {cleanup_error}")

    except Exception as e:
        conn.rollback()
        return f"Erreur: {e}"

    finally:

        cur.close()
        conn.close()










from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, ServerSelectionTimeoutError, InvalidURI



def get_flights_by_id( collection_name,  date=None, id=None, nb_flight=None, collection_to_create = None):

    try:
        client = MongoClient(
            os.getenv('MONGODB_URI'),
            serverSelectionTimeoutMS=5000  # Timeout de 5 secondes
        )
        # ✅ Force une vraie connexion avec un ping
        client.admin.command('ping')
        mongo_db_connect = client[os.getenv('DATABASE_NAME')]
    except InvalidURI as e:
        mongo_db_connect = None
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        mongo_db_connect = None
    except ConfigurationError as e:
        mongo_db_connect = None
    except Exception as e:
        mongo_db_connect = None


    GREATEST_EUROPEAN_AIRPORT = [
        "CDG", "AMS", "ORY", "FCO", "LHR", "CPH", "MAD", "ARN", "OSL", "LIN",
        "NCE", "BCN", "LYS", "BGO", "LIS", "DUB", "HEL", "TLS", "OTP", "FRA",
        "MRS", "ATH", "PMI", "MUC", "TRD", "MAN", "BER", "AGP", "OPO"
    ]
    
    pipeline = [
        {
            "$addFields": {
                "departureAirports": {
                    "$map": {
                        "input": "$flightLegs",
                        "as": "leg",
                        "in": "$$leg.departureInformation.airport.code"
                    }
                },
                "arrivalAirports": {
                    "$map": {
                        "input": "$flightLegs",
                        "as": "leg",
                        "in": "$$leg.arrivalInformation.airport.code"
                    }
                }
            }
        },
        {
            "$addFields": {
                "hasReturnFlight": {
                    "$gt": [
                        {
                            "$size": {
                                "$setIntersection": ["$departureAirports", "$arrivalAirports"]
                            }
                        },
                        0
                    ]
                }
            }
        },
        {
            "$match": {
                "hasReturnFlight": False
            }
        },
        {
            "$match": {
                "flightLegs": {
                    "$not": {
                        "$elemMatch": {
                            "statusName": {"$in": ["New", "Cancelled"]}
                        }
                    }
                }
            }
        }
    ]
    if nb_flight is not None:
        pipeline.append({"$limit": nb_flight})
    
    if collection_name == "historic_flights":
        pipeline.append({
            "$addFields": {
                "debug_status": "$flightStatusPublic",
                "debug_length": {"$strLenCP": "$flightStatusPublic"}
            }
        })
        pipeline.append({
            "$match": {
                "flightStatusPublic": {"$nin": ["SCHEDULED", "Scheduled"]}
            }
        })
    

    pipeline.append({"$unwind": "$flightLegs"})
    

    pipeline.append({
        "$match": {
            "$or": [
                {"flightLegs.departureInformation.airport.code": {"$in": GREATEST_EUROPEAN_AIRPORT}},
                {"flightLegs.arrivalInformation.airport.code": {"$in": GREATEST_EUROPEAN_AIRPORT}}
            ]
        }
    })
    
    pipeline.append({
        "$project": {
            "id": "$id",
            "airline_code": {"$ifNull": ["$airline.code", ""]},
            "airline_name": {"$ifNull": ["$airline.name", ""]},
            "flightLegs_aircraft_ownerAirlineCode": {"$ifNull": ["$flightLegs.aircraft.ownerAirlineCode", ""]},
            "flightLegs_aircraft_typeCode": {"$ifNull": ["$flightLegs.aircraft.typeCode", ""]},
            "flightLegs_arrivalInformation_airport_city_country_areaCode": {"$ifNull":["$flightLegs.arrivalInformation.airport.city.country.areaCode", ""]},
            "flightLegs_arrivalInformation_airport_city_country_code": {"$ifNull":["$flightLegs.arrivalInformation.airport.city.country.code", ""]} ,
            "flightLegs_arrivalInformation_airport_city_country_name": {"$ifNull":["$flightLegs.arrivalInformation.airport.city.country.name", ""]} , 
            "flightLegs_arrivalInformation_airport_code": {"$ifNull":["$flightLegs.arrivalInformation.airport.code", ""]} ,
            "flightLegs_arrivalInformation_airport_location_latitude": {"$ifNull":["$flightLegs.arrivalInformation.airport.location.latitude", ""]} ,
            "flightLegs_arrivalInformation_airport_location_longitude": {"$ifNull":["$flightLegs.arrivalInformation.airport.location.longitude", ""]} ,
            "flightLegs_arrivalInformation_times_scheduled": {"$ifNull":["$flightLegs.arrivalInformation.times.scheduled", ""]} ,
            "flightLegs_departureInformation_airport_city_country_areaCode": {"$ifNull":["$flightLegs.departureInformation.airport.city.country.areaCode", ""]} ,
            "flightLegs_departureInformation_airport_city_country_code": {"$ifNull":["$flightLegs.departureInformation.airport.city.country.code", ""]} ,
            "flightLegs_departureInformation_airport_city_country_name": {"$ifNull":["$flightLegs.departureInformation.airport.city.country.name", ""]} ,
            "flightLegs_departureInformation_airport_code": {"$ifNull":["$flightLegs.departureInformation.airport.code", ""]}  ,
            "flightLegs_departureInformation_airport_location_latitude": {"$ifNull":["$flightLegs.departureInformation.airport.location.latitude", ""]} ,
            "flightLegs_departureInformation_airport_location_longitude": {"$ifNull":["$flightLegs.departureInformation.airport.location.longitude", ""]} , 
            "flightLegs_departureInformation_airport_places_departurePositionTerminal_gateNumber": {"$ifNull":["$flightLegs.departureInformation.airport.places.gateNumber", ""]},
            "flightLegs_departureInformation_times_scheduled": {"$ifNull":["$flightLegs.departureInformation.times.scheduled", ""]},
            "flightLegs_irregularity_delayDuration": {"$ifNull":["$flightLegs.irregularity.delayDuration", ""]}, 
            "flightLegs_irregularity_delayInformation_delayReasonPublicLong": {"$ifNull":["$flightLegs.irregularity.delayInformation.delayReasonPublicLong", ""]}, 
            "flightLegs_irregularity_delayInformation_delayCode": {"$ifNull":["$flightLegs.irregularity.delayInformation.delayCode", ""]}, 
            "flightLegs_irregularity_delayInformation_delayReasonPublicShort": {"$ifNull":["$flightLegs.irregularity.delayInformation.delayReasonPublicShort", ""]}, 
            "flightLegs_irregularity_delayReason": {"$ifNull":["$flightLegs.irregularity.delayReason", ""]},
            "flightLegs_scheduledFlightDuration": {"$ifNull":["$flightLegs.scheduledFlightDuration", ""]} ,
            "flightLegs_serviceType": {"$ifNull":["$flightLegs.serviceType", ""]} ,
            "flightLegs_serviceTypeName": {"$ifNull":["$flightLegs.serviceTypeName", ""]} ,
            "flightLegs_status": {"$ifNull":["$flightLegs.status", ""]} ,
            "flightLegs_publishedStatus": {"$ifNull":["$flightLegs.publishedStatus", ""]} ,
            "flightLegs_legStatusPublic":  {"$ifNull":["$flightLegs.legStatusPublic", ""]} , 
            "flightLegs_statusName": {"$ifNull": ["$flightLegs.statusName", ""]},
            "flightNumber": {"$ifNull": ["$flightNumber",""]},
            "flightStatusPublic": {"$ifNull": ["$flightStatusPublic",""]},
            "flightLegs_arrivalInformation_times_estimated_value": {"$ifNull": ["$flightLegs.arrivalInformation.times.estimated.value",""]}, 
            "flightLegs_arrivalInformation_times_latestPublished": {"$ifNull": ["$flightLegs.arrivalInformation.times.latestPublished",""]},
            "flightLegs_departureInformation_times_actual": {"$ifNull": ["$flightLegs.departureInformation.times.actual",""]},  
            "flightLegs_departureInformation_times_actualTakeOffTime": {"$ifNull": ["$flightLegs.departureInformation.times.actualTakeOffTime",""]}, 
            "flightLegs_departureInformation_times_latestPublished": {"$ifNull": ["$flightLegs.departureInformation.times.latestPublished",""]},  
            "flightLegs_arrivalInformation_airport_places_arrivalPositionTerminal": {"$ifNull":["$flightLegs.arrivalInformation.airport.places.arrivalPositionTerminal", ""]} ,
            "flightLegs_arrivalInformation_times_actual": {"$ifNull": ["$flightLegs.arrivalInformation.times.actual",""]}, 
            "flightLegs_arrivalInformation_times_actualTouchDownTime": {"$ifNull": ["$flightLegs.arrivalInformation.times.actualTouchDownTime",""]},  
            "flightLegs_departureInformation_airport_places_departurePositionTerminal_boardingTerminal": {"$ifNull":["$flightLegs.departureInformation.airport.places.boardingTerminal", ""]}
            }
    })

    pipeline.append({
        "$sort": {"id": 1}  
    })


    if id is not None:
        pipeline.append({
            "$match": {
                "id": {"$gte": id}  
            }
        })



    if collection_to_create:
        pipeline.append({"$out": collection_to_create })
        mongo_db_connect[collection_name].aggregate(pipeline)
            



@app.get("/export_all_collections_to_postgres_data_input",
        description=f"afklm_d1_from_mongo.csv.tar.gz in {folder_path}", tags=['training'])
def download_update_d1_flights_test( ):

    folder_path = "/data/output_for_postgres"
    csv_file = f"{folder_path}/afklm_d1_from_mongo.csv"
    tar_file = f"{csv_file}.tar.gz"

    CSV_FIELDS = (
        "_id,id,airline_code,airline_name,"
        "flightLegs_aircraft_ownerAirlineCode,flightLegs_aircraft_typeCode,"
        "flightLegs_arrivalInformation_airport_city_country_areaCode,"
        "flightLegs_arrivalInformation_airport_city_country_code,"
        "flightLegs_arrivalInformation_airport_city_country_name,"
        "flightLegs_arrivalInformation_airport_code,"
        "flightLegs_arrivalInformation_airport_location_latitude,"
        "flightLegs_arrivalInformation_airport_location_longitude,"
        "flightLegs_arrivalInformation_times_scheduled,"
        "flightLegs_departureInformation_airport_city_country_areaCode,"
        "flightLegs_departureInformation_airport_city_country_code,"
        "flightLegs_departureInformation_airport_city_country_name,"
        "flightLegs_departureInformation_airport_code,"
        "flightLegs_departureInformation_airport_location_latitude,"
        "flightLegs_departureInformation_airport_location_longitude,"
        "flightLegs_departureInformation_airport_places_departurePositionTerminal_gateNumber,"
        "flightLegs_departureInformation_times_scheduled,"
        "flightLegs_irregularity_delayDuration,"
        "flightLegs_irregularity_delayInformation_delayReasonPublicLong,"
        "flightLegs_irregularity_delayInformation_delayCode,"
        "flightLegs_irregularity_delayInformation_delayReasonPublicShort,"
        "flightLegs_irregularity_delayReason,"
        "flightLegs_scheduledFlightDuration,"
        "flightLegs_serviceType,"
        "flightLegs_serviceTypeName,"
        "flightLegs_status,"
        "flightLegs_publishedStatus,"
        "flightLegs_legStatusPublic,"
        "flightLegs_statusName,"
        "flightNumber,"
        "flightStatusPublic,"
        "flightLegs_arrivalInformation_times_estimated_value,"
        "flightLegs_arrivalInformation_times_latestPublished,"
        "flightLegs_departureInformation_times_actual,"
        "flightLegs_departureInformation_times_actualTakeOffTime,"
        "flightLegs_departureInformation_times_latestPublished,"
        "flightLegs_arrivalInformation_airport_places_arrivalPositionTerminal,"
        "flightLegs_arrivalInformation_times_actual,"
        "flightLegs_arrivalInformation_times_actualTouchDownTime,"
        "flightLegs_departureInformation_airport_places_departurePositionTerminal_boardingTerminal,"
        "flightLegs_irregularity_delayDuration_total"
    )

    results = []

    for collection in ['historic_flights','scheduled_flights','update_scheduled_d1_flights']:
        
        # Drop collection first
        drop_cmd = [
            "docker", "exec", "afklm_mongodb",
            "mongosh",
            "--quiet",
            "--eval",
            'try { db.getSiblingDB("airlines").getCollection("for_csv_export").drop(); } catch(e) { print("collection not found"); }'
        ]
        subprocess.run(drop_cmd, check=True, capture_output=True, text=True)
        
        # Populate collection
        get_flights_by_id(collection, collection_to_create="for_csv_export")
        
        # Export CSV
        csv_file = f"/data/output_for_postgres/afklm_{collection}_from_mongo.csv"
        tar_file = f"{csv_file}.tar.gz"
        
        mongoexport_cmd = [
            "docker", "exec", "afklm_mongodb",
            "mongoexport",
            "--username", "airlines",
            "--password", "airlines",
            "--authenticationDatabase", "admin",
            "--db", "airlines",
            "--collection", "for_csv_export",
            "--type=csv",
            "--fields", CSV_FIELDS,
            "--out", csv_file
        ]

        subprocess.run(mongoexport_cmd, check=True, capture_output=True, text=True)

            
        # Compress CSV
        tar_cmd = [
            "docker", "exec", "afklm_mongodb",
            "tar", "-czf", tar_file,
            "-C", "/data/output_for_postgres",
            os.path.basename(csv_file)
        ]
        subprocess.run(tar_cmd, check=True, capture_output=True, text=True)
        
        # Delete CSV to keep only tar.gz
        cleanup_cmd = ["docker", "exec", "afklm_mongodb", "rm", csv_file]
        subprocess.run(cleanup_cmd, check=True)
        
        # Drop collection again to clean up
        subprocess.run(drop_cmd, check=True, capture_output=True, text=True)
        
        results.append({"collection": collection, "tar_file": tar_file})


    try:
        subprocess.run(
            ["docker", "restart", "afklm_mongodb"],
            check=True,
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Mongo restart failed: {e.stderr}"
        )

    return {
            "status": "success",
            "mongoDB":"Restarting",

            "files": [
                "/data/output_for_postgres/afklm_historic_flights_from_mongo.csv.tar.gz",
                "/data/output_for_postgres/afklm_scheduled_flights_from_mongo.csv.tar.gz",
                "/data/output_for_postgres/afklm_update_scheduled_d1_flights_from_mongo.csv.tar.gz"
            ]
            }

