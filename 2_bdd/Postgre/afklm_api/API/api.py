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

    file_list_data_expected = ['afklm_d1_from_mongo.csv.tar.gz',
                               'afklm_historic_from_mongo.csv.tar.gz',
                               'afklm_scheduled_from_mongo.csv.tar.gz']
    
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

        print(f"PostgreSQL database with the contents of the files located in {data_file_folder}")

    except Exception as e:
        conn.rollback()
        return f"Erreur: {e}"

    finally:

        cur.close()
        conn.close()

        try:
            for f in os.listdir(tmp_path):
                os.remove(os.path.join(tmp_path, f))
            return("Postgres database updated and temporary files in /tmp erased successfully.")

        except Exception as cleanup_error:
            print(f"Warning: failed to clean /tmp: {cleanup_error}")
