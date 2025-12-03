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
import os
import psycopg2



load_dotenv()






api = FastAPI(
    title = "Air France KLM - PostgreSQL API",
    description = "REST API for updating the afklm PostgreSQL database with the latest data from MongoDB",
    docs_url = "/"

)

@api.get('/health', name="Check if the API is running", tags=['tests'],response_class=PlainTextResponse)
def get_index():
    """Check if the API is running"""
    return "The API is running"



@api.get('/load_mongodb_data_into_postgres', name="Loads the contents of the files in the data_input folder into the postreSQL database", tags=['training'],response_class=PlainTextResponse)
def retrieve_latest_training_dataset():
    """Check if the API is running"""



    sql_file_folder = os.getenv('SQL_FILE_FOLDER')
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_URI')
    port = os.getenv('POSTGRES_PORT')
    database_name = os.getenv('POSTGRES_DB')


    conn = psycopg2.connect(database=database_name,
                            host=host,
                            user=username,
                            password=password,
                            port=port)

    cur = conn.cursor()



    data_file_folder = os.getenv('DATA_FILE_FOLDER')  # /data_input
    tmp_path = f"{data_file_folder}/tmp"

    os.makedirs(tmp_path, exist_ok=True)

    for file in file_list_data:
        print(f"Decompressing {file} into {tmp_path}")
        shutil.unpack_archive(f"{data_file_folder}/{file}", tmp_path)

    print("Extracted files:", os.listdir(tmp_path))




    file_list_data = os.listdir(data_file_folder)

    file_list_data = [file for file in file_list_data if ".tar.gz" in file]

    file_list_sql = os.listdir(sql_file_folder)

    file_list_sql = [file for file in file_list_sql if ".sql" in file]
    file_list_sql.sort()

    os.makedirs('/app/data_input/',exist_ok=True)


    file_list_data_expected = ['afklm_d1_from_mongo.csv.tar.gz',
                               'afklm_historic_from_mongo.csv.tar.gz',
                               'afklm_scheduled_from_mongo.csv.tar.gz']
    
    file_list_missing = list(set(file_list_data_expected) - set(file_list_data))

    if len(file_list_missing) != 0:
        
        raise HTTPException(status_code=444, detail=f"Missing data files {file_list_missing}")

    try:
        for file in file_list_data:

            print(f"Decompressing {file} into tmp/")
            shutil.unpack_archive(f"{data_file_folder}/{file}", f'tmp/')
        print("Decompression over")
    except:
        return("issue with decompression")
    
    print(os.getcwd())
    print(os.listdir('.'))
    print(os.listdir('./tmp'))
    try:

        for file in file_list_sql:
            print(f"{file}")
            sql_file = open(f'{sql_file_folder}/{file}','r')
            cur.execute(sql_file.read())


        print("commit")
        conn.commit()
        cur.close()
        conn.close()
        return "PostgreSQL database updated with the latest dataset from MongoDB"
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()

        return f"Erreur: {e}"
