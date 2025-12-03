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



@api.get('/retrieve_latest_data_for_training', name="Retrieve the latest training dataset", tags=['training'],response_class=PlainTextResponse)
def retrieve_latest_training_dataset():

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
    data_file_folder = os.getenv('DATA_FILE_FOLDER')

    file_list = os.listdir(data_file_folder)

    file_list = [file for file in file_list if ".tar.gz" in file]

    os.makedirs('/app/data_input/',exist_ok=True)

    try:
        for file in file_list:
            shutil.unpack_archive(f"{data_file_folder}/{file}", f'/tmp/data_input/')
        print("Decompressing the data files")
    except:
        return("issue with decompression")

    try:
        print("5_insert_mongodbdump")
        sql_file = open(f'{sql_file_folder}/5_insert_mongodbdump.sql','r')
        cur.execute(sql_file.read())
        print("6_insert_select_flight")
        sql_file = open(f'{sql_file_folder}/6_insert_select_flight.sql','r')
        print("execute")
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
