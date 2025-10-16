from ..db_context.db_context import mongo_db_connect
from ..services.folder_exploration import get_folder_path_in_env
import json
from dotenv import load_dotenv
import os



def insert_one(file):
    load_dotenv()
    file_path = build_insert_path_file(file)
    mongo_db_connect[os.getenv('DATABASE_NAME')]['operational_flights'].insert_one(json.load(open(file_path)))


def insert_many(file):
    load_dotenv()
    file_path = build_insert_path_file(file)
    mongo_db_connect[os.getenv('DATABASE_NAME')]['operational_flights'].insert_one(json.load(open(file_path)))


def build_insert_path_file(file):
    folder_path = get_folder_path_in_env()
    file_path = folder_path + file
    return file_path