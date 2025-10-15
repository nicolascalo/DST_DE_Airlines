from ..db_context.db_context import mongo_db_connect
import json
from dotenv import load_dotenv
import os





def insert_one(file):
    load_dotenv()
    mongo_db_connect[os.getenv('DATABASE_NAME')]['operational_flights'].insert_one(json.load(open(file)))


def insert_many(file):
    load_dotenv()

    mongo_db_connect[os.getenv('DATABASE_NAME')]['operational_flights'].insert_one(json.load(open(file)))
