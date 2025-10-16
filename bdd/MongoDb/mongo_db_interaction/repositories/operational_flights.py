from ..db_context.db_context import mongo_db_connect
from ..services.folder_exploration import get_folder_path_in_env
import json
import os

collection = 'operational_flights'


def insert_one(json_file):

    mongo_db_connect[collection].insert_one(json_file)


def insert_many(json_file):

    mongo_db_connect[collection].insert_one(json_file)


def get_by_id(id):
    mongo_db_connect[collection].findOne({"operationalFlights.id:"+id})



