from .use_cases.insert_operational_flights_uc import insert_operation_fly
from .use_cases.insert_name_files_collection_uc import insert_name_file_collection, is_file_name_exist
from .services.exploitation_json import open_json, delete_page_object_in_json
from .services.folder_exploration import get_name_files_by_folder, get_folder_path_in_env
import os
from pathlib import Path



def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    name_files = get_name_files_by_folder(folder_path)
    for file_name in name_files:
        if is_file_name_exist(file_name) == False:
            json_file = open_json(file_name)
            json_file = delete_page_object_in_json(json_file)
            insert_operation_fly(json_file)
            insert_name_file_collection(file_name)
    #create_index()


      


import_operationalflights_in_mongodb()
    