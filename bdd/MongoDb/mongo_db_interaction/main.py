from .use_cases.insert_operational_flights_uc import insert
from .services.exploitation_json import open_json
from .services.folder_exploration import get_name_files_by_folder, get_folder_path_in_env
import os
from pathlib import Path



def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    name_files = get_name_files_by_folder(folder_path)
    i = 0
    for name_file in name_files:
        json_file = open_json(name_file)
        insert(json_file)
      


import_operationalflights_in_mongodb()
    