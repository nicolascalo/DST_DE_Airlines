from .use_cases.insert_operational_flights_uc import insert
from .services.folder_exploration import get_list_files_by_folder, get_folder_path_in_env
import os
from pathlib import Path



def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    files = get_list_files_by_folder(folder_path)
    i = 0
    for file in files:
        insert(file)
      


import_operationalflights_in_mongodb()
    