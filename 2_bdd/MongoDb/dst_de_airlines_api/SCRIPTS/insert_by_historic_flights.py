from USE_CASES.insert_historic_flights_uc import insert_operation_fly
from USE_CASES.insert_file_names_uc import insert_file_name, is_file_name_exist
from USE_CASES.insert_compressed_file_name_uc import is_compressed_file_name_exist, insert_one_compressed_file_name
from SERVICES.exploitation_json import open_json_by_its_name, delete_page_object_in_json, is_json
from SERVICES.exploration_gz_file import get_json_in_gz_file_by_its_name_local
from SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, is_gz_file
import os
from pathlib import Path



def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    file_names = get_file_names_by_folder(folder_path)
    for file_name in file_names:
        if is_gz_file(file_name) == True:
            gz_file_name = file_name
            if is_compressed_file_name_exist(gz_file_name) == False:
                decompressed_file = get_json_in_gz_file_by_its_name_local(gz_file_name)
                decompressed_file = delete_page_object_in_json(decompressed_file)
                insert_operation_fly(decompressed_file)
                insert_one_compressed_file_name(gz_file_name)
            
            
            

        else : 
            if is_file_name_exist(file_name) == False:
                if is_json(file_name) == True:
                    json_file = open_json_by_its_name(file_name)
                    json_file = delete_page_object_in_json(json_file)
                    insert_operation_fly(json_file)
                    insert_file_name(file_name)
    #create_index()


      


import_operationalflights_in_mongodb()
    