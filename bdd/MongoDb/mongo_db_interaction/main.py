from mongo_db_interaction.USE_CASES.insert_operational_flights_uc import insert_operation_fly
from mongo_db_interaction.USE_CASES.insert_file_names import insert_file_name, is_file_name_exist
from mongo_db_interaction.USE_CASES.insert_compressed_file_name_uc import is_compressed_file_name_exist, insert_compressed_file_name
from mongo_db_interaction.SERVICES.exploitation_json import open_json, delete_page_object_in_json, is_json
from mongo_db_interaction.SERVICES.exploration_compressed_file import open_compressed_file
from mongo_db_interaction.SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, is_compressed_file
import os
from pathlib import Path



def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    file_names = get_file_names_by_folder(folder_path)
    for file_name in file_names:
        if is_compressed_file(file_name) == True:
            compressed_file_name = file_name
            if is_compressed_file_name_exist(compressed_file_name) == False:
                decompressed_file = open_compressed_file(compressed_file_name)
                decompressed_file = delete_page_object_in_json(decompressed_file)
                insert_operation_fly(decompressed_file)
                insert_compressed_file_name(compressed_file_name)
            
            
            

        else : 
            if is_file_name_exist(file_name) == False:
                if is_json(file_name) == True:
                    json_file = open_json(file_name)
                    json_file = delete_page_object_in_json(json_file)
                    insert_operation_fly(json_file)
                    insert_file_name(file_name)
    #create_index()


      


import_operationalflights_in_mongodb()
    