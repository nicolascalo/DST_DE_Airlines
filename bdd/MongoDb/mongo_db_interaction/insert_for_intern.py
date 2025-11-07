from .SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, remove_file, is_gz_file
from .SERVICES.exploration_compressed_file import open_gz_file_by_its_name
from .SERVICES.exploitation_json import delete_page_object_in_json
from .USE_CASES.insert_compressed_file_name_uc import insert_compressed_file_name
from .REPOSITORIES.operation_flights import insert_one, delete_duplicates, move_to_flight_collection, delete_all_opreation_flights_collection
import os
from pathlib import Path
def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    file_names = get_file_names_by_folder(folder_path)
    for file_name in file_names:
        if is_gz_file(file_name) == True:
            gz_file_name = file_name
            decompressed_file = open_gz_file_by_its_name(gz_file_name)
            if decompressed_file == "corrupted file" or decompressed_file == "invalid json":
                remove_file(folder_path, file_name)
            else:
                

                decompressed_file = delete_page_object_in_json(decompressed_file)
                insert_one(decompressed_file)
                insert_compressed_file_name(gz_file_name)
        
                
    move_to_flight_collection()
    delete_duplicates()
    delete_all_opreation_flights_collection()
    


    
import_operationalflights_in_mongodb()




    


