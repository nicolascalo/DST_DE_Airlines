from .SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, remove_file, is_gz_file
from .SERVICES.exploration_gz_file import get_json_in_gz_file_by_its_name, get_collection_name_by_end_gz_file_name
from .SERVICES.exploitation_json import delete_page_object_in_json
from .USE_CASES.insert_compressed_file_name_uc import insert_compressed_file_name
from .REPOSITORIES.operational_flights import insert_one, delete_duplicates, move_to_flight_collection, delete_all_opreation_flights_collection
import os
from pathlib import Path
def import_operationalflights_in_mongodb():
    folder_path = get_folder_path_in_env()
    file_names = get_file_names_by_folder(folder_path)
   
    for file_name in file_names:
        
        if is_gz_file(file_name) == True:
            gz_file_name = file_name
            collection_name = get_collection_name_by_end_gz_file_name(gz_file_name)
            print(collection_name)
            json_file = get_json_in_gz_file_by_its_name(gz_file_name)
            if json_file == "corrupted file" or json_file == "invalid json":
                remove_file(folder_path, file_name)
                # Ajouter une fonction permetant d'ajouter le nom du fichier corompu dans un .txt
            else:
                json_file = delete_page_object_in_json(json_file)
                 
                insert_one(json_file, collection_name)
                insert_compressed_file_name(gz_file_name)

    
        
    dst_collections = ['historic_flights', 'update_scheduled_d1_flights','scheduled_flights']
    for dst_collection in dst_collections:
        org_collection = 'operation_flights'
        move_to_flight_collection(org_collection, dst_collection)
        delete_duplicates(dst_collection)
        delete_all_opreation_flights_collection(org_collection)
    


    
import_operationalflights_in_mongodb()




    


