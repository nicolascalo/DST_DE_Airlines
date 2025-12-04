from SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, remove_file, is_gz_file
from SERVICES.exploration_gz_file import get_json_in_gz_file_by_its_name_local, get_collection_name_by_end_gz_file_name
from SERVICES.exploitation_json import delete_page_object_in_json
from USE_CASES.insert_compressed_file_name_uc import insert_one_compressed_file_name
from DAO.operational_flights import insert_one, delete_duplicates, move_to_dst_collection, delete_all_opreation_flights_collection, remove_past_flights_on_d1_collection, remove_duplicate_flights_from_scheduled,remove_past_flights_on_scheduled_collection
from DAO.flights import add_date_insertion
from DAO.collections import get_all_collection_name
from DAO.compressed_file_name import get_all_compressed_file_names
from USE_CASES.insert_compressed_file_name_uc import  is_compressed_file_name_exist



def insert_by_operational_flights():
    folder_path = get_folder_path_in_env()
    file_names = get_file_names_by_folder(folder_path)

    compressed_file_names = get_all_compressed_file_names()
    
  
    compressed_file_names = list(compressed_file_names)

    gz_file_names = []

    for item in compressed_file_names:
        gz_file_names.append(item['compressed_file_name'])

    


 

    missing_file_names = set(file_names) - set(gz_file_names)


   
    for file_name in missing_file_names:
        
        if is_gz_file(file_name) == True:
            gz_file_name = file_name
            
            collection_name = get_collection_name_by_end_gz_file_name(gz_file_name)

            json_file = get_json_in_gz_file_by_its_name_local(gz_file_name)

            if json_file == "corrupted file" or json_file == "invalid json":
                remove_file(folder_path, file_name)
              
            else:
                json_file = delete_page_object_in_json(json_file)

              
                insert_one(json_file, collection_name)
                insert_one_compressed_file_name(gz_file_name)

    

def clean():
            
    org_collections = ['historic_operational_flights', 'update_scheduled_d1_operational_flights','scheduled_operational_flights']
    for org_collection in org_collections:
        dst_collection = org_collection.replace("_operational_","_")
        move_to_dst_collection(org_collection, dst_collection)
        delete_duplicates(org_collection)
        delete_all_opreation_flights_collection(org_collection)
    remove_duplicate_flights_from_scheduled()
    remove_past_flights_on_d1_collection()
    remove_past_flights_on_scheduled_collection()
    
    
    
def add_date_insertion_in_flights():
    collection_names = get_all_collection_name()
    for collection_name in collection_names:
        if collection_name != 'compressed_file_names':
            add_date_insertion(collection_name)


    
insert_by_operational_flights()
clean()
add_date_insertion_in_flights()