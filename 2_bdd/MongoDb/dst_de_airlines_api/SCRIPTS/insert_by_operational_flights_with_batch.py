from SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, remove_file, is_gz_file, get_file_names_on_gcp
from SERVICES.exploration_gz_file import get_json_in_gz_file_by_its_name_local, get_collection_name_by_end_gz_file_name, get_json_in_gz_file_by_its_name_gcp
from SERVICES.exploitation_json import delete_page_object_in_json
from USE_CASES.insert_compressed_file_name_uc import insert_many_compressed_file_names
from DAO.operational_flights import insert_many, delete_duplicates, move_to_dst_collection, delete_all_opreation_flights_collection, remove_past_flights_on_d1_collection, remove_duplicate_flights_from_scheduled,remove_past_flights_on_scheduled_collection
from DAO.flights import add_date_insertion
from DAO.compressed_file_name import get_all_compressed_file_names
from DAO.collections import get_all_collection_name
from CONNECTION.check_gcp_connection import check_gcp_connection
import gc






def import_operationalflights_in_mongodb():

    bucket = check_gcp_connection()


    documents_by_collection = {}
    gz_file_name_json = []
    folder_path = None


    if bucket != None:
        file_names = get_file_names_on_gcp()
    else:
        folder_path = get_folder_path_in_env()
        file_names = get_file_names_by_folder(folder_path)




    batch_size = 200

    compressed_file_names = get_all_compressed_file_names()
    existing_files = [doc['compressed_file_name'] for doc in compressed_file_names]

    print("nb compressed file names already in data base"+str(len(existing_files)))

  
    i = batch_size


    for file_name in file_names:
        if is_gz_file(file_name) == True:
            gz_file_name = file_name
            collection_name = get_collection_name_by_end_gz_file_name(gz_file_name)

            if bucket == None:

                json_file = get_json_in_gz_file_by_its_name_local(gz_file_name)
            else: 
                json_file = get_json_in_gz_file_by_its_name_gcp(gz_file_name)

            if json_file == "corrupted file" or json_file == "invalid json":
                remove_file(folder_path, file_name)
               
            else:
                json_file = delete_page_object_in_json(json_file)

                if gz_file_name not in existing_files:
                    if collection_name not in documents_by_collection:
                        documents_by_collection[collection_name] = []
                
                    print(gz_file_name + " add to list for insert")
                    

                    documents_by_collection[collection_name].append(json_file)


                    gz_file_name_json.append({"compressed_file_name": gz_file_name})
                    

                    if len(documents_by_collection[collection_name])>= batch_size:
                        insert_many(documents_by_collection[collection_name], collection_name)
                        documents_by_collection[collection_name] = []
                
                        insert_many_compressed_file_names(gz_file_name_json)
                        gz_file_name_json = []

                   
                
                        print("nb_inserted " + str(i))
                        i = i + batch_size
                else:
                    print(gz_file_name + " is alredady in database")
                       
        gc.collect()

    for collection_name, batch in documents_by_collection.items():
        if batch:
            insert_many(batch, collection_name)
            gc.collect()
        gc.collect()

    if gz_file_name_json:
        insert_many_compressed_file_names(gz_file_name_json)
        gc.collect()


    
    

    

def clean():
            
    org_collections = ['historic_operational_flights', 'update_scheduled_d1_operational_flights','scheduled_operational_flights']
    for org_collection in org_collections:
        dst_collection = org_collection.replace("_operational_","_")
        move_to_dst_collection(org_collection, dst_collection)
        delete_duplicates(dst_collection)
        delete_all_opreation_flights_collection(org_collection)
        gc.collect()
    remove_duplicate_flights_from_scheduled()
    gc.collect()
    remove_past_flights_on_d1_collection()
    gc.collect()
    remove_past_flights_on_scheduled_collection()

    
    
    
def add_date_insertion_in_flights():
    collection_names = get_all_collection_name()
    for collection_name in collection_names:
        if collection_name != 'compressed_file_names':
            add_date_insertion(collection_name)
        gc.collect()
    gc.collect()


    
import_operationalflights_in_mongodb()
clean()
add_date_insertion_in_flights()






    

