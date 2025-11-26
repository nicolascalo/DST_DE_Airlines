from SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, remove_file, is_gz_file
from SERVICES.exploration_gz_file import get_json_in_gz_file_by_its_name, get_collection_name_by_end_gz_file_name
from SERVICES.exploitation_json import delete_page_object_in_json
from USE_CASES.insert_compressed_file_name_uc import insert_compressed_file_name
from REPOSITORIES.operational_flights import insert_many, delete_duplicates, move_to_dst_collection, delete_all_opreation_flights_collection, remove_past_flights_on_d1_collection, remove_duplicate_flights_from_scheduled,remove_past_flights_on_scheduled_collection
from REPOSITORIES.flights import add_date_insertion
from REPOSITORIES.collections import get_all_collection_name
from USE_CASES.insert_compressed_file_name_uc import  is_compressed_file_name_exist
import gc



def import_operationalflights_in_mongodb():

    documents_by_collection = {}
    gz_file_name_json = []



    folder_path = get_folder_path_in_env()
    file_names = get_file_names_by_folder(folder_path)
    batch_size = 100
  
    i = batch_size

    for file_name in file_names:
        if is_gz_file(file_name) == True:
            gz_file_name = file_name
            collection_name = get_collection_name_by_end_gz_file_name(gz_file_name)

            json_file = get_json_in_gz_file_by_its_name(gz_file_name)
            if json_file == "corrupted file" or json_file == "invalid json":
                remove_file(folder_path, file_name)
                    # Ajouter une fonction permetant d'ajouter le nom du fichier corompu dans un .txt
            else:
                json_file = delete_page_object_in_json(json_file)
                if is_compressed_file_name_exist(gz_file_name) == False:
                    if collection_name not in documents_by_collection:
                        documents_by_collection[collection_name] = []
                    

                    #AJOUT------------------------------------------------------
                    documents_by_collection[collection_name].append(json_file)

                

                    gz_file_name_json.append(gz_file_name)
                    


                    #FIN AJOUT---------------------------------------------

                    if len(documents_by_collection[collection_name])>= batch_size:
                        insert_many(documents_by_collection[collection_name], collection_name)
                        documents_by_collection[collection_name] = []
                
                        insert_compressed_file_name(gz_file_name)

                        gz_file_name_json = []
                
                        print("nb_inserted " + str(i))
                        i = i + batch_size
                       
                       
        gc.collect()

    for collection_name, batch in documents_by_collection.items():
        if batch:
            insert_many(batch, collection_name)

    if gz_file_name_json != []:
        insert_compressed_file_name(gz_file_name)


    
    

    

def clean():
            
    org_collections = ['historic_operational_flights', 'update_scheduled_d1_operational_flights','scheduled_operational_flights']
    for org_collection in org_collections:
        dst_collection = org_collection.replace("_operational_","_")
        move_to_dst_collection(org_collection, dst_collection)
        delete_duplicates(dst_collection)
        delete_all_opreation_flights_collection(org_collection)
        gc.collect()
    remove_duplicate_flights_from_scheduled()
    remove_past_flights_on_d1_collection()
    remove_past_flights_on_scheduled_collection()

    
    
    
def add_date_insertion_in_flights():
    collection_names = get_all_collection_name()
    for collection_name in collection_names:
        if collection_name != 'compressed_file_names':
            add_date_insertion(collection_name)
        gc.collect()


    
import_operationalflights_in_mongodb()
clean()
add_date_insertion_in_flights()






    

