from SERVICES.folder_exploration import get_file_names_by_folder, get_folder_path_in_env, remove_file, is_gz_file
from SERVICES.exploration_gz_file import get_json_in_gz_file_by_its_name, get_collection_name_by_end_gz_file_name
from SERVICES.exploitation_json import delete_page_object_in_json
from USE_CASES.insert_compressed_file_name_uc import insert_compressed_file_name
from REPOSITORIES.operational_flights import insert_one, delete_duplicates, move_to_dst_collection, delete_all_opreation_flights_collection, remove_past_flights_on_d1_collection, remove_duplicate_flights_from_scheduled,remove_past_flights_on_scheduled_collection
from REPOSITORIES.flights import add_date_insertion
from REPOSITORIES.collections import get_all_collection_name
from google.cloud import storage
from io import BytesIO
import gzip
import json

def test():


    bucket_name = "airfrance-bucket"
    prefix = "data/"

    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        print(blob.name)
        




test()

def import_operationalflights_in_mongodb():




    with gzip.GzipFile(fileobj=BytesIO(gzip_data)) as gz :
        data = json.load(gz)
    print(json.dumps(data))


    

def clean():
            
    org_collections = ['historic_operational_flights', 'update_scheduled_d1_operational_flights','scheduled_operational_flights']
    for org_collection in org_collections:
        dst_collection = org_collection.replace("_operational_","_")
        move_to_dst_collection(org_collection, dst_collection)
        delete_duplicates(dst_collection)
        delete_all_opreation_flights_collection(org_collection)
    remove_duplicate_flights_from_scheduled()
    remove_past_flights_on_d1_collection()
    remove_past_flights_on_scheduled_collection()
    
    
    
def add_date_insertion_in_flights():
    collection_names = get_all_collection_name()
    for collection_name in collection_names:
        if collection_name != 'compressed_file_names':
            add_date_insertion(collection_name)


    
#import_operationalflights_in_mongodb()
#clean()
#add_date_insertion_in_flights()
