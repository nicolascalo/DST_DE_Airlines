
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
'''
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
'''

    
#import_operationalflights_in_mongodb()
#clean()
#add_date_insertion_in_flights()
