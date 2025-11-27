from .folder_exploration import get_folder_path_in_env
import json
import zipfile
import os
import io
import tarfile
import gzip
import bz2
import lzma
from CONNECTION.check_gcp_connection import check_gcp_connection





def get_json_in_gz_file_by_its_name_local(gz_file_name):

 
    folder_path = get_folder_path_in_env()
    compressed_file_path = folder_path + gz_file_name

   
    with  gzip.open(compressed_file_path, 'rt', encoding='utf-8') as gz_file:
        try:
            
            decompressed_file = json.load(gz_file)
        except gzip.BadGzipFile as e:

                print("corrompu: "+gz_file_name)
                decompressed_file =  "corrupted file"
        
        except json.JSONDecodeError as e:
                print("Json invalide : "+gz_file_name)
                decompressed_file = "invalid json"
                

 
    return decompressed_file


def get_json_in_gz_file_by_its_name_gcp(gz_file_name):
    bucket = check_gcp_connection()
    try:
        blob = bucket.blob(gz_file_name)
        compressed_content = blob.download_as_bytes()  
        
        with gzip.open(io.BytesIO(compressed_content), 'rt', encoding='utf-8') as gz_file:
            try:
                decompressed_file = json.load(gz_file)
                return decompressed_file
                
            except gzip.BadGzipFile as e:
                print(f"Corrompu: {gz_file_name}")
                return "corrupted file"
            
            except json.JSONDecodeError as e:
                print(f"Json invalide: {gz_file_name}")
                return "invalid json"
                
    except Exception as e:
        print(f"Erreur téléchargement: {e}")
        return None

    

     


def get_collection_name_by_end_gz_file_name(gz_name):
     
    collection_name = ''
    if gz_name.endswith("_sched.json.gz"):
        collection_name = 'scheduled_operational_flights'
    elif gz_name.endswith("_updSchedD1.json.gz") :
        collection_name = 'update_scheduled_d1_operational_flights'
    else:
        collection_name = 'historic_operational_flights'
    return collection_name
        


