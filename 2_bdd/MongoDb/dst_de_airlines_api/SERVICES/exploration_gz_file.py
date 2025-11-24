from .folder_exploration import get_folder_path_in_env
import json
import zipfile
import os
import tarfile
import gzip
import bz2
import lzma
from .folder_exploration import get_extension 




def get_json_in_gz_file_by_its_name(gz_file_name):

 
    folder_path = get_folder_path_in_env()
    compressed_file_path = folder_path + gz_file_name

   
    with  gzip.open(compressed_file_path, 'rt', encoding='utf-8') as gz_file:
        try:
            
            decompressed_file = json.load(gz_file)
 
            return decompressed_file
        except gzip.BadGzipFile as e:

            print("corrompu: "+gz_file_name)
            return "corrupted file"
    
        except json.JSONDecodeError as e:
            print("Json invalide : "+gz_file_name)
            return "invalid json"


def get_collection_name_by_end_gz_file_name(gz_name):
     
    collection_name = ''
    if gz_name.endswith("_sched.json.gz"):
        collection_name = 'scheduled_operational_flights'
    elif gz_name.endswith("_updSchedD1.json.gz") :
        collection_name = 'update_scheduled_d1_operational_flights'
    else:
        collection_name = 'historic_operational_flights'
    return collection_name
        


