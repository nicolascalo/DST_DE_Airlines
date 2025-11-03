from .folder_exploration import get_folder_path_in_env
import json
import zipfile
import os
import tarfile
import gzip
import bz2
import lzma
from .folder_exploration import get_extension 




def open_compressed_file(compressed_file_name):

 
    folder_path = get_folder_path_in_env()
    compressed_file_path = folder_path + compressed_file_name

   
    with  gzip.open(compressed_file_path, 'rt', encoding='utf-8') as gz_file:
        try:
            decompressed_file = json.load(gz_file)
            return decompressed_file
        except gzip.BadGzipFile as e:

            print("corrompu: "+compressed_file_name)
            return "corrupted file"
    
        except json.JSONDecodeError as e:
            print("Json invalide : "+compressed_file_name)
            return "invalid json"

