
### json to json.gzip


import shutil
import os

path_data_storage = "data"
path_data_gzip = "data"

file_list = os.listdir(path_data_storage)
json_list = [file for file in file_list if ('.json'  in file) & ('.gz' not in file) ]

for file in json_list:

    with open('data/'+file, 'rb') as f_in:
        with gzip.open(f"{path_data_storage}/"+file+".gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
            
            
    os.remove('data/'+file)
            

