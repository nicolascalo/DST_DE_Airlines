import json
from dotenv import load_dotenv

import os


load_dotenv()

def save_tar_gz_in_folder(tar_gz, file_name):
    path = os.getenv('DATA_INPUT')

    file_path = path + file_name+'.csv.tar.gz'

    with open(file_path, 'wb') as f:
        f.write(tar_gz)

    return path



