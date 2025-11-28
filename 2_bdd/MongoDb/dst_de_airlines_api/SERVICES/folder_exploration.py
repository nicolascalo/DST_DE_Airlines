
import os
from dotenv import load_dotenv
from CONNECTION.check_gcp_connection import check_gcp_connection


def is_gz_file(file_name):
 
    if file_name.endswith(".gz"):
        return True
    else :
        print("Not .gz file")
        return False
    

def get_extension(file_name):
    _, extension = os.path.splitext(file_name)
    return extension.lower()


def get_folder_path_in_env():
    load_dotenv()
    folder_path = os.getenv('FOLDER_PATH')
    return folder_path


def get_file_names_by_folder(folder_path):
    file_names = os.listdir(folder_path)
    return file_names


def is_file_already_in_folder(folder_path, file_name):
    print(folder_path)
    file_name = file_name.replace("data/", "")
   
    path = folder_path + file_name
    print(path)
    if os.path.isfile(path):
        print("file already in folder")
        return True
    else:
        return False

def get_file_names_on_gcp():
    bucket = check_gcp_connection()
    prefix = "data/"
    blobs = bucket.list_blobs(prefix=prefix)
    file_names = []
    for blob in blobs:
        file_names.append(blob.name)
    return file_names



def remove_file(folder_path, file_name):

    os.remove(folder_path+file_name)


    




