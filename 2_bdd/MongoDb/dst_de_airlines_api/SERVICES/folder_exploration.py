
import os
from dotenv import load_dotenv


def is_gz_file(file_name):
 
    if file_name.endswith(".gz"):
        return True
    else :
        print("Not compressed file")
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

def remove_file(folder_path, file_name):

    os.remove(folder_path+file_name)



