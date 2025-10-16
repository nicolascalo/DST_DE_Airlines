
import os
from dotenv import load_dotenv


def get_folder_path_in_env():
    load_dotenv()
    folder_path = os.getenv('FOLDER_PATH')
    return folder_path


def get_name_files_by_folder(file):
    files = os.listdir(file)
    return files




