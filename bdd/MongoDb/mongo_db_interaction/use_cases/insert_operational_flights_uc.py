from ..services.exploitation_json import count_object_json
from ..repositories.operational_flights import insert_one, insert_many
from ..services.folder_exploration import get_folder_path_in_env




def insert(file):
    if is_many_objects_in_file(file) == True:
        insert_many(file)
    else:
        insert_one(file)




def is_many_objects_in_file(file):
    if count_object_json(file) > 1:
        return True
    else :
        return False

def build_insert_path_file(file):
    folder_path = get_folder_path_in_env
    file_path = folder_path + file
    return file_path