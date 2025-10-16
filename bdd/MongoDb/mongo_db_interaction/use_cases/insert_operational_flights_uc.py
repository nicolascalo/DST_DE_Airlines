from ..services.exploitation_json import count_documents_in_json
from ..repositories.operational_flights import insert_one, insert_many
from ..services.folder_exploration import get_folder_path_in_env




def insert(json_file):
    if is_many_objects_in_file(json_file) == True:
        insert_many(json_file)
    else:
        insert_one(json_file)


def is_many_objects_in_file(json_file):
    if count_documents_in_json(json_file) > 1:
        return True
    else :
        return False

