from queue import Full
from ..services.folder_exploration import create_file_name_collection
from ..repositories.file_names_collection import insert_one, get_by_name

def insert_name_file_collection(file_name):
    if is_file_name_exist(file_name) == False:
        file_name_collection = create_file_name_collection(file_name)
        insert_one(file_name_collection)




def is_file_name_exist(file_name):
    file_name_collection = get_by_name(file_name)
    if file_name_collection == None:
        return False
    else :
        print(file_name+ "already in database")
        return True

