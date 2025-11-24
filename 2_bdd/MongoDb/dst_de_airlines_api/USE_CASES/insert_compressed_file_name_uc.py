from queue import Full
from REPOSITORIES.compressed_file_name import insert_one, get_by_compressed_file_name

def insert_compressed_file_name(compressed_file_name):
    if is_compressed_file_name_exist(compressed_file_name) == False:
        insert_one(compressed_file_name)



def is_compressed_file_name_exist(compressed_file_name):
    file_name_collection = get_by_compressed_file_name(compressed_file_name)
    if file_name_collection == None:
        print(compressed_file_name)
        return False
        
    else :
        print(compressed_file_name + " is already in database")
        return True