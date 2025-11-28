from DAO.file_names import insert_one, get_by_name

def insert_file_name(file_name):
    if is_file_name_exist(file_name) == False:
        insert_one(file_name)

def is_file_name_exist(file_name):
    file_name_collection = get_by_name(file_name)
    if file_name_collection == None:
        return False
    else :
        print(file_name+ " Already in database")
        return True

