from ..services.exploitation_json import count_json
from ..repositories.operational_flights import insert_one, insert_many



def insert(file):
    if is_many_objects_in_file(file) == True:
        insert_many(file)
    else:
        insert_one(file)




def is_many_objects_in_file(file):
    if count_json(file) > 1:
        return True
    else :
        return False


insert("/home/johan/Documents/Formation/Projet/Recherche_Mongo/afklm_api_data_collection_arrivalCity=DXB_0.json")