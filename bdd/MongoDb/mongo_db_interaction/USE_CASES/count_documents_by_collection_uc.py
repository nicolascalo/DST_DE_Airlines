from mongo_db_interaction.REPOSITORIES.flights import count_flight
from mongo_db_interaction.REPOSITORIES.collections import get_all_collection_name
from mongo_db_interaction.REPOSITORIES.compressed_file_name import count_compressed_file_name

def count_documents_by_collection():

    collection_names = get_all_collection_name()

    nb_documents_by_colleciton = {}
    for collection_name in collection_names:

        if collection_name != 'compressed_file_names':
            nb = count_flight(collection_name)
        else :
            nb = count_compressed_file_name()

        nb_documents_by_colleciton[collection_name] = nb


    

   

    return nb_documents_by_colleciton