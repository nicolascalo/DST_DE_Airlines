from REPOSITORIES.flights import count_flight
from REPOSITORIES.collections import get_all_collection_name
from REPOSITORIES.compressed_file_name import count_compressed_file_name

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