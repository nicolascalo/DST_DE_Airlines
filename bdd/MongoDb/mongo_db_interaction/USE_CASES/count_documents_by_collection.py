from mongo_db_interaction.REPOSITORIES.flights import count_flight
from mongo_db_interaction.REPOSITORIES.compressed_file_name import count_compressed_file_name

def count_documents_by_collection():
    flights = count_flight()
    compressed_file_name = count_compressed_file_name()

    nb_documents_by_colleciton = {"flights": flights, "compressed_file_name": compressed_file_name}

    return nb_documents_by_colleciton