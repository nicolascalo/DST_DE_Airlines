from DAO.historic_flights import get_historic_flights 
from SERVICES.exploration_gz_file import rename_tar_gz
from SERVICES.folder_exploration import save_tar_gz_in_folder

def download_data(file_name, route):
    tar_gz = get_historic_flights(route)

    renamed_tar_gz = rename_tar_gz(tar_gz, file_name)

    path = save_tar_gz_in_folder(renamed_tar_gz, file_name)

    return path
    