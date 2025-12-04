from CONNECTION.check_gcp_connection import check_gcp_connection
from SERVICES.folder_exploration import get_file_names_on_gcp, get_folder_path_in_env, is_gz_file, is_file_already_in_folder
from SERVICES.exploration_gz_file import download_gz_file_on_gcp


def import_gz_files_from_gcp():

    check_gcp_connection()
  
    
    file_names = get_file_names_on_gcp()
    folder_path = get_folder_path_in_env()



    for file_name in file_names:
       
        if is_gz_file(file_name) == True:
           
            if is_file_already_in_folder(folder_path, file_name) == False:
                download_gz_file_on_gcp(folder_path, file_name)

import_gz_files_from_gcp()