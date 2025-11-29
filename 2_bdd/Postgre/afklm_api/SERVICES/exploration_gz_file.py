import json
import zipfile
import re
import os
import io
import tarfile
import gzip



def rename_tar_gz(tar_gz, file_name):
    original_tar_gz = io.BytesIO(tar_gz)
    with tarfile.open(fileobj=original_tar_gz, mode='r:gz') as tar:
        csv_member = tar.getmembers()[0]  
        csv_data = tar.extractfile(csv_member).read()
    

    new_tar = io.BytesIO()
    with tarfile.open(fileobj=new_tar, mode='w:gz') as tar:

        csv_info = tarfile.TarInfo(name=f"{file_name}.csv")
        csv_info.size = len(csv_data)
        
       
        tar.addfile(csv_info, io.BytesIO(csv_data))
    
    return new_tar.getvalue()
     


    


        


