from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os
from USE_CASES.download_data import download_data


load_dotenv()


folder_path = os.getenv('DATA_INPUT')



app = FastAPI(
    title="AFKLM API",
    version="1.0.0",
    docs_url="/"
)

@app.get("/download_historic_flights",
         description=f"Download a file named afklm_historic_from_mongo.csv.tar.gz in {folder_path}"
)
def download_historic_flights():
    

    route = "historic/export"

    file_name = "afklm_historic_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="historic flights not found")
    return {'message': file_name + 'saved in' + path}



@app.get("/download_update_scheduled_d1_flights")
def download_update_d1_flights():

    route = "/update_scheduled_d1/export"

    file_name = "afklm_d1_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="update scheduled d1 flights not found")
    return {'message': file_name + 'saved in' + path}



@app.get("/download_scheduled_flights")
def download_scheduled_flights():

    route = "/scheduled/export"

    file_name = "afklm_scheduled_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="update scheduled d1 flights not found")
    return {'message': file_name + 'saved in' + path}


