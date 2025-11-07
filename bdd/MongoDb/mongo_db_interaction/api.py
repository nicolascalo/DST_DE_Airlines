from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import StreamingResponse
from datetime import datetime
import subprocess
from SERIALIZER.utils import mongo_to_json
from mongo_db_interaction.USE_CASES.get_by_id_flight_uc import get_by_id_flight
from mongo_db_interaction.USE_CASES.count_documents_by_collection_uc import count_documents_by_collection
from mongo_db_interaction.USE_CASES.format_for_tabular_data_uc import format_for_tabular_data
from dotenv import load_dotenv
import os



app = FastAPI(
    title ="AirlinesApi"
)


@app.get("/flights/{id}")
def read_flight(id: str):
    flight = get_by_id_flight(id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)

@app.get("/all/flights/")
def read_flight(nb_flights: int):
    csv_content, filename = format_for_tabular_data(nb_flights)
    if csv_content is None:
        raise HTTPException(status_code=404, detail="flight not found")


    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )

@app.get("/count_documents_by_collection/")
def count_doc():
    count_doucments_by_collection = count_documents_by_collection()
    return count_doucments_by_collection


@app.get("/dump/all")
def get_full_dump():
    load_dotenv()
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")  # Format : AAAAMMJJ-HHMMSS
    filename = f"dump-{date_time}.archive"


    command = [
        "mongodump",
        "--uri", os.getenv('MONGODB_URI'),
        "--db", os.getenv('DATABASE_NAME'), 
        "--archive",
        "--gzip"  
    ]

    try:
      
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate() 
        if process.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Erreur mongodump : {stderr.decode('utf-8', errors='replace')}"
            )


     
        return Response(
            content=stdout, 
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="mongodump not installed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")


    



