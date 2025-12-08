from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
import tarfile
from datetime import datetime
from zoneinfo import ZoneInfo
import subprocess
from SERIALIZER.utils import mongo_to_json
import gzip
import io
from dotenv import load_dotenv
from typing import Optional
import os
import re
from USE_CASES.get_by_id_uc import get_flight_by_id
from USE_CASES.count_documents_by_collection_uc import count_documents_by_collection
from USE_CASES.get_df_flights_uc import get_df_flights
from SERVICES.exploration_gz_file import create_csv_tar_gz
from pydantic import BaseModel, create_model
from typing import Any



app = FastAPI(
    title="Air France KLM - MongoDB API",
    description="REST API for querying and exporting Air France-KLM flight data from Europe's 30 largest airports",
    version="1.0.0",
    docs_url="/",
    openapi_tags=[
        {
            'name': 'historic',
            'description': 'Query and export historic flight data with European airport filters'
        },
        {
            'name': 'scheduled',
            'description': 'Query and export scheduled flight data'
        },
        {
            'name': 'scheduled d1',
            'description': 'Query and export D1 scheduled flights (updated schedules)'
        },
        {
            'name': 'database',
            'description': 'Database operations: statistics, exports, and dumps'
        }
    ]
)


@app.get("/historic/with_id{id}", tags=['historic'])
def get_historic_by_id(id: str):
    collection_name = "historic_flights"
    flight = get_flight_by_id(collection_name, id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)


@app.get("/scheduled/with_id{id}", tags=['scheduled'])
def get_scheduled_by_id(id: str):
    collection_name = "scheduled_flights"
    flight = get_flight_by_id(collection_name, id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)



@app.get("/update_scheduled_d1/with_id{id}", tags=['scheduled d1'])
def get_scheduled_d1_by_id(id: str):
    collection_name = "update_scheduled_d1_flights"
    flight = get_flight_by_id(collection_name, id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)


    

@app.get("/remove_scheduled/export", tags=['scheduled'])
def export_removed_scheduled(
    date: Optional[str] = Query(
        ...,
        description="Date filter in format YYYYMMDD-HH-MM-SS (French time - Europe/Paris)",
        example="20251114-15-30-45",
        regex=r"^\d{8}-\d{2}-\d{2}-\d{2}$"
    )
):
    """
    Export removed scheduled flights inserted after a given date as gzipped CSV.
    
    Args:
        date: Date filter (YYYYMMDD-HH-MM-SS, French time)
    
    Returns:
        Gzipped CSV file containing removed flights data
    """

    if date is not None:
        try:
            parsed_date = datetime.strptime(date, "%Y%m%d-%H-%M-%S")
            now_paris = datetime.now(ZoneInfo("Europe/Paris"))
            
            if parsed_date.replace(tzinfo=ZoneInfo("Europe/Paris")) > now_paris:
                raise HTTPException(
                    status_code=400,
                    detail="Date cannot be in the future"
                )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYYMMDD-HH-MM-SS"
            )

    collection_name = "removed_scheduled_flights"
    id = None
    nb_flights = None

    df, filename = get_df_flights(collection_name, date, id, nb_flights)
    
    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )

    tar_filename = create_csv_tar_gz(df, filename)
    return StreamingResponse(
        iter([tar_filename]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename.replace('.csv', '.tar.gz')}"
        }
    )
    

@app.get("/removed_scheduled_d1/export", tags=['scheduled d1'])
def export_removed_d1_flights(
    date: Optional[str] = Query(
        None,
        description="Date filter in format YYYYMMDD-HH-MM-SS (French time - Europe/Paris)",
        example="20251114-15-30-45",
        regex=r"^\d{8}-\d{2}-\d{2}-\d{2}$"
    )
):
    """
    Export removed scheduled D1 flights inserted after a given date as gzipped CSV.
    
    Args:
        date: Date filter (YYYYMMDD-HH-MM-SS, French time)
    
    Returns:
        Gzipped CSV file containing removed flights data
    """

    if date is not None:

        try:
            parsed_date = datetime.strptime(date, "%Y%m%d-%H-%M-%S")
            now_paris = datetime.now(ZoneInfo("Europe/Paris"))
            
            if parsed_date.replace(tzinfo=ZoneInfo("Europe/Paris")) > now_paris:
                raise HTTPException(
                    status_code=400,
                    detail="Date cannot be in the future"
                )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYYMMDD-HH-MM-SS"
            )
    
    collection_name = "removed_scheduled_flights"
    id = None
    nb_flights = None

    df, filename = get_df_flights(collection_name, date, id, nb_flights)
    
    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )

    tar_filename = create_csv_tar_gz(df, filename)
    return StreamingResponse(
        iter([tar_filename]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename.replace('.csv', '.tar.gz')}"
        }
    )

@app.get("/historic/export", tags=['historic'])
def export_historic_flights(
    limit: int = None,
    date: Optional[str] = None,
    start_id: Optional[str] = None
):
    """
    Export historic flights as a gzipped CSV file.

    Result order by id flight
    
    Args:
        limit: Number of flights to retrieve
        date: Optional date filter in format YYYYMMDD-HH-MM-SS (French time - Europe/Paris) (e.g., 20251126-17-30-45)
        start_id: Optional flight ID to start from (pagination)
    
    Returns:
        Gzipped CSV file containing flight data
    
    Examples:
        - /historic/export?limit=100
        - /historic/export?limit=100&date=20251126-17-30-45
        - /historic/export?limit=100&start_id=20250609+KL+1664
        - /historic/export?limit=100&date=20251126-17-30-45&start_id=20250609+KL+1664
    """

    if date is not None:
        if not re.match(r'^\d{8}-\d{2}-\d{2}-\d{2}$', date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYYMMDD-HH-MM-SS (French time - Europe/Paris)"
            )
    collection_name = "historic_flights"


    df, filename = get_df_flights(collection_name, date, start_id, limit)

    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )

    tar_filename = create_csv_tar_gz(df, filename)
    return StreamingResponse(
        iter([tar_filename]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename.replace('.csv', '.tar.gz')}"
        }
    )
    

@app.get("/scheduled/export", tags=['scheduled'])
def export_scheduled_flights(
    limit: int = None,
    date: Optional[str] = None,
    start_id: Optional[str] = None
):
    """
    Export scheduled flights as a gzipped CSV file.

    Result order by id flight
    
    Args:
        limit: Number of flights to retrieve
        date: Optional date filter in format YYYYMMDD-HH-MM-SS (French time - Europe/Paris) (e.g., 20251126-17-30-45)
        start_id: Optional flight ID to start from (pagination)
    
    Returns:
        Gzipped CSV file containing flight data
    
    Examples:
        - /scheduled/export?limit=100
        - /scheduled/export?limit=100&date=20251126-17-30-45
        - /scheduled/export?limit=100&start_id=20250609+KL+1664
        - /scheduled/export?limit=100&date=20251126-17-30-45&start_id=20250609+KL+1664
    """

    if date is not None:
        if not re.match(r'^\d{8}-\d{2}-\d{2}-\d{2}$', date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYYMMDD-HH-MM-SS (French time - Europe/Paris)"
            )
    collection_name = "scheduled_flights"
    df, filename = get_df_flights(collection_name, date, start_id, limit)

    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )

    tar_filename = create_csv_tar_gz(df, filename)
    return StreamingResponse(
        iter([tar_filename]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename.replace('.csv', '.tar.gz')}"
        }
    )


@app.get("/update_scheduled_d1/export", tags=['scheduled d1'])
def export_scheduled_d1_flights(
    limit: int = None,
    date: Optional[str] = None,
    start_id: Optional[str] = None
):
    """
    Export scheduled flights as a gzipped CSV file.

    Result order by id flight
    
    Args:
        limit: Number of flights to retrieve
        date: Optional date filter in format YYYYMMDD-HH-MM-SS (French time - Europe/Paris) (e.g., 20251126-17-30-45)
        start_id: Optional flight ID to start from (pagination)
    
    Returns:
        Gzipped CSV file containing flight data
    
    Examples:
        - /update_scheduled_d1/export?limit=100
        - /update_scheduled_d1/export?limit=100&date=20251126-17-30-45
        - /update_scheduled_d1/export?limit=100&start_id=20250609+KL+1664
        - /update_scheduled_d1/export?limit=100&date=20251126-17-30-45&start_id=20250609+KL+1664
    """

    if date is not None:
        if not re.match(r'^\d{8}-\d{2}-\d{2}-\d{2}$', date):
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYYMMDD-HH-MM-SS (French time - Europe/Paris)"
            )
        
    collection_name = "update_scheduled_d1_flights"
    
    df, filename = get_df_flights(collection_name, date, start_id, limit )
    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )

    tar_filename = create_csv_tar_gz(df, filename)
    return StreamingResponse(
        iter([tar_filename]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename.replace('.csv', '.tar.gz')}"
        }
    )


@app.get("/collections/stats", tags=['database'])
def get_collections_stats():
    """
    Get document count statistics for all collections.
    
    Returns:
        Dictionary with document counts per collection
    """
    return count_documents_by_collection()


@app.post("/database/dumps", tags=['database'])
def create_database_dump():
    """
    Create a new database dump.
    
    Returns:
        Gzipped MongoDB archive file
    """
    load_dotenv()
    date_time = datetime.now().strftime("%Y%m%d-%H-%M-%S")
    filename = f"dump-{date_time}.archive.gz"

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
                detail=f"Dump creation failed: {stderr.decode('utf-8', errors='replace')}"
            )

        return StreamingResponse(
            iter([stdout]),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail="mongodump is not installed"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Dump creation error: {str(e)}"
        )





        








