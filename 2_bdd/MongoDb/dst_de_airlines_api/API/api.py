from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from datetime import datetime
from zoneinfo import ZoneInfo
import subprocess
from SERIALIZER.utils import mongo_to_json
import gzip
import io
from USE_CASES.get_historic_by_id_uc import get_historic_by_id
from USE_CASES.count_documents_by_collection_uc import count_documents_by_collection
from USE_CASES.get_csv_historic_uc import get_csv_historic_by_id
from USE_CASES.get_schedulled_csv_uc import get_csv_schedulled_by_id
from USE_CASES.get_csv_update_d1_uc import get_csv_update_d1_by_id
from USE_CASES.get_csv_sch_removed_uc import get_csv_removed_sch
from USE_CASES.get_csv_d1_removed_uc import get_csv_d1_removed
from dotenv import load_dotenv
from typing import Optional
import os
import re





app = FastAPI(
    title="Airlines API",
    description="REST API for querying and exporting Air France-KLM flight data from Europe's 30 largest airports",
    version="1.0.0",
    docs_url="/docs",
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
def get_by_id(id: str):
    flight = get_historic_by_id(id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)


    

@app.get("/remove_scheduled/export", tags=['scheduled'])
def export_removed_scheduled(
    date: str = Query(
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


    df, filename = get_csv_removed_sch(date)
    
    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )
    

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    
    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )
    

@app.get("/removed_scheduled_d1/export", tags=['scheduled d1'])
def export_removed_d1_flights(
    date: str = Query(
        ...,
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


    df, filename = get_csv_d1_removed(date)
    
    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No flights found after {date}"
        )
    

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    
    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )




@app.get("/historic/export", tags=['historic'])
def export_historic_flights(
    limit: int,
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
    
    df, filename = get_csv_historic_by_id(limit, start_id, date)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    
    if csv_content is None:
        raise HTTPException(status_code=404, detail="No flights found")

    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )

@app.get("/scheduled/export", tags=['scheduled'])
def export_scheduled_flights(
    limit: int,
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
    
    df, filename = get_csv_schedulled_by_id(limit, start_id, date)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    
    if csv_content is None:
        raise HTTPException(status_code=404, detail="No flights found")

    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )





@app.get("/update_scheduled_d1/export", tags=['scheduled d1'])
def export_scheduled_d1_flights(
    limit: int,
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
    
    df, filename = get_csv_update_d1_by_id(limit, start_id, date)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    
    if csv_content is None:
        raise HTTPException(status_code=404, detail="No flights found")

    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
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


    