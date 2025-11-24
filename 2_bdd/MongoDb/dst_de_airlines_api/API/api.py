from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from datetime import datetime
from zoneinfo import ZoneInfo
import subprocess
from SERIALIZER.utils import mongo_to_json
import gzip
import io
from USE_CASES.get_by_id_historic_uc import get_by_id_historic_flight
from USE_CASES.count_documents_by_collection_uc import count_documents_by_collection
from USE_CASES.get_all_csv_uc import get_flights_to_csv
from USE_CASES.get_historic_csv_uc import get_historic_flights_to_csv
from USE_CASES.get_schedulled_csv_uc import get_schedulled_flights_to_csv
from USE_CASES.get_update_d1_csv_uc import get_update_d1_csv
from USE_CASES.get_sch_removed_csv_uc import get_removed_sch_flights_to_csv
from USE_CASES.get_d1_removed_csv_uc import get_d1_removed_to_csv
from dotenv import load_dotenv
import os





app = FastAPI(
    title ="AirlinesApi",
    docs_url="/docs",
    openapi_tags=[
    {
        'name': 'historic',
        'description': 'request on the historic flights'
    },
    {
        'name': 'scheduled',
        'description': 'request on the scheduled flights'
    },
        {
        'name': 'scheduled d1',
        'description': 'request on the scheduled d1 flights'
    },
            {
        'name': 'all',
        'description': 'request all the flights and all documents'
    },
    
])


@app.get("/historic_flight/json/with_id{id}", tags=['historic'])
def read_flight(id: str):
    flight = get_by_id_historic_flight(id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)

@app.get(
    "/flights/csv/with_date",
    summary="Télécharger les vols en CSV filtrés par date",
    description="""
    Get inserted flights afer a given date and export them in compressed CSV (gzip).
    
    **Required format:** `YYYYMMDD-HH-MM-SS` (French time - Europe/Paris)
    
    **Valid example :**
    - `20251114-15-30-45` 
    - `20250101-00-00-00` 
    
    **Note:** Date in French time and automatic gesture for summer/winter 
    """,
    responses={
        200: {
            "description": "Compressed CSV containing the fliths",
            "content": {"application/gzip": {}},
        },
        400: {"description": "Invalid format date"},
        404: {"description": "Not found flights for this date"},
        500: {"description": "Internal error"},
    },
    tags = ['all']
)
def read_flight(
    date: str = Query(
        ...,
        description="Format Date YYYYMMDD-HH-MM-SS (french time)",
        example="20251114-15-30-45",
        regex="^[0-9]{8}-[0-9]{2}-[0-9]{2}-[0-9]{2}$"
    )
):
    """
    Endpoint to download the flights filtered with date.
    """
    

    try:
        parsed_date = datetime.strptime(date, "%Y%m%d-%H-%M-%S")
        

        now_paris = datetime.now(ZoneInfo("Europe/Paris"))
        if parsed_date.replace(tzinfo=ZoneInfo("Europe/Paris")) > now_paris:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid Date",
                    "message": "The date can't be from the futur",
                    "required format": "YYYYMMDD-HH-MM-SS",
                    "example": "20251114-15-30-45",
                }
            )
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid format date",
                "message": "Required format date YYYYMMDD-HH-MM-SS",
                "format_attendu": "YYYYMMDD-HH-MM-SS",
                "exemple": "20251114-15-30-45",
                "valeur_recue": date,
                "erreur_technique": str(e),
            }
        )
    

    try:
        df, filename = get_flights_to_csv(date)
        

        if df is None or df.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Data not found",
                    "message": f"Not inserted flight after  {date}",
                    "filter_date": date,
                }
            )
        
    except HTTPException:
   
        raise
    except Exception as e:
   
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal error",
                "message": "An error occurred while retrieving the flights",
                "technical_error": str(e),
            }
        )
    

    try:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
            df.to_csv(f, index=False, na_rep="")
        csv_content = buffer.getvalue()
        
        if not csv_content:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Compressed Error",
                    "message": "The CSV file could not be generated correctly.",
                }
            )
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(csv_content)),
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Compressed error",
                "message": "The CSV file could not be generated correctly.",
                "erreur_technique": str(e),
            }
        )
    

@app.get(
    "/removed/scheduled/flights/csv/with_date",
    summary="Download the removed scheduled flights in CSV filtered by inserted date",
    description="""
    Get inserted removed scheduled flights afer a given date and export them in compressed CSV (gzip).
    
    **Required format:** `YYYYMMDD-HH-MM-SS` (French time - Europe/Paris)
    
    **Valid example :**
    - `20251114-15-30-45` 
    - `20250101-00-00-00` 
    
    **Note:** Date in French time and automatic gesture for summer/winter 
    """,
    responses={
        200: {
            "description": "Compressed CSV containing the fliths",
            "content": {"application/gzip": {}},
        },
        400: {"description": "Invalid format date"},
        404: {"description": "Data not found"},
        500: {"description": "Internal error"},
    },
    tags = ['scheduled']
)
def read_flight(
    date: str = Query(
        ...,
        description="Format Date YYYYMMDD-HH-MM-SS (french time)",
        example="20251114-15-30-45",
        regex="^[0-9]{8}-[0-9]{2}-[0-9]{2}-[0-9]{2}$"
    )
):
    """
    Endpoint to download the flights filtered with date.
    """
    

    try:
        parsed_date = datetime.strptime(date, "%Y%m%d-%H-%M-%S")
        

        now_paris = datetime.now(ZoneInfo("Europe/Paris"))
        if parsed_date.replace(tzinfo=ZoneInfo("Europe/Paris")) > now_paris:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid Date",
                    "message": "The date can't be from the futur",
                    "required format": "YYYYMMDD-HH-MM-SS",
                    "example": "20251114-15-30-45",
                }
            )
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid format date",
                "message": "Required format: YYYYMMDD-HH-MM-SS",
                "format_attendu": "YYYYMMDD-HH-MM-SS",
                "exemple": "20251114-15-30-45",
                "valeur_recue": date,
                "erreur_technique": str(e),
            }
        )
    

    try:
        df, filename = get_removed_sch_flights_to_csv(date)
        

        if df is None or df.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Not found flights",
                    "message": f"Not inserted flight after  {date}",
                    "filter_date": date,
                }
            )
        
    except HTTPException:
   
        raise
    except Exception as e:
   
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal error",
                "message": "An error occurred while retrieving the flights",
                "technical_error": str(e),
            }
        )
    

    try:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
            df.to_csv(f, index=False, na_rep="")
        csv_content = buffer.getvalue()
        
        if not csv_content:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Compressed Error",
                    "message": "The CSV file could not be generated correctly.",
                }
            )
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(csv_content)),
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Compressed error",
                "message": "The CSV file could not be generated correctly.",
                "erreur_technique": str(e),
            }
        )
    
    

@app.get(
    "/removed/update/d1/flights/csv/with_date",
    summary="Download the removed scheduled flights in CSV filtered by inserted date",
    description="""
    Get inserted removed scheduled flights afer a given date and export them in compressed CSV (gzip).
    
    **Required format:** `YYYYMMDD-HH-MM-SS` (French time - Europe/Paris)
    
    **Valid example :**
    - `20251114-15-30-45` 
    - `20250101-00-00-00` 
    
    **Note:** Date in French time and automatic gesture for summer/winter 
    """,
    responses={
        200: {
            "description": "Compressed CSV containing the fliths",
            "content": {"application/gzip": {}},
        },
        400: {"description": "Invalid format date"},
        404: {"description": "Data not found"},
        500: {"description": "Internal error"},
    },
    tags = ['scheduled d1']
)
def read_flight(
    date: str = Query(
        ...,
        description="Format Date YYYYMMDD-HH-MM-SS (french time)",
        example="20251114-15-30-45",
        regex="^[0-9]{8}-[0-9]{2}-[0-9]{2}-[0-9]{2}$"
    )
):
    """
    Endpoint to download the flights filtered with date.
    """
    

    try:
        parsed_date = datetime.strptime(date, "%Y%m%d-%H-%M-%S")
        

        now_paris = datetime.now(ZoneInfo("Europe/Paris"))
        if parsed_date.replace(tzinfo=ZoneInfo("Europe/Paris")) > now_paris:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid Date",
                    "message": "The date can't be from the futur",
                    "required format": "YYYYMMDD-HH-MM-SS",
                    "example": "20251114-15-30-45",
                }
            )
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid format date",
                "message": "Required format: YYYYMMDD-HH-MM-SS",
                "format_attendu": "YYYYMMDD-HH-MM-SS",
                "exemple": "20251114-15-30-45",
                "valeur_recue": date,
                "erreur_technique": str(e),
            }
        )
    

    try:
        df, filename = get_d1_removed_to_csv(date)
        

        if df is None or df.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Not found flights",
                    "message": f"Not inserted flight after  {date}",
                    "filter_date": date,
                }
            )
        
    except HTTPException:
   
        raise
    except Exception as e:
   
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal error",
                "message": "An error occurred while retrieving the flights",
                "technical_error": str(e),
            }
        )
    

    try:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
            df.to_csv(f, index=False, na_rep="")
        csv_content = buffer.getvalue()
        
        if not csv_content:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Compressed Error",
                    "message": "The CSV file could not be generated correctly.",
                }
            )
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(csv_content)),
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Compressed error",
                "message": "The CSV file could not be generated correctly.",
                "erreur_technique": str(e),
            }
        )



@app.get("/historic_flights/csv/with_nb_limit_flights{nb_limit_flights}", tags = ['historic'])
def read_flight(nb_limit_flights:int):
    df, filename = get_historic_flights_to_csv(nb_limit_flights)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    if csv_content is None:
        raise HTTPException(status_code=404, detail="flight not found")


    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )

@app.get("/scheduled_flights/csv/with_nb_limit_flights{nb_limit_flights}", tags = ['scheduled'])
def read_flight(nb_limit_flights:int):
    df, filename = get_schedulled_flights_to_csv(nb_limit_flights)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    if csv_content is None:
        raise HTTPException(status_code=404, detail="flight not found")


    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@app.get("/update_schedulae_d1_flights/csv/with_nb_limit_flights{nb_limit_flights}", tags = ['scheduled d1'])
def read_flight(nb_limit_flights:int):
    df, filename = get_update_d1_csv(nb_limit_flights)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        df.to_csv(f, index=False, na_rep="")
    csv_content = buffer.getvalue()
    if csv_content is None:
        raise HTTPException(status_code=404, detail="flight not found")


    return StreamingResponse(
        iter([csv_content]),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@app.get("/collections/count_documents", tags = ['all'])
def count_doc():
    count_doucments_by_collection = count_documents_by_collection()
    return count_doucments_by_collection


@app.get("/dump/all",   tags = ['all'])
def get_full_dump():
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
                detail=f"Erreur mongodump : {stderr.decode('utf-8', errors='replace')}"
            )

        # Le stdout contient déjà les données compressées grâce à --gzip
        return StreamingResponse(
            iter([stdout]),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="mongodump not installed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")


    



