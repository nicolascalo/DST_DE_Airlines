from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from datetime import datetime
from zoneinfo import ZoneInfo
import subprocess
from SERIALIZER.utils import mongo_to_json
import gzip
import io
from mongo_db_interaction.USE_CASES.get_by_id_historic_flights_uc import get_by_id_historic_flight
from mongo_db_interaction.USE_CASES.count_documents_by_collection_uc import count_documents_by_collection
from mongo_db_interaction.USE_CASES.get_all_flights_csv_uc import get_flights_to_csv
from mongo_db_interaction.USE_CASES.get_historic_flights_csv_uc import get_historic_flights_to_csv
from mongo_db_interaction.USE_CASES.get_schedulled_flights_csv_uc import get_schedulled_flights_to_csv
from mongo_db_interaction.USE_CASES.get_update_d1_flights_csv_uc import get_update_d1_csv
from dotenv import load_dotenv
import os



app = FastAPI(
    title ="AirlinesApi"
)


@app.get("/historic_flight/json/with_id{id}")
def read_flight(id: str):
    flight = get_by_id_historic_flight(id)

    if flight is None:
        raise HTTPException(status_code=404, detail="flight not found")
    return mongo_to_json(flight)

@app.get(
    "/flights/csv/with_date",
    summary="Télécharger les vols en CSV filtrés par date",
    description="""
    Récupère les vols insérés après une date donnée et les exporte en CSV compressé (gzip).
    
    **Format de date attendu:** `YYYYMMDD-HH-MM-SS` (heure française - Europe/Paris)
    
    **Exemples valides:**
    - `20251114-15-30-45` (14 novembre 2025 à 15h30:45)
    - `20250101-00-00-00` (1er janvier 2025 à minuit)
    
    **Note:** La date est en heure française et gère automatiquement l'heure d'été/hiver.
    """,
    responses={
        200: {
            "description": "Fichier CSV compressé contenant les vols",
            "content": {"application/gzip": {}},
        },
        400: {"description": "Format de date invalide"},
        404: {"description": "Aucun vol trouvé pour cette date"},
        500: {"description": "Erreur serveur"},
    },
)
def read_flight(
    date: str = Query(
        ...,
        description="Date au format YYYYMMDD-HH-MM-SS (heure française)",
        example="20251114-15-30-45",
        regex="^[0-9]{8}-[0-9]{2}-[0-9]{2}-[0-9]{2}$"
    )
):
    """
    Endpoint pour télécharger les vols filtrés par date d'insertion.
    """
    

    try:
        parsed_date = datetime.strptime(date, "%Y%m%d-%H-%M-%S")
        

        now_paris = datetime.now(ZoneInfo("Europe/Paris"))
        if parsed_date.replace(tzinfo=ZoneInfo("Europe/Paris")) > now_paris:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Date invalide",
                    "message": "La date ne peut pas être dans le futur",
                    "format_attendu": "YYYYMMDD-HH-MM-SS",
                    "exemple": "20251114-15-30-45",
                }
            )
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Format de date invalide",
                "message": "Le format de date attendu est YYYYMMDD-HH-MM-SS",
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
                    "error": "Aucun vol trouvé",
                    "message": f"Aucun vol n'a été inséré après la date {date}",
                    "date_filtree": date,
                }
            )
        
    except HTTPException:
   
        raise
    except Exception as e:
   
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Erreur serveur",
                "message": "Une erreur est survenue lors de la récupération des vols",
                "erreur_technique": str(e),
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
                    "error": "Erreur de compression",
                    "message": "Le fichier CSV n'a pas pu être généré correctement",
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
                "error": "Erreur de compression",
                "message": "Une erreur est survenue lors de la compression du CSV",
                "erreur_technique": str(e),
            }
        )


@app.get("/historic_flights/csv/with_nb_limit_flights{nb_limit_flights}")
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

@app.get("/scheduled_flights/csv/with_nb_limit_flights{nb_limit_flights}")
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


@app.get("/update_schedulae_d1_flights/csv/with_nb_limit_flights{nb_limit_flights}")
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


@app.get("/collections/count_documents")
def count_doc():
    count_doucments_by_collection = count_documents_by_collection()
    return count_doucments_by_collection


@app.get("/dump/all")
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


    



