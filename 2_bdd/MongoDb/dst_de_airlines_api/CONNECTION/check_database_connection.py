from CONNECTION.db_context import mongo_db_connect
from fastapi import HTTPException

def check_db_connection():
  
    if mongo_db_connect is None:
        raise HTTPException(
            status_code=503,
            detail="Database connection faild"
        )