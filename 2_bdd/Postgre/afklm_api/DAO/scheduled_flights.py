from CONNECTION.db_context import mongo_db_connect
from CONNECTION.check_database_connection import check_db_connection
from fastapi import HTTPException
from datetime import datetime
from zoneinfo import ZoneInfo

collection = "historic_flights"

def get_(flight):
  