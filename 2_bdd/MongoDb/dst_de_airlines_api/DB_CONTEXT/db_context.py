from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, ServerSelectionTimeoutError, InvalidURI
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)

load_dotenv()

try:
    client = MongoClient(
        os.getenv('MONGODB_URI'),
        serverSelectionTimeoutMS=5000  # Timeout de 5 secondes
    )
    # ✅ Force une vraie connexion avec un ping
    client.admin.command('ping')
    mongo_db_connect = client[os.getenv('DATABASE_NAME')]
    logger.info("✅ MongoDB connected")
except InvalidURI as e:
    logger.error(f"❌ Invalid MongoDB URI : {e}")
    mongo_db_connect = None
except (ConnectionFailure, ServerSelectionTimeoutError) as e:
    logger.warning(f"⚠️ MongoDb no available at the starting: {e}")
    mongo_db_connect = None
except ConfigurationError as e:
    logger.error(f"❌ Cofiguration Error MongoDb {e}")
    mongo_db_connect = None
except Exception as e:
    logger.error(f"❌ Unexpected error during MongoDB connection: {e}")
    mongo_db_connect = None