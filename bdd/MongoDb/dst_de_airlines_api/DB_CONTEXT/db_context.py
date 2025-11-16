from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, ServerSelectionTimeoutError, InvalidURI
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)

load_dotenv()

try:
    mongo_db_connect = MongoClient(os.getenv('MONGODB_URI'))[os.getenv('DATABASE_NAME')]
except (ConnectionFailure, ServerSelectionTimeoutError) as e:

    logger.warning(f"⚠️ MongoDB non disponible au démarrage: {e}")
    mongo_db_connect = None
except ConfigurationError as e:

    logger.error(f"❌ Erreur de configuration MongoDB: {e}")
    mongo_db_connect = None
except Exception as e:

    logger.error(f"❌ Erreur inattendue lors de la connexion MongoDB: {e}")
    mongo_db_connect = None

