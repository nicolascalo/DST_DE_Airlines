import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import google
import google.cloud
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")


console_handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(formatter)
logger = logging.getLogger("loading_data_into_postgresql_logger")
logger.setLevel(logging.INFO)

client = google.cloud.logging.Client(project=PROJECT_ID)
cloud_handler = CloudLoggingHandler(client)
logger.addHandler(cloud_handler)

logger.addHandler(console_handler)
logger.propagate = False