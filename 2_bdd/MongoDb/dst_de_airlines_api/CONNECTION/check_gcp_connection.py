from google.cloud import storage
from io import BytesIO
import gc


bucket_name = "airfrance-bucket"


def check_gcp_connection():
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bucket.exists() 
        return bucket
    except:
        return None