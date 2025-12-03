from fastapi import HTTPException
from requests.exceptions import ConnectionError, Timeout, RequestException
import requests
import os
from dotenv import load_dotenv

load_dotenv()
dst_airlines_api_url = os.getenv('API_DST_AIRLINES_URL')

if not dst_airlines_api_url:
    raise ValueError("Check environment variable")


def check_api_connection():
    try:
        print(f"Attempting connection to : {dst_airlines_api_url}")
        
        response = requests.get(dst_airlines_api_url, timeout=5)
        response.raise_for_status()
        print("Success connection to dst airlines api")
        return True
      
    except ConnectionError as e:
        print(f"Error connection API: Impossible to connect dst airlines api {dst_airlines_api_url}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: Cannot reach API at {dst_airlines_api_url}"
        ) from e 
    
    except Timeout as e:
        print(f"Timeout: No response from dst airlines API")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: API timeout at {dst_airlines_api_url}"
        ) from e

    except requests.exceptions.HTTPError as e:
        print(f"HTTP ERROR {response.status_code}")
        raise HTTPException(
            status_code=502,
            detail=f"Bad gateway: Remote API returned {response.status_code}"
        ) from e
    
    except RequestException as e:
        print(f"Request error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error while connecting to remote API"
        ) from e