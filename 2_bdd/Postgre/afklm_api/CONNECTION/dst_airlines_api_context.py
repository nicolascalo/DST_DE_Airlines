import requests
from requests.exceptions import RequestException, ConnectionError, Timeout
from dotenv import load_dotenv
import os
import logging



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
      
    except ConnectionError as e:
        print(f"Error connection API: Impossible to connect dst airlines api {dst_airlines_api_url}")
        raise ConnectionError(f"API is not accessible at {dst_airlines_api_url}") from e 
    
    except Timeout as e:
        print(f"Temeout: Not response from dst airlines API")
        raise Timeout(f"Not response from API to {dst_airlines_api_url} ") from e
    

    except requests.exceptions.HTTPError as e:
        print(f" HTTP ERROR {response.status_code}")
        raise ConnectionError(f"API return error: {response.status_code}")
    
    except RequestException as e:
        print(f"Request error:: {str(e)}")
        raise


        


                                
                
