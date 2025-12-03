from CONNECTION.dst_airlines_api_context import check_api_connection, dst_airlines_api_url
import requests
from datetime import datetime
from zoneinfo import ZoneInfo



def get_historic_flights(route):
  check_api_connection()
  response = requests.get(dst_airlines_api_url+route)
  print(response.status_code)
  

  if response.status_code == 200:
        return response.content  
  if response.status_code == 404:
       return None
  else:
        raise Exception(f"Error {response.status_code}: {response.text}")
