import pandas as pd
import dash, requests, json
from dash import Dash, dcc, html, State
from dash.dependencies import Input, Output
from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from pydantic import BaseModel
import json
from fastapi.responses import JSONResponse


df = pd.DataFrame({"index" : 0,
                   "flightLegs_depInfo_airport_code" : "CDG",
                    "flightLegs_arrInfo_airport_code" : "AMS",
                    "flightlegs_arrinfo_times_scheduled" : "CDG",
                    "flightlegs_depinfo_times_scheduled" : "CDG",
                    "flightlegs_depinfo_times_scheduleffgdfghdfgfdd" : "CDG",
                    "flightlegs_arrinfo_airport_location_longitude" : "4324",
                            "continent_name" : "test", 
    "subcontinent_name": "test", 
    "country_name": "test", 
    "location_name": "test", 
    "airport_name": "test",
    "airline_code" : "test",
    "flight_id": "test"}, index=[0])



json_tosend = df.to_dict(orient="records")
json_tosend = json_tosend[0]
#response = requests.post("http://0.0.0.0:8002/flight", json=json_tosend)
#print(response.content)


