from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import List, Optional

api = FastAPI()

class Item(BaseModel):
    model_config = {
    "extra": "allow"

    }
    continent_name: Optional[str] = None


class Item2(BaseModel):
    model_config = {
    "extra": "allow"

    }
    continent_name: Optional[str] = None
    subcontinent_name: Optional[str] = None
    country_name: Optional[str] = None
    location_name: Optional[str] = None
    flightlegs_depinfo_airport_code: Optional[str] = None
    airline_code: Optional[str] = None
    flight_id: Optional[str] = None


@api.post("/flight")
async def received_flight(flight: Item):
    print("it worked")
    return "test"
