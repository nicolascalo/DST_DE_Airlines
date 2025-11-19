from fastapi import FastAPI, Request
from pydantic import BaseModel

api = FastAPI()

class Item(BaseModel):
    continent_name: str
    subcontinent_name: str
    country_name: str
    location_name: str
    flightlegs_depinfo_airport_code: str
    airline_code: str
    flight_id: str

@api.post("/flight")
async def received_flight(flight: Item):
    print(f"{flight.continent_name}, {flight.subcontinent_name}, {flight.country_name}, {flight.location_name}, {flight.flightlegs_depinfo_airport_code}, {flight.airline_code}, {flight.flight_id}")
    return flight.dict()
