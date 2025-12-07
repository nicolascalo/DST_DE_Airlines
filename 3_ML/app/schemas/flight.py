from pydantic import BaseModel, Field
from typing import Optional


class PayloadFlight(BaseModel):
    """
    Parameters of the flight for which to predict delay
    """

    # --- Flight identity ---
    flight_id: Optional[str] = None
    flightnumber: Optional[int] = None
    airline_code: Optional[str] = None
    airline_name: Optional[str] = None

    # --- Aircraft & service ---
    flightlegs_aircraft_typecode: Optional[str] = None
    flightlegs_servicetypename: Optional[str] = None
    flightlegs_aircraft_ownerairlinecode: Optional[str] = None

    # --- Departure airport ---
    flightlegs_depinfo_airport_continent_name: Optional[str] = None
    flightlegs_depinfo_airport_subcontinent_name: Optional[str] = None
    flightlegs_depinfo_airport_country_code: Optional[str] = None
    flightlegs_depinfo_airport_country_name: Optional[str] = None
    flightlegs_depinfo_airport_airport_name: Optional[str] = None
    flightlegs_depinfo_airport_code: str = Field(..., description="3-letter IATA code of departure airport", example="CDG",min_length=3, max_length=3 )

    flightlegs_depinfo_airport_places_depposterm_boardingterminal: Optional[str] = None
    flightlegs_depinfo_airport_places_depposterm_gatenumber: Optional[str] = None

    # --- Departure time ---
    flightlegs_depinfo_times_scheduled_date: str
    flightlegs_depinfo_times_scheduled_time: str
    flightlegs_depinfo_times_scheduled_year: Optional[int] = None
    flightlegs_depinfo_times_scheduled_month: Optional[int] = None
    flightlegs_depinfo_times_scheduled_day: Optional[int] = None
    flightlegs_depinfo_times_scheduled_hour: Optional[int] = None
    flightlegs_depinfo_times_scheduled_minute: Optional[int] = None
    flightlegs_depinfo_times_scheduled_timezone: str
    flightlegs_depinfo_times_number_week: Optional[int] = None

    # --- Arrival airport ---
    flightlegs_arrinfo_airport_continent_name: Optional[str] = None
    flightlegs_arrinfo_airport_subcontinent_name: Optional[str] = None
    flightlegs_arrinfo_airport_country_code: Optional[str] = None
    flightlegs_arrinfo_airport_country_name: Optional[str] = None
    flightlegs_arrinfo_airport_airport_name: Optional[str] = None
    flightlegs_arrinfo_airport_code: str = Field(..., description="3-letter IATA code of arrival airport", example="AMS",min_length=3, max_length=3 )

    flightlegs_arrinfo_airport_places_arrivalpositionterminal: Optional[str] = None

    # --- Arrival time ---
    flightlegs_arrinfo_times_scheduled_date: str
    flightlegs_arrinfo_times_scheduled_time: str
    flightlegs_arrinfo_times_scheduled_year: Optional[int] = None
    flightlegs_arrinfo_times_scheduled_month: Optional[int] = None
    flightlegs_arrinfo_times_scheduled_day: Optional[int] = None
    flightlegs_arrinfo_times_scheduled_hour: Optional[int] = None
    flightlegs_arrinfo_times_scheduled_minute: Optional[int] = None
    flightlegs_arrinfo_times_scheduled_timezone: str
    flightlegs_arrinfo_times_number_week: Optional[int] = None

    class Config:
        extra = "allow"
        json_schema_extra = {
            "example": {
                "flightlegs_aircraft_ownerairlinecode":"KL",
                "flight_id": "690c974f228ea0580c98a8be",
                "flightnumber": 7612,
                "airline_code": "G3",
                "airline_name": "GOL LINHAS AEREAS S.A.",
                "flightlegs_aircraft_typecode": "7M8",
                "flightlegs_servicetypename": "Normal Service",
                "flightlegs_depinfo_airport_code": "GIG",
                "flightlegs_depinfo_times_scheduled_date": "2025-05-16",
                "flightlegs_depinfo_times_scheduled_time": "22:30",
                "flightlegs_depinfo_times_scheduled_timezone": "-03:00",
                "flightlegs_arrinfo_airport_code": "COR",
                "flightlegs_arrinfo_times_scheduled_date": "2025-05-17",
                "flightlegs_arrinfo_times_scheduled_time": "02:20",
                "flightlegs_arrinfo_times_scheduled_timezone": "-03:00"
            }
        }
