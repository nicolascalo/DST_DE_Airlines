from fastapi import HTTPException
import os
import pandas as pd
from sqlalchemy import create_engine
import traceback

def retrieve_latest_training_dataset():

    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_URI')
    port = os.getenv('POSTGRES_PORT')
    database_name = os.getenv('POSTGRES_DB')

    DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"




    query = 'select v_past_flight.flight_id,  v_past_flight.flightNumber,  v_past_flight.airline_code,  v_past_flight.airline_name,  v_past_flight.flightStatusPublic,  v_past_flight.flightLegs_aircraft_typeCode,  v_past_flight.flightLegs_scheduledFlightDuration,  v_past_flight.flightLegs_serviceType,  v_past_flight.flightLegs_aircraft_ownerAirlineCode,  v_past_flight.flightLegs_status,  v_past_flight.delay_status,  v_past_flight.flightLegs_serviceTypeName, v_past_flight.flightLegs_publishedStatus, v_past_flight.flightLegs_legStatusPublic, v_past_flight.flightLegs_statusName, v_past_flight.flightLegs_irregularity_delayDuration, v_past_flight.flightlegs_irregularity_delayduration_total, v_past_flight.flightLegs_irregularity_delayInfo_delayReasonPublicLong, v_past_flight.flightLegs_irregularity_delayInfo_delayReasonPublicShort, v_geod.flightLegs_depInfo_airport_Continent_Name,  v_geod.flightLegs_depInfo_airport_Subcontinent_Name,  v_geod.flightLegs_depInfo_airport_Country_Code,  v_geod.flightLegs_depInfo_airport_Country_Name,  v_geod.flightLegs_depInfo_airport_Location_name,  v_geod.flightLegs_depInfo_airport_Airport_Name,  v_geod.flightLegs_depInfo_airport_Icao_Code,  v_geod.flightLegs_depInfo_airport_Latitude,  v_geod.flightLegs_depInfo_airport_Longitude, v_past_flight.flightLegs_depInfo_airport_code, v_past_flight.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, v_past_flight.flightLegs_depInfo_airport_places_depPosTerm_gateNumber, v_past_flight.flightLegs_depInfo_times_scheduled_date, v_past_flight.flightLegs_depInfo_times_scheduled_time, v_past_flight.flightLegs_depInfo_times_scheduled_year, v_past_flight.flightLegs_depInfo_times_scheduled_month, v_past_flight.flightLegs_depInfo_times_scheduled_day , v_past_flight.flightLegs_depInfo_times_scheduled_hour, v_past_flight.flightLegs_depInfo_times_scheduled_minute, v_past_flight.flightLegs_depInfo_times_scheduled_timezone, v_past_flight.flightLegs_depInfo_times_number_week, v_geoa.flightLegs_arrInfo_airport_Continent_Name,  v_geoa.flightLegs_arrInfo_airport_Subcontinent_Name,  v_geoa.flightLegs_arrInfo_airport_Country_Code,  v_geoa.flightLegs_arrInfo_airport_Country_Name,  v_geoa.flightLegs_arrInfo_airport_Location_name,  v_geoa.flightLegs_arrInfo_airport_Airport_Name,  v_geoa.flightLegs_arrInfo_airport_Icao_Code,  v_geoa.flightLegs_arrInfo_airport_Latitude,  v_geoa.flightLegs_arrInfo_airport_Longitude, v_past_flight.flightLegs_arrInfo_airport_code, v_past_flight.flightLegs_arrInfo_airport_places_arrivalPositionTerminal, v_past_flight.flightLegs_arrInfo_times_scheduled_date, v_past_flight.flightLegs_arrInfo_times_scheduled_time, v_past_flight.flightLegs_arrInfo_times_scheduled_year, v_past_flight.flightLegs_arrInfo_times_scheduled_month, v_past_flight.flightLegs_arrInfo_times_scheduled_day, v_past_flight.flightLegs_arrInfo_times_scheduled_hour, v_past_flight.flightLegs_arrInfo_times_scheduled_minute, v_past_flight.flightLegs_arrInfo_times_scheduled_timezone, v_past_flight.flightLegs_arrInfo_times_number_week from v_past_flight v_past_flight 	INNER JOIN v_geod v_geod ON v_geod.flightLegs_depInfo_airport_Iata_Code = v_past_flight.flightLegs_depInfo_airport_code 	INNER JOIN v_geoa v_geoa ON v_geoa.flightLegs_arrInfo_airport_Iata_Code = v_past_flight.flightLegs_arrInfo_airport_code ;'

    try:
        engine = create_engine(DATABASE_URL)

        def get_sql_data(query):
            df = pd.read_sql(query, engine)
            return df

        try:
            df = get_sql_data(query)

        except Exception as e:
            full_trace = traceback.format_exc()
            print(full_trace)  # shows full stack trace in Docker logs
            raise HTTPException(
                status_code=500,
                detail=f"Database query failed: {str(e)}"
            )

        df.to_csv('./data/afklm_past_flight.csv.zip', compression='zip', index=False)
        print("PostgreSQL data retrieved")

    except Exception as e:
        full_trace = traceback.format_exc()
        print(full_trace)

        raise HTTPException(
            status_code=500,
            detail=f"Database connection error: {str(e)}"
        )


retrieve_latest_training_dataset()