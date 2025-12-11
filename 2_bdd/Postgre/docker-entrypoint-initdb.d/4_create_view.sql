
DROP VIEW IF EXISTS v_geod;
CREATE VIEW v_geod AS 
select Continent.Continent_ID as flightLegs_depInfo_airport_Continent_ID, 
Continent.Continent_Name as flightLegs_depInfo_airport_Continent_Name, 
Subcontinent.Subcontinent_ID as flightLegs_depInfo_airport_Subcontinent_ID,
Subcontinent.Subcontinent_Name as flightLegs_depInfo_airport_Subcontinent_Name, 
Country.Country_ID as flightLegs_depInfo_airport_Country_ID,
Country.Country_Code as flightLegs_depInfo_airport_Country_Code, 
Country.Country_Name as flightLegs_depInfo_airport_Country_Name, 
Location.Location_ID as flightLegs_depInfo_airport_Location_ID, 
Location.Location_name as flightLegs_depInfo_airport_Location_name, 
Airport.Airport_ID as flightLegs_depInfo_airport_Airport_ID,
Airport.Airport_Name as flightLegs_depInfo_airport_Airport_Name, 
Airport.Iata_Code as flightLegs_depInfo_airport_Iata_Code, 
Airport.Icao_Code as flightLegs_depInfo_airport_Icao_Code, 
Airport.Latitude as flightLegs_depInfo_airport_Latitude, 
Airport.Longitude as flightLegs_depInfo_airport_Longitude
from Continent Continent
INNER JOIN  Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID
INNER JOIN  Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID
INNER JOIN Location  ON Location.Country_ID = Country.Country_ID
INNER JOIN Airport  ON Airport.Location_ID = Location.Location_ID;

DROP VIEW IF EXISTS v_geoa;
CREATE VIEW v_geoa AS 
select Continent.Continent_ID as flightLegs_arrInfo_airport_Continent_ID, 
Continent.Continent_Name as flightLegs_arrInfo_airport_Continent_Name, 
Subcontinent.Subcontinent_ID as flightLegs_arrInfo_airport_Subcontinent_ID,
Subcontinent.Subcontinent_Name as flightLegs_arrInfo_airport_Subcontinent_Name, 
Country.Country_ID as flightLegs_arrInfo_airport_Country_ID,
Country.Country_Code as flightLegs_arrInfo_airport_Country_Code, 
Country.Country_Name as flightLegs_arrInfo_airport_Country_Name, 
Location.Location_ID as flightLegs_arrInfo_airport_Location_ID, 
Location.Location_name as flightLegs_arrInfo_airport_Location_name, 
Airport.Airport_ID as flightLegs_arrInfo_airport_Airport_ID,
Airport.Airport_Name as flightLegs_arrInfo_airport_Airport_Name, 
Airport.Iata_Code as flightLegs_arrInfo_airport_Iata_Code, 
Airport.Icao_Code as flightLegs_arrInfo_airport_Icao_Code, 
Airport.Latitude as flightLegs_arrInfo_airport_Latitude, 
Airport.Longitude as flightLegs_arrInfo_airport_Longitude
from Continent Continent
INNER JOIN  Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID
INNER JOIN  Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID
INNER JOIN Location  ON Location.Country_ID = Country.Country_ID
INNER JOIN Airport  ON Airport.Location_ID = Location.Location_ID;

DROP VIEW IF EXISTS v_past_flight;
CREATE VIEW v_past_flight AS 
select
flight_past.flight_id, 
flight_past.flightNumber, 
flight_past.airline_code, 
flight_past.airline_name, 
flight_past.flightStatusPublic, 
flight_past.flightLegs_aircraft_typeCode, 
flight_past.flightLegs_scheduledFlightDuration, 
flight_past.flightLegs_serviceType, 
flight_past.flightLegs_aircraft_ownerAirlineCode, 
flight_past.flightLegs_status, 
flight_past.delay_status, 
flight_past.flightLegs_serviceTypeName,
flight_past.flightLegs_publishedStatus,
flight_past.flightLegs_legStatusPublic,
flight_past.flightLegs_statusName,
delay.flightLegs_irregularity_delayDuration,
delay.flightlegs_irregularity_delayduration_total,
delay.flightLegs_irregularity_delayInfo_delayReasonPublicLong,
delay.flightLegs_irregularity_delayInfo_delayReasonPublicShort,
departure_past.flightLegs_depInfo_airport_code, 
departure_past.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal,
departure_past.flightLegs_depInfo_airport_places_depPosTerm_gateNumber,
departure_past.flightLegs_depInfo_times_scheduled_date,
departure_past.flightLegs_depInfo_times_scheduled_time,
departure_past.flightLegs_depInfo_times_scheduled_year,
departure_past.flightLegs_depInfo_times_scheduled_month,
departure_past.flightLegs_depInfo_times_scheduled_day ,
departure_past.flightLegs_depInfo_times_scheduled_hour,
departure_past.flightLegs_depInfo_times_scheduled_minute,
departure_past.flightLegs_depInfo_times_scheduled_timezone,
departure_past.flightLegs_depInfo_times_number_week,
arrival_past.flightLegs_arrInfo_airport_code,
arrival_past.flightLegs_arrInfo_airport_places_arrivalPositionTerminal,
arrival_past.flightLegs_arrInfo_times_scheduled_date,
arrival_past.flightLegs_arrInfo_times_scheduled_time,
arrival_past.flightLegs_arrInfo_times_scheduled_year,
arrival_past.flightLegs_arrInfo_times_scheduled_month,
arrival_past.flightLegs_arrInfo_times_scheduled_day,
arrival_past.flightLegs_arrInfo_times_scheduled_hour,
arrival_past.flightLegs_arrInfo_times_scheduled_minute,
arrival_past.flightLegs_arrInfo_times_scheduled_timezone,
arrival_past.flightLegs_arrInfo_times_number_week
 from flight_past flight_past
	INNER JOIN delay ON delay.flight_id = flight_past.flight_id
	INNER JOIN departure_past  ON departure_past.flight_id = flight_past.flight_id
	INNER JOIN arrival_past  ON arrival_past.flight_id = flight_past.flight_id
where departure_past.flightLegs_depInfo_times_scheduled_date < current_date;

DROP VIEW IF EXISTS v_future_flight;
CREATE VIEW v_future_flight AS 
select 
flight_future.flight_id, 
flight_future.flightNumber, 
flight_future.airline_code, 
flight_future.airline_name, 
flight_future.flightStatusPublic, 
flight_future.flightLegs_aircraft_typeCode, 
flight_future.flightLegs_scheduledFlightDuration, 
flight_future.flightLegs_serviceType, 
flight_future.flightLegs_aircraft_ownerAirlineCode, 
flight_future.flightLegs_status, 
flight_future.flightLegs_serviceTypeName,
flight_future.flightLegs_publishedStatus,
flight_future.flightLegs_legStatusPublic,
flight_future.flightLegs_statusName,
departure_future.flightLegs_depInfo_airport_code, 
departure_future.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal,
departure_future.flightLegs_depInfo_airport_places_depPosTerm_gateNumber,
departure_future.flightLegs_depInfo_times_scheduled_date,
departure_future.flightLegs_depInfo_times_scheduled_time,
departure_future.flightLegs_depInfo_times_scheduled_year,
departure_future.flightLegs_depInfo_times_scheduled_month,
departure_future.flightLegs_depInfo_times_scheduled_day ,
departure_future.flightLegs_depInfo_times_scheduled_hour,
departure_future.flightLegs_depInfo_times_scheduled_minute,
departure_future.flightLegs_depInfo_times_scheduled_timezone,
departure_future.flightLegs_depInfo_times_number_week,
arrival_future.flightLegs_arrInfo_airport_code,
arrival_future.flightLegs_arrInfo_airport_places_arrivalPositionTerminal,
arrival_future.flightLegs_arrInfo_times_scheduled_date,
arrival_future.flightLegs_arrInfo_times_scheduled_time,
arrival_future.flightLegs_arrInfo_times_scheduled_year,
arrival_future.flightLegs_arrInfo_times_scheduled_month,
arrival_future.flightLegs_arrInfo_times_scheduled_day,
arrival_future.flightLegs_arrInfo_times_scheduled_hour,
arrival_future.flightLegs_arrInfo_times_scheduled_minute,
arrival_future.flightLegs_arrInfo_times_scheduled_timezone,
arrival_future.flightLegs_arrInfo_times_number_week
 from flight_future 
	INNER JOIN departure_future ON departure_future.flight_id = flight_future.flight_id
	INNER JOIN arrival_future ON arrival_future.flight_id = flight_future.flight_id
where departure_future.flightLegs_depInfo_times_scheduled_date > current_date;

