
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
INNER JOIN Subcontinent Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID
INNER JOIN Country Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID
INNER JOIN Location Location ON Location.Country_ID = Country.Country_ID
INNER JOIN Airport Airport ON Airport.Location_ID = Location.Location_ID;

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
INNER JOIN Subcontinent Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID
INNER JOIN Country Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID
INNER JOIN Location Location ON Location.Country_ID = Country.Country_ID
INNER JOIN Airport Airport ON Airport.Location_ID = Location.Location_ID;

DROP VIEW IF EXISTS v_past_flight;
CREATE VIEW v_past_flight AS 
select flight.flight_id, 
flight.flightNumber, 
flight.airline_code, 
flight.airline_name, 
flight.flightStatusPublic, 
flight.flightLegs_aircraft_typeCode, 
flight.flightLegs_scheduledFlightDuration, 
flight.flightLegs_serviceType, 
flight.flightLegs_aircraft_ownerAirlineCode, 
flight.flightLegs_status, 
flight.delay_status, 
delay.flightLegs_irregularity_delayDuration,
delay.flightLegs_irregularity_delayInfo_delayReasonPublicLong,
delay.flightLegs_irregularity_delayInfo_delayReasonPublicShort,
departure_airport.flightLegs_depInfo_airport_code, 
departure_airport.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal,
departure_airport.flightLegs_depInfo_airport_places_depPosTerm_gateNumber,
departure_airport.flightLegs_depInfo_times_scheduled_date,
departure_airport.flightLegs_depInfo_times_scheduled_time,
departure_airport.flightLegs_depInfo_times_scheduled_year,
departure_airport.flightLegs_depInfo_times_scheduled_month,
departure_airport.flightLegs_depInfo_times_scheduled_day ,
departure_airport.flightLegs_depInfo_times_scheduled_hour,
departure_airport.flightLegs_depInfo_times_scheduled_minute,
departure_airport.flightLegs_depInfo_times_scheduled_timezone,
departure_airport.flightLegs_depInfo_times_number_week,
arrival_airport.flightLegs_arrInfo_airport_code,
arrival_airport.flightLegs_arrInfo_airport_places_arrivalPositionTerminal,
arrival_airport.flightLegs_arrInfo_times_scheduled_date,
arrival_airport.flightLegs_arrInfo_times_scheduled_time,
arrival_airport.flightLegs_arrInfo_times_scheduled_year,
arrival_airport.flightLegs_arrInfo_times_scheduled_month,
arrival_airport.flightLegs_arrInfo_times_scheduled_day,
arrival_airport.flightLegs_arrInfo_times_scheduled_hour,
arrival_airport.flightLegs_arrInfo_times_scheduled_minute,
arrival_airport.flightLegs_arrInfo_times_scheduled_timezone,
arrival_airport.flightLegs_arrInfo_times_number_week
 from flight flight
	INNER JOIN delay delay ON delay.flight_id = flight.flight_id
	INNER JOIN departure_airport departure_airport ON departure_airport.flight_id = flight.flight_id
	INNER JOIN arrival_airport arrival_airport ON arrival_airport.flight_id = flight.flight_id
where departure_airport.flightLegs_depInfo_times_scheduled_date < current_date;

DROP VIEW IF EXISTS v_future_flight;
CREATE VIEW v_future_flight AS 
select flight.flight_id, 
flight.flightNumber, 
flight.airline_code, 
flight.airline_name, 
flight.flightStatusPublic, 
flight.flightLegs_aircraft_typeCode, 
flight.flightLegs_scheduledFlightDuration, 
flight.flightLegs_serviceType, 
flight.flightLegs_aircraft_ownerAirlineCode, 
flight.flightLegs_status, 
flight.delay_status, 
delay.flightLegs_irregularity_delayDuration,
delay.flightLegs_irregularity_delayInfo_delayReasonPublicLong,
delay.flightLegs_irregularity_delayInfo_delayReasonPublicShort,
departure_airport.flightLegs_depInfo_airport_code, 
departure_airport.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal,
departure_airport.flightLegs_depInfo_airport_places_depPosTerm_gateNumber,
departure_airport.flightLegs_depInfo_times_scheduled_date,
departure_airport.flightLegs_depInfo_times_scheduled_time,
departure_airport.flightLegs_depInfo_times_scheduled_year,
departure_airport.flightLegs_depInfo_times_scheduled_month,
departure_airport.flightLegs_depInfo_times_scheduled_day ,
departure_airport.flightLegs_depInfo_times_scheduled_hour,
departure_airport.flightLegs_depInfo_times_scheduled_minute,
departure_airport.flightLegs_depInfo_times_scheduled_timezone,
departure_airport.flightLegs_depInfo_times_number_week,
arrival_airport.flightLegs_arrInfo_airport_code,
arrival_airport.flightLegs_arrInfo_airport_places_arrivalPositionTerminal,
arrival_airport.flightLegs_arrInfo_times_scheduled_date,
arrival_airport.flightLegs_arrInfo_times_scheduled_time,
arrival_airport.flightLegs_arrInfo_times_scheduled_year,
arrival_airport.flightLegs_arrInfo_times_scheduled_month,
arrival_airport.flightLegs_arrInfo_times_scheduled_day,
arrival_airport.flightLegs_arrInfo_times_scheduled_hour,
arrival_airport.flightLegs_arrInfo_times_scheduled_minute,
arrival_airport.flightLegs_arrInfo_times_scheduled_timezone,
arrival_airport.flightLegs_arrInfo_times_number_week
 from flight flight
	INNER JOIN delay delay ON delay.flight_id = flight.flight_id
	INNER JOIN departure_airport departure_airport ON departure_airport.flight_id = flight.flight_id
	INNER JOIN arrival_airport arrival_airport ON arrival_airport.flight_id = flight.flight_id
where departure_airport.flightLegs_depInfo_times_scheduled_date > current_date;


