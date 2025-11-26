
DROP VIEW IF EXISTS v_geod;
CREATE VIEW v_geod AS 
select Continent.Continent_ID as Continent_ID_dep, 
Continent.Continent_Name as Continent_Name_dep, 
Subcontinent.Subcontinent_ID as Subcontinent_ID_dep,
Subcontinent.Subcontinent_Name as Subcontinent_Name_dep, 
Country.Country_ID as Country_ID_dep,
Country.Country_Code as Country_Code_dep, 
Country.Country_Name as Country_Name_dep, 
Location.Location_ID as Location_ID_dep, 
Location.Location_name as Location_name_dep, 
Airport.Airport_ID as Airport_ID_dep,
Airport.Airport_Name as Airport_Name_dep, 
Airport.Iata_Code as Iata_Code_dep, 
Airport.Icao_Code as Icao_Code_dep, 
Airport.Latitude as Latitude_dep, 
Airport.Longitude as Longitude_dep
from Continent Continent
INNER JOIN Subcontinent Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID
INNER JOIN Country Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID
INNER JOIN Location Location ON Location.Country_ID = Country.Country_ID
INNER JOIN Airport Airport ON Airport.Location_ID = Location.Location_ID;

DROP VIEW IF EXISTS v_geoa;
CREATE VIEW v_geoa AS 
select Continent.Continent_ID as Continent_ID_arr, 
Continent.Continent_Name as Continent_Name_arr, 
Subcontinent.Subcontinent_ID as Subcontinent_ID_arr,
Subcontinent.Subcontinent_Name as Subcontinent_Name_arr, 
Country.Country_ID as Country_ID_arr,
Country.Country_Code as Country_Code_arr, 
Country.Country_Name as Country_Name_arr, 
Location.Location_ID as Location_ID_arr, 
Location.Location_name as Location_name_arr, 
Airport.Airport_ID as Airport_ID_arr,
Airport.Airport_Name as Airport_Name_arr, 
Airport.Iata_Code as Iata_Code_arr, 
Airport.Icao_Code as Icao_Code_arr, 
Airport.Latitude as Latitude_arr, 
Airport.Longitude as Longitude_arr
from Continent Continent
INNER JOIN Subcontinent Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID
INNER JOIN Country Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID
INNER JOIN Location Location ON Location.Country_ID = Country.Country_ID
INNER JOIN Airport Airport ON Airport.Location_ID = Location.Location_ID;

DROP VIEW IF EXISTS v_flight;
CREATE VIEW v_flight AS 
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
	INNER JOIN arrival_airport arrival_airport ON arrival_airport.flight_id = flight.flight_id;

select v_flight.flight_id, 
v_flight.flightNumber, 
v_flight.airline_code, 
v_flight.airline_name, 
v_flight.flightStatusPublic, 
v_flight.flightLegs_aircraft_typeCode, 
v_flight.flightLegs_scheduledFlightDuration, 
v_flight.flightLegs_serviceType, 
v_flight.flightLegs_aircraft_ownerAirlineCode, 
v_flight.flightLegs_status, 
v_flight.delay_status, 
v_flight.flightLegs_irregularity_delayDuration,
v_flight.flightLegs_irregularity_delayInfo_delayReasonPublicLong,
v_flight.flightLegs_irregularity_delayInfo_delayReasonPublicShort,
v_geod.Continent_Name_dep, 
v_geod.Subcontinent_Name_dep, 
v_geod.Country_Code_dep, 
v_geod.Country_Name_dep, 
v_geod.Location_name_dep, 
v_geod.Airport_Name_dep,
v_flight.flightLegs_depInfo_airport_code,
v_flight.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal,
v_flight.flightLegs_depInfo_airport_places_depPosTerm_gateNumber,
v_flight.flightLegs_depInfo_times_scheduled_date,
v_flight.flightLegs_depInfo_times_scheduled_time,
v_flight.flightLegs_depInfo_times_scheduled_year,
v_flight.flightLegs_depInfo_times_scheduled_month,
v_flight.flightLegs_depInfo_times_scheduled_day ,
v_flight.flightLegs_depInfo_times_scheduled_hour,
v_flight.flightLegs_depInfo_times_scheduled_minute,
v_flight.flightLegs_depInfo_times_scheduled_timezone,
v_flight.flightLegs_depInfo_times_number_week,
v_geoa.Continent_Name_arr, 
v_geoa.Subcontinent_Name_arr, 
v_geoa.Country_Code_arr, 
v_geoa.Country_Name_arr, 
v_geoa.Location_name_arr, 
v_geoa.Airport_Name_arr,
v_flight.flightLegs_arrInfo_airport_code,
v_flight.flightLegs_arrInfo_airport_places_arrivalPositionTerminal,
v_flight.flightLegs_arrInfo_times_scheduled_date,
v_flight.flightLegs_arrInfo_times_scheduled_time,
v_flight.flightLegs_arrInfo_times_scheduled_year,
v_flight.flightLegs_arrInfo_times_scheduled_month,
v_flight.flightLegs_arrInfo_times_scheduled_day,
v_flight.flightLegs_arrInfo_times_scheduled_hour,
v_flight.flightLegs_arrInfo_times_scheduled_minute,
v_flight.flightLegs_arrInfo_times_scheduled_timezone,
v_flight.flightLegs_arrInfo_times_number_week
from v_flight v_flight
	INNER JOIN v_geod v_geod ON v_geod.Iata_Code_dep = v_flight.flightLegs_depInfo_airport_code
	INNER JOIN v_geoa v_geoa ON v_geoa.Iata_Code_arr = v_flight.flightLegs_arrInfo_airport_code
where v_flight.flightLegs_depInfo_times_scheduled_date > current_date;
