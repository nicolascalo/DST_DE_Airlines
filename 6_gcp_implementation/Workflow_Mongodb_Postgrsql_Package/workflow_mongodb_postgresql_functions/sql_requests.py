CREATING_MONGODB_FUTURE = """
DROP TABLE IF EXISTS mongodb_future;
CREATE TABLE mongodb_future (
_id varchar(100) NULL,
id varchar(100) NULL,
airline_code varchar(100) NULL,
airline_name varchar(100) NULL,
flightLegs_aircraft_ownerAirlineCode varchar(100) NULL,
flightLegs_aircraft_typeCode varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_areaCode varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_code varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_name varchar(100) NULL,
flightLegs_arrInfo_airport_code varchar(100) NULL,
flightLegs_arrInfo_airport_location_latitude varchar(100) NULL,
flightLegs_arrInfo_airport_location_longitude varchar(100) NULL,
flightLegs_arrInfo_times_scheduled varchar(100) NULL,
flightLegs_depInfo_airport_city_country_areaCode varchar(100) NULL,
flightLegs_depInfo_airport_city_country_code varchar(100) NULL,
flightLegs_depInfo_airport_city_country_name varchar(100) NULL,
flightLegs_depInfo_airport_code varchar(100) NULL,
flightLegs_depInfo_airport_location_latitude varchar(100) NULL,
flightLegs_depInfo_airport_location_longitude varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_gateNumber varchar(100) NULL,
flightLegs_depInfo_times_scheduled varchar(100) NULL,
flightLegs_irregularity_delayDuration varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicLong varchar(250) NULL,
flightLegs_irregularity_delayInformation_delayCode varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicShort varchar(250) NULL,
flightLegs_irregularity_delayReason varchar(250) NULL,
flightLegs_scheduledFlightDuration varchar(100) NULL,
flightLegs_serviceType varchar(100) NULL,
flightLegs_serviceTypeName varchar(100) NULL,
flightLegs_status varchar(100) NULL,
flightLegs_publishedStatus varchar(100) NULL,
flightLegs_legStatusPublic varchar(100) NULL,
flightLegs_statusName varchar(100) NULL,
flightNumber varchar(100) NULL,
flightStatusPublic varchar(100) NULL,
flightLegs_arrInfo_times_estimated_value varchar(100) NULL,
flightLegs_arrInfo_times_latestPublished varchar(100) NULL,
flightLegs_depInfo_times_actual varchar(100) NULL,
flightLegs_depInfo_times_actualTakeOffTime varchar(100) NULL,
flightLegs_depInfo_times_latestPublished varchar(100) NULL,
flightLegs_arrInfo_airport_places_arrivalPositionTerminal varchar(100) NULL,
flightLegs_arrInfo_times_actual varchar(100) NULL,
flightLegs_arrInfo_times_actualTouchDownTime varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal varchar(100) NULL,
flightLegs_irregularity_delayDuration_total varchar(250) NULL);
"""
CREATING_MONGODB_FUTURE_D1 = """
DROP TABLE IF EXISTS mongodb_future_d1;
CREATE TABLE mongodb_future_d1 (
_id varchar(100) NULL,
id varchar(100) NULL,
airline_code varchar(100) NULL,
airline_name varchar(100) NULL,
flightLegs_aircraft_ownerAirlineCode varchar(100) NULL,
flightLegs_aircraft_typeCode varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_areaCode varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_code varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_name varchar(100) NULL,
flightLegs_arrInfo_airport_code varchar(100) NULL,
flightLegs_arrInfo_airport_location_latitude varchar(100) NULL,
flightLegs_arrInfo_airport_location_longitude varchar(100) NULL,
flightLegs_arrInfo_times_scheduled varchar(100) NULL,
flightLegs_depInfo_airport_city_country_areaCode varchar(100) NULL,
flightLegs_depInfo_airport_city_country_code varchar(100) NULL,
flightLegs_depInfo_airport_city_country_name varchar(100) NULL,
flightLegs_depInfo_airport_code varchar(100) NULL,
flightLegs_depInfo_airport_location_latitude varchar(100) NULL,
flightLegs_depInfo_airport_location_longitude varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_gateNumber varchar(100) NULL,
flightLegs_depInfo_times_scheduled varchar(100) NULL,
flightLegs_irregularity_delayDuration varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicLong varchar(250) NULL,
flightLegs_irregularity_delayInformation_delayCode varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicShort varchar(250) NULL,
flightLegs_irregularity_delayReason varchar(250) NULL,
flightLegs_scheduledFlightDuration varchar(100) NULL,
flightLegs_serviceType varchar(100) NULL,
flightLegs_serviceTypeName varchar(100) NULL,
flightLegs_status varchar(100) NULL,
flightLegs_publishedStatus varchar(100) NULL,
flightLegs_legStatusPublic varchar(100) NULL,
flightLegs_statusName varchar(100) NULL,
flightNumber varchar(100) NULL,
flightStatusPublic varchar(100) NULL,
flightLegs_arrInfo_times_estimated_value varchar(100) NULL,
flightLegs_arrInfo_times_latestPublished varchar(100) NULL,
flightLegs_depInfo_times_actual varchar(100) NULL,
flightLegs_depInfo_times_actualTakeOffTime varchar(100) NULL,
flightLegs_depInfo_times_latestPublished varchar(100) NULL,
flightLegs_arrInfo_airport_places_arrivalPositionTerminal varchar(100) NULL,
flightLegs_arrInfo_times_actual varchar(100) NULL,
flightLegs_arrInfo_times_actualTouchDownTime varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal varchar(100) NULL,
flightLegs_irregularity_delayDuration_total varchar(250) NULL);
"""
CREATING_MONGODB_PAST = """
DROP TABLE IF EXISTS mongodb_past;
CREATE TABLE mongodb_past (
_id varchar(100) NULL,
id varchar(100) NULL,
airline_code varchar(100) NULL,
airline_name varchar(100) NULL,
flightLegs_aircraft_ownerAirlineCode varchar(100) NULL,
flightLegs_aircraft_typeCode varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_areaCode varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_code varchar(100) NULL,
flightLegs_arrInfo_airport_city_country_name varchar(100) NULL,
flightLegs_arrInfo_airport_code varchar(100) NULL,
flightLegs_arrInfo_airport_location_latitude varchar(100) NULL,
flightLegs_arrInfo_airport_location_longitude varchar(100) NULL,
flightLegs_arrInfo_times_scheduled varchar(100) NULL,
flightLegs_depInfo_airport_city_country_areaCode varchar(100) NULL,
flightLegs_depInfo_airport_city_country_code varchar(100) NULL,
flightLegs_depInfo_airport_city_country_name varchar(100) NULL,
flightLegs_depInfo_airport_code varchar(100) NULL,
flightLegs_depInfo_airport_location_latitude varchar(100) NULL,
flightLegs_depInfo_airport_location_longitude varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_gateNumber varchar(100) NULL,
flightLegs_depInfo_times_scheduled varchar(100) NULL,
flightLegs_irregularity_delayDuration varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicLong varchar(250) NULL,
flightLegs_irregularity_delayInformation_delayCode varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicShort varchar(250) NULL,
flightLegs_irregularity_delayReason varchar(250) NULL,
flightLegs_scheduledFlightDuration varchar(100) NULL,
flightLegs_serviceType varchar(100) NULL,
flightLegs_serviceTypeName varchar(100) NULL,
flightLegs_status varchar(100) NULL,
flightLegs_publishedStatus varchar(100) NULL,
flightLegs_legStatusPublic varchar(100) NULL,
flightLegs_statusName varchar(100) NULL,
flightNumber varchar(100) NULL,
flightStatusPublic varchar(100) NULL,
flightLegs_arrInfo_times_estimated_value varchar(100) NULL,
flightLegs_arrInfo_times_latestPublished varchar(100) NULL,
flightLegs_depInfo_times_actual varchar(100) NULL,
flightLegs_depInfo_times_actualTakeOffTime varchar(100) NULL,
flightLegs_depInfo_times_latestPublished varchar(100) NULL,
flightLegs_arrInfo_airport_places_arrivalPositionTerminal varchar(100) NULL,
flightLegs_arrInfo_times_actual varchar(100) NULL,
flightLegs_arrInfo_times_actualTouchDownTime varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal varchar(100) NULL,
flightLegs_irregularity_delayDuration_total varchar(250) NULL);
"""
CLEANING_TEMPORARY_TABLES="""
DELETE FROM mongodb_future USING mongodb_future_d1
  WHERE mongodb_future.flight_id = mongodb_future_d1.flight_id;
  
DELETE FROM flight_future USING mongodb_future_d1
  WHERE flight_future.flight_id = mongodb_future_d1.flight_id;
  
DELETE FROM departure_future USING mongodb_future_d1
  WHERE departure_future.flight_id = mongodb_future_d1.flight_id;
  
DELETE FROM arrival_future USING mongodb_future_d1
  WHERE arrival_future.flight_id = mongodb_future_d1.flight_id;


INSERT INTO mongodb_future SELECT * FROM mongodb_future_d1;

DROP TABLE mongodb_future_d1;
DELETE FROM mongodb_future USING mongodb_past
  WHERE mongodb_future.flight_id = mongodb_past.flight_id;
"""
CLEANING_AIROPORT = """
DELETE FROM mongodb_past 
where mongodb_past.flightlegs_arrinfo_airport_code not in (
select distinct airport.Iata_Code
from airport 
);

DELETE FROM mongodb_future
where mongodb_future.flightlegs_arrinfo_airport_code not in (
select distinct airport.Iata_Code
from airport 
);

DELETE FROM mongodb_past 
where mongodb_past.flightlegs_depinfo_airport_code not in (
select distinct airport.Iata_Code
from airport 
);

DELETE FROM mongodb_future
where mongodb_future.flightlegs_depinfo_airport_code not in (
select distinct airport.Iata_Code
from airport 
);
"""
INSERT_SELECT_FLIGHT_PAST="""
INSERT INTO flight_past (flight_id, flightNumber, airline_code, airline_name, flightStatusPublic, flightLegs_aircraft_typeCode, flightLegs_scheduledFlightDuration, flightLegs_serviceType, flightLegs_aircraft_ownerAirlineCode, flightLegs_status, flightLegs_serviceTypeName, flightLegs_publishedStatus, flightLegs_legStatusPublic, flightLegs_statusName, delay_status)
SELECT distinct flight_id, CAST (flightNumber AS INTEGER)flightNumber, airline_code, airline_name, flightStatusPublic, flightLegs_aircraft_typeCode, flightLegs_scheduledFlightDuration, flightLegs_serviceType, flightLegs_aircraft_ownerAirlineCode, flightLegs_status, flightLegs_serviceTypeName, flightLegs_publishedStatus, flightLegs_legStatusPublic, flightLegs_statusName,
CASE
	WHEN flightlegs_irregularity_delayduration_total isnull THEN false
	ELSE true END 
FROM mongodb_past
ON CONFLICT DO NOTHING;

INSERT INTO departure_past (flight_id, flightLegs_depInfo_airport_code, flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, flightLegs_depInfo_airport_places_depPosTerm_gateNumber, flightLegs_depInfo_times_scheduled_date, flightLegs_depInfo_times_scheduled_time, flightLegs_depInfo_times_scheduled_year, flightLegs_depInfo_times_scheduled_month, flightLegs_depInfo_times_scheduled_day, flightLegs_depInfo_times_scheduled_hour, flightLegs_depInfo_times_scheduled_minute, flightLegs_depInfo_times_scheduled_timezone, flightLegs_depInfo_times_number_week)
SELECT distinct flight_id, flightLegs_depInfo_airport_code, flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, flightLegs_depInfo_airport_places_depPosTerm_gateNumber, CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 0, 11) AS DATE) date, CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 12, 5) AS time) time, CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 1, 4) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 6, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 9, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 12, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 15, 2) AS INTEGER), SUBSTRING (flightLegs_depInfo_times_scheduled, 24, 6), EXTRACT(WEEK FROM TO_TIMESTAMP(SUBSTRING (flightLegs_depInfo_times_scheduled, 1, 10), 'YYYY/MM/DD/HH24:MI:ss')) 
FROM mongodb_past
ON CONFLICT DO NOTHING;

INSERT INTO arrival_past (flight_id, flightLegs_arrInfo_airport_code, flightLegs_arrInfo_airport_places_arrivalPositionTerminal, flightLegs_arrInfo_times_scheduled_date, flightLegs_arrInfo_times_scheduled_time, flightLegs_arrInfo_times_scheduled_year, flightLegs_arrInfo_times_scheduled_month, flightLegs_arrInfo_times_scheduled_day, flightLegs_arrInfo_times_scheduled_hour, flightLegs_arrInfo_times_scheduled_minute, flightLegs_arrInfo_times_scheduled_timezone, flightLegs_arrInfo_times_number_week)
SELECT distinct flight_id, flightLegs_arrInfo_airport_code, flightLegs_arrInfo_airport_places_arrivalPositionTerminal, CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 0, 11) AS DATE) date, CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 12, 5) AS time) time, CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 1, 4) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 6, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 9, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 12, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 15, 2) AS INTEGER), SUBSTRING (flightLegs_arrInfo_times_scheduled, 24, 6), EXTRACT(WEEK FROM TO_TIMESTAMP(SUBSTRING (flightLegs_arrInfo_times_scheduled, 1, 10), 'YYYY/MM/DD/HH24:MI:ss'))
FROM mongodb_past ON CONFLICT DO NOTHING;

INSERT INTO delay (flight_id, flightLegs_irregularity_delayDuration, flightlegs_irregularity_delayduration_total, flightLegs_irregularity_delayInfo_delayReasonPublicLong, flightLegs_irregularity_delayInfo_delayReasonPublicShort)
SELECT distinct flight_id, flightLegs_irregularity_delayDuration, flightlegs_irregularity_delayduration_total, flightLegs_irregularity_delayInfo_delayReasonPublicLong, flightLegs_irregularity_delayInfo_delayReasonPublicShort
FROM mongodb_past 
ON CONFLICT DO NOTHING;
"""
INSERT_SELECT_FLIGHT_FUTURE="""
INSERT INTO flight_future (flight_id, flightNumber, airline_code, airline_name, flightStatusPublic, flightLegs_aircraft_typeCode, flightLegs_scheduledFlightDuration, flightLegs_serviceType, flightLegs_aircraft_ownerAirlineCode, flightLegs_status, flightLegs_serviceTypeName, flightLegs_publishedStatus, flightLegs_legStatusPublic, flightLegs_statusName)
SELECT distinct flight_id, CAST (flightNumber AS INTEGER)flightNumber, airline_code, airline_name, flightStatusPublic, flightLegs_aircraft_typeCode, flightLegs_scheduledFlightDuration, flightLegs_serviceType, flightLegs_aircraft_ownerAirlineCode, flightLegs_status, flightLegs_serviceTypeName, flightLegs_publishedStatus, flightLegs_legStatusPublic, flightLegs_statusName
FROM mongodb_future
ON CONFLICT DO NOTHING;

INSERT INTO departure_future (flight_id, flightLegs_depInfo_airport_code, flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, flightLegs_depInfo_airport_places_depPosTerm_gateNumber, flightLegs_depInfo_times_scheduled_date, flightLegs_depInfo_times_scheduled_time, flightLegs_depInfo_times_scheduled_year, flightLegs_depInfo_times_scheduled_month, flightLegs_depInfo_times_scheduled_day, flightLegs_depInfo_times_scheduled_hour, flightLegs_depInfo_times_scheduled_minute, flightLegs_depInfo_times_scheduled_timezone, flightLegs_depInfo_times_number_week)
SELECT distinct flight_id, flightLegs_depInfo_airport_code, flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, flightLegs_depInfo_airport_places_depPosTerm_gateNumber, CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 0, 11) AS DATE) date, CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 12, 5) AS time) time, CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 1, 4) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 6, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 9, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 12, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_depInfo_times_scheduled, 15, 2) AS INTEGER), SUBSTRING (flightLegs_depInfo_times_scheduled, 24, 6), EXTRACT(WEEK FROM TO_TIMESTAMP(SUBSTRING (flightLegs_depInfo_times_scheduled, 1, 10), 'YYYY/MM/DD/HH24:MI:ss')) 
FROM mongodb_future
ON CONFLICT DO NOTHING;

INSERT INTO arrival_future (flight_id, flightLegs_arrInfo_airport_code, flightLegs_arrInfo_airport_places_arrivalPositionTerminal, flightLegs_arrInfo_times_scheduled_date, flightLegs_arrInfo_times_scheduled_time, flightLegs_arrInfo_times_scheduled_year, flightLegs_arrInfo_times_scheduled_month, flightLegs_arrInfo_times_scheduled_day, flightLegs_arrInfo_times_scheduled_hour, flightLegs_arrInfo_times_scheduled_minute, flightLegs_arrInfo_times_scheduled_timezone, flightLegs_arrInfo_times_number_week)
SELECT distinct flight_id, flightLegs_arrInfo_airport_code, flightLegs_arrInfo_airport_places_arrivalPositionTerminal, CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 0, 11) AS DATE) date, CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 12, 5) AS time) time, CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 1, 4) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 6, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 9, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 12, 2) AS INTEGER), CAST (SUBSTRING (flightLegs_arrInfo_times_scheduled, 15, 2) AS INTEGER), SUBSTRING (flightLegs_arrInfo_times_scheduled, 24, 6), EXTRACT(WEEK FROM TO_TIMESTAMP(SUBSTRING (flightLegs_arrInfo_times_scheduled, 1, 10), 'YYYY/MM/DD/HH24:MI:ss'))
FROM mongodb_future ON CONFLICT DO NOTHING;
"""
REMOVING_FROM_FUTURE_BASED_ON_PAST="""
DELETE FROM flight_future USING flight_past
  WHERE flight_future.flight_id = flight_past.flight_id ;


DELETE FROM arrival_future USING arrival_past
  WHERE arrival_future.flight_id = arrival_past.flight_id ;

DELETE FROM departure_future USING departure_past
  WHERE departure_future.flight_id = departure_past.flight_id ;
"""
FINAL_CLEANING ="""
DROP TABLE mongodb_past;
DROP TABLE mongodb_future;
"""
