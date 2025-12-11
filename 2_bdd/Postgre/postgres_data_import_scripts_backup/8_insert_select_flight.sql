

-- insert data into final tables from mongodb tables

-- PAST

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


-- future




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



-- Removing from future based on past

DELETE FROM flight_future USING flight_past
  WHERE flight_future.flight_id = flight_past.flight_id ;


DELETE FROM arrival_future USING arrival_past
  WHERE arrival_future.flight_id = arrival_past.flight_id ;

DELETE FROM departure_future USING departure_past
  WHERE departure_future.flight_id = departure_past.flight_id ;




-- Final cleaning



DROP mongodb_past;
DROP mongodb_future;

