
DROP TABLE IF EXISTS mongodb;
CREATE TABLE mongodb (
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
flightLegs_statusName varchar(100) NULL,
flightNumber varchar(100) NULL,
flightStatusPublic varchar(100) NULL,
flightLegs_arrInfo_times_estimated_value varchar(100) NULL,
flightLegs_arrInfo_times_latestPublished varchar(100) NULL,
flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal varchar(100) NULL,
flightLegs_depInfo_times_latestPublished varchar(100) NULL,
flightLegs_arrInfo_airport_places_arrivalPositionTerminal varchar(100) NULL,
flightLegs_arrInfo_times_actual varchar(100) NULL,
flightLegs_arrInfo_times_actualTouchDownTime varchar(100) NULL,
flightLegs_depInfo_times_actual varchar(100) NULL,
flightLegs_depInfo_times_actualTakeOffTime varchar(100) NULL,
flightLegs_irregularity_delayDuration_total varchar(250) NULL);

DROP TABLE IF EXISTS delay;
CREATE TABLE delay (
flight_id varchar(50) PRIMARY KEY,
flightLegs_irregularity_delayDuration varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicLong varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicShort varchar(250) NULL
);

DROP TABLE IF EXISTS arrival_airport;
CREATE TABLE arrival_airport (
flight_id varchar(50) PRIMARY KEY,
flightLegs_arrInfo_airport_code varchar(3) NOT NULL,
flightLegs_arrInfo_airport_places_arrivalPositionTerminal varchar(50) NULL,
flightLegs_arrInfo_times_scheduled_date date NULL,
flightLegs_arrInfo_times_scheduled_time time NULL,
flightLegs_arrInfo_times_scheduled_year int NULL,
flightLegs_arrInfo_times_scheduled_month int NULL,
flightLegs_arrInfo_times_scheduled_day int NULL,
flightLegs_arrInfo_times_scheduled_hour int NULL,
flightLegs_arrInfo_times_scheduled_minute int NULL,
flightLegs_arrInfo_times_scheduled_timezone varchar(6) NULL,
flightLegs_arrInfo_times_number_week int NULL
);

DROP TABLE IF EXISTS departure_airport;
CREATE TABLE departure_airport (
flight_id varchar(50) PRIMARY KEY,
flightLegs_depInfo_airport_code varchar(3) NOT NULL,
flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal varchar(50) NULL,
flightLegs_depInfo_airport_places_depPosTerm_gateNumber varchar(50) NULL,
flightLegs_depInfo_times_scheduled_date date NULL,
flightLegs_depInfo_times_scheduled_time time NULL,
flightLegs_depInfo_times_scheduled_year int NULL,
flightLegs_depInfo_times_scheduled_month int NULL,
flightLegs_depInfo_times_scheduled_day int NULL,
flightLegs_depInfo_times_scheduled_hour int NULL,
flightLegs_depInfo_times_scheduled_minute int NULL,
flightLegs_depInfo_times_scheduled_timezone varchar(6) NULL,
flightLegs_depInfo_times_number_week int NULL
);

DROP TABLE IF EXISTS flight;
CREATE TABLE flight (
flight_id varchar(50) PRIMARY KEY,
flightNumber int DEFAULT NULL,
airline_code varchar(50) NULL,
airline_name varchar(100) NULL,
flightStatusPublic varchar(50) NULL,
flightLegs_aircraft_typeCode varchar(50) NULL,
flightLegs_scheduledFlightDuration varchar(50) NULL,
flightLegs_serviceType varchar(50) NULL,
flightLegs_aircraft_ownerAirlineCode varchar(50) NULL,
flightLegs_status varchar(50) NULL,
delay_status boolean NULL
);
