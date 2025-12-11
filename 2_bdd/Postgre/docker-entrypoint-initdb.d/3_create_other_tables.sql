
DROP TABLE IF EXISTS flight_past CASCADE;
CREATE TABLE flight_past (
flight_id varchar(50) PRIMARY KEY,
flightNumber int DEFAULT NULL,
airline_code varchar(50) NULL,
airline_name varchar(100) NULL,
flightStatusPublic varchar(50) NULL,
flightLegs_aircraft_typeCode varchar(50) NULL,
flightLegs_scheduledFlightDuration varchar(50) NULL,
flightLegs_serviceType varchar(50) NULL,
flightLegs_aircraft_ownerAirlineCode varchar(50) NULL,
flightLegs_status varchar(100) NULL,
flightLegs_serviceTypeName varchar(100) NULL,
flightLegs_publishedStatus varchar(100) NULL,
flightLegs_legStatusPublic varchar(100) NULL,
flightLegs_statusName varchar(100) NULL,
delay_status boolean NULL
);

DROP TABLE IF EXISTS flight_future CASCADE;
CREATE TABLE flight_future (
flight_id varchar(50) PRIMARY KEY,
flightNumber int DEFAULT NULL,
airline_code varchar(50) NULL,
airline_name varchar(100) NULL,
flightStatusPublic varchar(50) NULL,
flightLegs_aircraft_typeCode varchar(50) NULL,
flightLegs_scheduledFlightDuration varchar(50) NULL,
flightLegs_serviceType varchar(50) NULL,
flightLegs_aircraft_ownerAirlineCode varchar(50) NULL,
flightLegs_status varchar(100) NULL,
flightLegs_serviceTypeName varchar(100) NULL,
flightLegs_publishedStatus varchar(100) NULL,
flightLegs_legStatusPublic varchar(100) NULL,
flightLegs_statusName varchar(100) NULL,
delay_status boolean NULL
);


DROP TABLE IF EXISTS delay CASCADE;
CREATE TABLE delay (
delay_id serial PRIMARY KEY,
flight_id varchar(50) UNIQUE NOT NULL,
flightLegs_irregularity_delayDuration varchar(250) NULL,
flightLegs_irregularity_delayDuration_total varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicLong varchar(250) NULL,
flightLegs_irregularity_delayInfo_delayReasonPublicShort varchar(250) NULL,
FOREIGN KEY (flight_id) REFERENCES flight_past(flight_id)
);

DROP TABLE IF EXISTS departure_future CASCADE;
CREATE TABLE departure_future (
departure_id serial PRIMARY KEY,
flight_id varchar(50) UNIQUE NOT NULL,
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
flightLegs_depInfo_times_number_week int NULL,
FOREIGN KEY (flight_id) REFERENCES flight_future(flight_id),
FOREIGN KEY (flightLegs_depInfo_airport_code) REFERENCES Airport(Iata_Code)
);



DROP TABLE IF EXISTS departure_past CASCADE;
CREATE TABLE departure_past (
departure_id serial PRIMARY KEY,
flight_id varchar(50) UNIQUE NOT NULL,
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
flightLegs_depInfo_times_number_week int NULL,
FOREIGN KEY (flight_id) REFERENCES flight_past(flight_id),
FOREIGN KEY (flightLegs_depInfo_airport_code) REFERENCES Airport(Iata_Code)
);

DROP TABLE IF EXISTS arrival_future CASCADE;
CREATE TABLE arrival_future (
arrival_id serial PRIMARY KEY,
flight_id varchar(50) UNIQUE NOT NULL,
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
flightLegs_arrInfo_times_number_week int NULL,
FOREIGN KEY (flight_id) REFERENCES flight_future(flight_id),
FOREIGN KEY (flightLegs_arrInfo_airport_code) REFERENCES Airport(Iata_Code)
);


DROP TABLE IF EXISTS arrival_past CASCADE;
CREATE TABLE arrival_past (
arrival_id serial PRIMARY KEY,
flight_id varchar(50) UNIQUE NOT NULL,
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
flightLegs_arrInfo_times_number_week int NULL,
FOREIGN KEY (flight_id) REFERENCES flight_past(flight_id),
FOREIGN KEY (flightLegs_arrInfo_airport_code) REFERENCES Airport(Iata_Code)
);
