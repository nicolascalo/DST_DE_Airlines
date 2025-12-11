------- CLEANING mongodb temp tables

DELETE FROM mongodb_future USING mongodb_future_d1
  WHERE mongodb_future.flight_id = mongodb_future_d1.flight_id;
  
DELETE FROM departure_future USING mongodb_future_d1
  WHERE departure_future.flight_id = mongodb_future_d1.flight_id;
  
DELETE FROM arrival_future USING mongodb_future_d1
  WHERE arrival_future.flight_id = mongodb_future_d1.flight_id;
DELETE FROM flight_future USING mongodb_future_d1
  WHERE flight_future.flight_id = mongodb_future_d1.flight_id;
  

INSERT INTO mongodb_future SELECT * FROM mongodb_future_d1;

DROP TABLE mongodb_future_d1;

DELETE FROM mongodb_future USING mongodb_past
  WHERE mongodb_future.flight_id = mongodb_past.flight_id;


-- CLEANING airports


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




