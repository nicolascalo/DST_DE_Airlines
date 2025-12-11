
DROP TABLE IF EXISTS tmp_table;
CREATE TEMP TABLE tmp_table AS SELECT * FROM mongodb_past WITH NO DATA;
COPY tmp_table FROM '/tmp/afklm_historic_flights_from_mongo.csv' CSV HEADER;
INSERT INTO mongodb_past SELECT * FROM tmp_table ON CONFLICT DO NOTHING;
ALTER TABLE mongodb_past ADD COLUMN flight_id VARCHAR(50);
UPDATE mongodb_past SET flight_id = CONCAT (id, flightLegs_depInfo_airport_code, flightLegs_arrInfo_airport_code);

DROP TABLE IF EXISTS tmp_table;



CREATE TEMP TABLE tmp_table AS SELECT * FROM mongodb_future_d1 WITH NO DATA;
COPY tmp_table FROM '/tmp/afklm_update_scheduled_d1_flights_from_mongo.csv' CSV HEADER;

INSERT INTO mongodb_future_d1 SELECT  * FROM tmp_table ON CONFLICT DO NOTHING;
ALTER TABLE mongodb_future_d1 ADD COLUMN flight_id VARCHAR(50);
UPDATE mongodb_future_d1 SET flight_id = CONCAT (id, flightLegs_depInfo_airport_code, flightLegs_arrInfo_airport_code);

DROP TABLE IF EXISTS tmp_table;




drop index if exists mongodb_past_flight_id;
CREATE INDEX mongodb_past_flight_id ON mongodb_past(flight_id);
drop index if exists mongodb_future_flight_id;
CREATE INDEX mongodb_future_flight_id ON mongodb_future(flight_id);
drop index if exists mongodb_future_d1_flight_id;
CREATE INDEX mongodb_future_d1_flight_id ON mongodb_future_d1(flight_id);
drop index if exists flight_past_flight_id;
CREATE INDEX flight_past_flight_id ON flight_past(flight_id);
drop index if exists flight_future_flight_id;
CREATE INDEX flight_future_flight_id ON flight_future(flight_id);
CREATE TEMP TABLE tmp_table AS SELECT * FROM mongodb_future WITH NO DATA;
COPY tmp_table FROM '/tmp/afklm_scheduled_flights_from_mongo.csv' CSV HEADER;
INSERT INTO mongodb_future SELECT * FROM tmp_table ON CONFLICT DO NOTHING;

ALTER TABLE mongodb_future ADD COLUMN flight_id VARCHAR(50);
UPDATE mongodb_future SET flight_id = CONCAT (id, flightLegs_depInfo_airport_code, flightLegs_arrInfo_airport_code);
DROP TABLE IF EXISTS tmp_table;

