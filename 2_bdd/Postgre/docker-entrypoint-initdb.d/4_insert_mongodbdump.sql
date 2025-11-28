
COPY mongodb FROM '/tmp/data_input/afklm_historic_flight_from_mongo_filtered.csv' CSV HEADER;
COPY mongodb FROM '/tmp/data_input/afklm_sched_flight_from_mongo_filtered.csv' CSV HEADER;

