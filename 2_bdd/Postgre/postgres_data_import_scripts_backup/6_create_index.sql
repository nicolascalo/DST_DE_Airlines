
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