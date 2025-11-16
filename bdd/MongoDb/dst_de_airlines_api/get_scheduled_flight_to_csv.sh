read -p "nb_flight: " nb_flights

./venv/bin/python3 -m API.get_scheduled_flight_to_csv "$nb_flights"



