read -p "nb_flight: " nb_flights

./venv/bin/python3 -m mongo_db_interaction.get_historic_flight_to_csv "$nb_flights"



