read -p "nb_flight: " nb_flights

./venv/bin/python3 -m mongo_db_interaction.get_update_d1_flight_to_csv "$nb_flights"



