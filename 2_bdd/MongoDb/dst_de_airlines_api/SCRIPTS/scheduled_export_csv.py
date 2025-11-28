from USE_CASES.get_csv_flights_uc import get_csv_flights
import sys

if len(sys.argv) < 2:
    sys.exit(1)
nb_flights = int(sys.argv[1])

date_param = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].strip() else None

start_id = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3].strip() else None


collection_name = "scheduled_flights"

df, filename  = get_csv_flights(collection_name, date_param, start_id, nb_flights)

df.to_csv(filename, index = 0,na_rep = "",compression='gzip')


