from USE_CASES.get_csv_historic_uc import get_historic_flights_to_csv
import sys

if len(sys.argv) < 2:
    print("Usage: python get_all_csv.py <nb_flights>")
    sys.exit(1)
nb_flights = int(sys.argv[1])

df, filename  = get_historic_flights_to_csv(nb_flights)
df.to_csv(filename, index = 0,na_rep = "",compression='gzip')

