from USE_CASES.format_for_tabular_data_uc import format_for_tabular_data
import sys

if len(sys.argv) < 2:
    print("Usage: python get_all_csv.py <nb_flights>")
    sys.exit(1)
nb_flights = int(sys.argv[1])
format_for_tabular_data(nb_flights)