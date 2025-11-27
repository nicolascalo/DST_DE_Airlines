from USE_CASES.get_schedulled_csv_uc import get_csv_schedulled_by_id
import sys

if len(sys.argv) < 2:
    sys.exit(1)
nb_flights = int(sys.argv[1])

date_param = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].strip() else None

start_id = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3].strip() else None

df, filename  = get_csv_schedulled_by_id(nb_flights, start_id, date_param)

df.to_csv(filename, index = 0,na_rep = "",compression='gzip')


