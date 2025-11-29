from USE_CASES.get_df_flights_uc import get_df_flights
from SERVICES.exploration_gz_file import create_csv_tar_gz
import sys

nb_flights = None
if len(sys.argv) > 1 and sys.argv[1].strip():
    nb_flights = int(sys.argv[1])

date_param = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].strip() else None

start_id = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3].strip() else None


collection_name = "scheduled_flights"

df, filename  = get_df_flights(collection_name, date_param, start_id, nb_flights)

tar_content = create_csv_tar_gz(df, filename)

tar_filename = filename.replace('.csv', '.tar.gz')

with open(tar_filename, 'wb') as f:
    f.write(tar_content) 



