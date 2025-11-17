import pandas as pd
import requests
import os

csv_files = sorted([f for f in list(os.listdir('data')) if 'afklm_flight_from_mongo_filtered' in f])
csv_to_import = csv_files[-1]
df = pd.read_csv(f'{'data'}/{csv_to_import}')

df.columns
df_test = df.head(1).reset_index().to_json(orient='records')


r = requests.get(
    url='http://127.0.0.1:8000//verify'
    )


response_dict = r.json
response_header = r.headers
status_code = r.status_code
response_content = r.content

response_content




r = requests.post(url='http://127.0.0.1:8000/get_delay_predictions',
    data = df_test,
    headers={"Content-Type": "application/json"}
    )


response_dict = r.json
response_header = r.headers
status_code = r.status_code
response_content = r.content

(response_dict)

