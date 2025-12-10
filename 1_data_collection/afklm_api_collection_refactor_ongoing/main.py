import os
from src.config import *
from src.utils import info_message, ensure_folder
from src.gcp_helpers import read_csv, save_csv, list_files
from src.api_keys import load_api_keys, get_available_key, increment_key_usage
from src.parameter_manager import update_call_parameters, generate_call_urls
from src.api_client import process_pages
from google.cloud import storage
from dotenv import load_dotenv

ensure_folder(DATA_FOLDER)

# Detect if running on cloud (GCP)
try:
    load_dotenv(verbose=True)
    client_storage = storage.Client()
    BUCKET = client_storage.bucket(BUCKET_NAME)
    on_cloud = True
except:
    on_cloud = False
    BUCKET = None

# Load API keys
api_keys_df = load_api_keys(API_KEY_FOLDER)

# List call parameter CSVs
call_parameter_csv_list = list_files(
    file_folder=CALL_PARAMETER_FOLDER,
    pattern='df_call_parameters',
    ext='csv',
    bucket=BUCKET,
    on_cloud=on_cloud
)

for call_csv in call_parameter_csv_list:
    info_message(f"Processing {call_csv}", 'blue')
    df_params = read_csv(CALL_PARAMETER_FOLDER, call_csv, bucket=BUCKET, on_cloud=on_cloud)
    df_params = update_call_parameters(df_params, FUTURE_DAYS_TO_RETRIEVE, NON_PARAMETERS)
    df_params = generate_call_urls(df_params, NON_PARAMETERS)

    # Loop through parameter rows
    for i, param_row in df_params.iterrows():
        while True:
            key_row, key_idx = get_available_key(api_keys_df, MAX_DAILY_API_CALL)
            if key_row is None:
                info_message("No more API keys available. All daily quotas reached.", 'red')
                break

            headers = {'Content-Type': 'application/x-www-form-urlencoded', 'API-Key': key_row['api_key']}
            last_call_time = 0

            # Process pages for this parameter row
            last_call_time = process_pages(param_row.to_frame().T, BASE_URL, headers, DATA_FOLDER, last_call_time, MAX_PAGE_TO_FETCH)

            # Increment API key usage
            api_keys_df = increment_key_usage(api_keys_df, key_idx, last_call_time)
            save_csv(api_keys_df, API_KEY_FOLDER, "afklm_api_keys.csv", bucket=BUCKET, on_cloud=on_cloud)

            # If quota reached for this key, continue to next key automatically
            if api_keys_df.at[key_idx, 'nb_calls_today'] >= MAX_DAILY_API_CALL:
                info_message(f"API key {key_row['key_desc']} quota reached. Rotating to next key.", 'yellow')
                continue
            break
