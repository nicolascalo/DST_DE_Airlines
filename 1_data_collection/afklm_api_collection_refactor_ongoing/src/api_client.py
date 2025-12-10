import requests
import time
from src.utils import info_message, compress_json, ensure_folder
import datetime
import re
import os

def call_api(url, headers, last_call_time, time_delay=1.1):
    """Perform API call with minimum delay between calls"""
    while (time.time() - last_call_time) < time_delay:
        time.sleep(0.1)
    response = requests.get(url, headers=headers)
    return response, time.time()

def should_skip(df_subset, skip_complete=True,
                skip_server=True,
                skip_not_found=True,
                skip_invalid_range=True,
                skip_other=True):
    """Determine if a query should be skipped based on previous response or completion"""
    to_test = float(df_subset.get('completion', [0])[0] or 0)
    startRange = df_subset['startRange'].item()
    day_diff_start = (datetime.datetime.fromisoformat(startRange.replace('Z','')).date() - datetime.datetime.now().date()).days

    # Skip based on completion
    if skip_complete and (
        (to_test == 100 and day_diff_start < 0) or
        (to_test == 66 and day_diff_start == 0) or
        (to_test == 33 and day_diff_start > 0)
    ):
        return True, "Already completed"

    # Skip based on previous response
    text_response = str(df_subset.get('response', [''])[0])
    match_error = re.search(r"\d\d\d", text_response)
    code = int(match_error[0]) if match_error else 0

    if code >= 500 and skip_server:
        return True, "Previously failed: server error"
    if code == 404 and skip_not_found:
        return True, "Previously failed: flight not found"
    if code == 416 and skip_invalid_range:
        return True, "Previously failed: invalid date range"
    if code > 200 and skip_other:
        return True, "Previously failed: other error"

    # Date range limits
    if day_diff_start < -180 or (datetime.datetime.fromisoformat(df_subset['endRange'].item().replace('Z','')).date() - datetime.datetime.now().date()).days < -180:
        return True, "Date too far in the past"
    if day_diff_start > 365:
        return True, "Start date too far in future"
    if (datetime.datetime.fromisoformat(df_subset['endRange'].item().replace('Z','')).date() - datetime.datetime.now().date()).days > 365:
        return True, "End date too far in future"

    return False, ""

def process_pages(df_subset, base_url, headers, data_folder, last_call_time, max_pages):
    """Handles API call with pagination, skip rules, and completion tracking"""
    ensure_folder(data_folder)

    skip, message = should_skip(df_subset)
    if skip:
        info_message(f"Skipping query: {message}", 'magenta')
        return last_call_time

    pageNumber = df_subset.get('nb_of_pages_already_retrieved', [0])[0] or 0

    while pageNumber < max_pages:
        file_name = f"{data_folder}/afklm_{df_subset['call_parameters'].item().replace(':','_')}_{pageNumber}.json.gz"
        url_page = f"{base_url}{df_subset['call_parameters'].item()}&pageNumber={pageNumber}"
        response, last_call_time = call_api(url_page, headers, last_call_time)

        if response.ok:
            data = response.json()
            compress_json(data, file_name)

            # Update completion percentage
            page_max = data['page']['totalPages']
            totalFlights = data['page']['fullCount']

            startRange = df_subset['startRange'].item()
            day_diff_start = (datetime.datetime.fromisoformat(startRange.replace('Z','')).date() - datetime.datetime.now().date()).days
            if day_diff_start < 0:
                completion = 100 * (pageNumber + 1) / page_max
            elif day_diff_start == 0:
                completion = 66 * (pageNumber + 1) / page_max
            else:
                completion = 33 * (pageNumber + 1) / page_max

            df_subset.at[0, 'completion'] = completion
            df_subset.at[0, 'nb_of_pages_already_retrieved'] = pageNumber + 1
            df_subset.at[0, 'totalPages'] = page_max
            df_subset.at[0, 'totalFlights'] = totalFlights
            df_subset.at[0, 'timestamp'] = datetime.datetime.now().isoformat()
            info_message(f"Page {pageNumber} retrieved (completion {completion:.0f}%)", 'green')

            pageNumber += 1
        else:
            info_message(f"API call failed: {response.status_code} - {response.text}", 'red')
            break

    return last_call_time
