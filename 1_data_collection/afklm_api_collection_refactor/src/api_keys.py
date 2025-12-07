import pandas as pd
import datetime
import os

def load_api_keys(api_key_folder, on_cloud=False, bucket=None):
    """Load API keys and reset daily counters if needed"""
    df = pd.DataFrame()
    today = datetime.datetime.now().date()

    if on_cloud:
        # TODO: implement GCP bucket read if needed
        pass
    else:
        for file in os.listdir(api_key_folder):
            if file.endswith(".csv"):
                df = pd.concat([df, pd.read_csv(os.path.join(api_key_folder, file))], ignore_index=True)

    df['timestamp'] = df['timestamp'].fillna(datetime.datetime.now().isoformat())

    # Reset daily call counters if date changed
    df['nb_calls_today'] = df.apply(
        lambda row: 0 if (today - datetime.datetime.fromisoformat(row['timestamp']).date()).days > 0 else row['nb_calls_today'],
        axis=1
    )

    # Update timestamp for reset rows
    df['timestamp'] = df.apply(
        lambda row: datetime.datetime.now().isoformat() if (today - datetime.datetime.fromisoformat(row['timestamp']).date()).days > 0 else row['timestamp'],
        axis=1
    )
    return df

def get_available_key(df, max_daily_calls=100):
    """Return the first API key that has remaining quota"""
    for idx, row in df.iterrows():
        if row['nb_calls_today'] < max_daily_calls:
            return row, idx
    return None, None

def increment_key_usage(df, key_idx, last_call_time):
    """Increment daily call counter and update timestamp"""
    df.at[key_idx, 'nb_calls_today'] += 1
    df.at[key_idx, 'timestamp'] = datetime.datetime.now().isoformat()
    return df
