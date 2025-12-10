import pandas as pd
import datetime
from src.utils import info_message

def update_call_parameters(df, future_days_to_retrieve, non_parameters):
    """Add missing future dates for API queries"""
    df_root = df.drop(non_parameters, axis=1, errors='ignore').drop_duplicates()
    params = list(df_root.columns)
    for col in ['startRange', 'endRange']:
        if col in params:
            params.remove(col)
    df_root = df_root.drop('startRange', axis=1, errors='ignore').groupby(params).max().reset_index()

    new_rows = []
    for _, row in df_root.iterrows():
        endRange = str(row.get('endRange')).replace('Z','')
        while (datetime.datetime.fromisoformat(endRange).date() - datetime.datetime.now().date()).days < future_days_to_retrieve:
            new_row = row.copy()
            start = (datetime.datetime.fromisoformat(endRange) + datetime.timedelta(seconds=1)).isoformat() + "Z"
            end = (datetime.datetime.fromisoformat(endRange) + datetime.timedelta(days=1)).isoformat() + "Z"
            new_row['startRange'] = start
            new_row['endRange'] = end
            new_rows.append(new_row)
            endRange = end[:-1]
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return df

def generate_call_urls(df, non_parameters):
    """Generate API call URL parameters"""
    call_parameters_list = []
    for i in range(len(df)):
        row_dict = df.iloc[i].drop(non_parameters).to_dict()
        url_params = "&".join(f"{k}={v}" for k, v in row_dict.items() if v not in ['', '[nan]'])
        call_parameters_list.append(url_params)
    df['call_parameters'] = call_parameters_list
    return df
