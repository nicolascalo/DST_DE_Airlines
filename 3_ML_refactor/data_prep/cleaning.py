import pandas as pd
import re
from settings_ml import SETTINGS
from logger import get_logger

logger = get_logger()

def clean_dataset(df, settings):
    df = df.copy()
    # small defensive cleaning from original script
    logger.info('Raw shape: %s', df.shape)

    # extract company_flight if flight_id exists
    if 'flight_id' in df.columns:
        df['company_flight'] = df['flight_id'].astype(str).apply(lambda x: re.sub(r'^.*?\\+', '', x))

    # drop columns by regex list
    if settings.get('columnKeywordsToDrop_all'):
        pat = '|'.join(settings['columnKeywordsToDrop_all'])
        cols_to_drop = df.filter(regex=pat).columns.tolist()
        df = df.drop(columns=cols_to_drop, errors='ignore')

    # keep only relevant status values (defensive)
    status_col = settings['TARGET_CLASSIFICATION_STATUS']
    if status_col in df.columns:
        df = df[df[status_col].isin(['ARRIVED','CANCELLED','DELAYED_DEPARTURE','ONTIME'])]

    # fill regression target nulls with 0
    reg_col = settings['TARGET_REGRESSION']
    if reg_col in df.columns:
        df[reg_col] = df[reg_col].fillna(0)

    logger.info('After cleaning shape: %s', df.shape)
    return df
