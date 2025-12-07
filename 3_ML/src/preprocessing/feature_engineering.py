from config.config_ml import SETTINGS_ML
from src.utils.logger import get_logger
import datetime
import re


def categorize_delay(row):   
    delay_value = row[SETTINGS_ML['TARGET_REGRESSION']] 
    if delay_value < 5:
        return "]000;005]"
    if delay_value < 15:
        return "]005;015]"
    elif delay_value < 30:
        return "]015;030]"
    elif delay_value < 60:
        return "]030;060]"
    elif delay_value < 120:
        return "]060;120]"
    elif delay_value < 240:
        return "]120;240]"
    else:
        return "]240;360]"
    

def categorize_status(row):
    if (row[SETTINGS_ML['TARGET_CLASSIFICATION_STATUS']]=='CANCELLED') |(row[SETTINGS_ML['TARGET_REGRESSION']]>360) : 
        return 'CANCELLED'
    return 'LATE' if row[SETTINGS_ML['TARGET_REGRESSION']] > 0 else 'ONTIME'



def get_dayPeriod(x):
    if (x >= 6) and (x < 12):
        return 'morning'
    if (x >= 12) and (x < 18):
        return 'afternoon'
    if (x >= 18) and (x < 24):
        return 'evening'
    return 'night'

season_dictionary = {1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall',12:'winter'}

def safe_isoparse(val):
    try:
        return datetime.datetime.fromisoformat(val)
    except Exception:
        return None

def add_features(df, SETTINGS_ML, create_log = True):


    if create_log:
        logger = get_logger()
        logger.info(f"================================== Feature engineering ==================================")
    
    df = df.copy()


    # extract company_flight if flight_id exists
    if create_log:
        logger.info("Extracting 'company_flight' from 'flight_id' column")


    if 'flight_id' in df.columns:
        df['company_flight'] = df['flight_id'].astype(str).apply(lambda x: re.sub(r'^.*?\\+', '', x))


    # compose scheduled datetime strings if columns exist
    if set(['flightlegs_arrinfo_times_scheduled_date','flightlegs_depinfo_times_scheduled_time','flightlegs_arrinfo_times_scheduled_timezone']).issubset(df.columns):
        df['flightlegs_arrinfo_times_scheduled'] = df.apply(lambda r: f"{r.flightlegs_arrinfo_times_scheduled_date}T{r.flightlegs_depinfo_times_scheduled_time}.000{r.flightlegs_arrinfo_times_scheduled_timezone}", axis=1)

    if set(['flightlegs_depinfo_times_scheduled_date','flightlegs_depinfo_times_scheduled_time','flightlegs_depinfo_times_scheduled_timezone']).issubset(df.columns):
        df['flightlegs_depinfo_times_scheduled'] = df.apply(lambda r: f"{r.flightlegs_depinfo_times_scheduled_date}T{r.flightlegs_depinfo_times_scheduled_time}.000{r.flightlegs_depinfo_times_scheduled_timezone}", axis=1)

    # scheduled duration in minutes
    if 'flightlegs_arrinfo_times_scheduled' in df.columns and 'flightlegs_depinfo_times_scheduled' in df.columns:
        def compute_duration(row):
            a = safe_isoparse(row['flightlegs_arrinfo_times_scheduled'])
            d = safe_isoparse(row['flightlegs_depinfo_times_scheduled'])
            if a and d:
                return (a - d).seconds / 60
            return None
        df['flightlegs_scheduledflightduration'] = df.apply(compute_duration, axis=1)

    if create_log:
        logger.info("Adding seasonality, isWeekend and dayPeriod")


    if 'flightlegs_depinfo_times_scheduled' in df.columns:

        # season
        df['flightlegs_season'] = df['flightlegs_depinfo_times_scheduled'].apply(lambda x: season_dictionary.get(safe_isoparse(x).month) if safe_isoparse(x) else None)

        # weekend
        df['flightlegs_arrinfo_times_scheduled_isWeekend'] = df['flightlegs_arrinfo_times_scheduled'].apply(lambda x: True if (safe_isoparse(x) and safe_isoparse(x).isoweekday() in [6,7]) else False)

        df['flightlegs_depinfo_times_scheduled_isWeekend'] = df['flightlegs_depinfo_times_scheduled'].apply(lambda x: True if (safe_isoparse(x) and safe_isoparse(x).isoweekday() in [6,7]) else False)

        # day period
        df['flightlegs_arrinfo_times_scheduled_dayPeriod'] = df['flightlegs_arrinfo_times_scheduled'].apply(lambda x: get_dayPeriod(safe_isoparse(x).hour + safe_isoparse(x).minute/60) if safe_isoparse(x) else None)

        df['flightlegs_depinfo_times_scheduled_dayPeriod'] = df['flightlegs_depinfo_times_scheduled'].apply(lambda x: get_dayPeriod(safe_isoparse(x).hour + safe_isoparse(x).minute/60) if safe_isoparse(x) else None)

    return df



