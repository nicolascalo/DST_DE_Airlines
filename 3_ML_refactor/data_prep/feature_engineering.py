from settings_ml import SETTINGS
from logger import get_logger
import datetime

logger = get_logger()

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

def add_time_features(df, settings):
    df = df.copy()

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

    # season
    if 'flightlegs_depinfo_times_scheduled' in df.columns:
        df['flightlegs_season'] = df['flightlegs_depinfo_times_scheduled'].apply(lambda x: season_dictionary.get(safe_isoparse(x).month) if safe_isoparse(x) else None)
        df['flightlegs_arrinfo_times_scheduled_isWeekend'] = df['flightlegs_arrinfo_times_scheduled'].apply(lambda x: True if (safe_isoparse(x) and safe_isoparse(x).isoweekday() in [6,7]) else False)
        df['flightlegs_arrinfo_times_scheduled_dayPeriod'] = df['flightlegs_arrinfo_times_scheduled'].apply(lambda x: get_dayPeriod(safe_isoparse(x).hour + safe_isoparse(x).minute/60) if safe_isoparse(x) else None)

    return df
