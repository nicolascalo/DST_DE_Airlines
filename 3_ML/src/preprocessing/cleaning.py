from config.config_ml import SETTINGS_ML
from src.utils.logger import get_logger

logger = get_logger()



def clean_dataset(df, settings):
    df = df.copy()

    logger.info(f"================================== Data cleaning ==================================")
    logger.info(f"Step: 'raw_data' | shape: {df.shape} | null values: {df.isnull().sum().sum()}")


    logger.info(f"Dropping columns: {df.filter(regex='|'.join(SETTINGS_ML['columnKeywordsToDrop_all'])).columns}")

    # drop columns by regex list
    if settings.get('columnKeywordsToDrop_all'):
        pat = '|'.join(settings['columnKeywordsToDrop_all'])
        cols_to_drop = df.filter(regex=pat).columns.tolist()
        df = df.drop(columns=cols_to_drop, errors='ignore')


    logger.info(f"Dropping rows with missing {SETTINGS_ML['TARGET_CLASSIFICATION_STATUS']}")

    # keep only relevant status values (defensive)
    status_col = settings['TARGET_CLASSIFICATION_STATUS']
    if status_col in df.columns:
        df = df[df[status_col].isin(['ARRIVED','CANCELLED','DELAYED_DEPARTURE','ONTIME'])]

    logger.info(f"Step: filtered past flights only | shape: {df.shape} | null values: {df.isnull().sum().sum()}")

    logger.info(f"Filled missing {SETTINGS_ML['TARGET_REGRESSION']} with 0")
    # fill regression target nulls with 0
    reg_col = settings['TARGET_REGRESSION']
    if reg_col in df.columns:
        df[reg_col] = df[reg_col].fillna(0)

    logger.info(f"Step: After cleaning | shape: {df.shape} | null values: {df.isnull().sum().sum()}")


    if SETTINGS_ML['airports_optional']:
        logger.info("Filtering airports in optional list")
        df = df[df['flightlegs_arrinfo_airport_code'].isin(SETTINGS_ML['airports_optional']) &
                        df['flightlegs_depinfo_airport_code'].isin(SETTINGS_ML['airports_optional'])]
        if SETTINGS_ML['airports_mandatory']:
            logger.info("Filtering airports in mandatory list")
            df = df[df['flightlegs_arrinfo_airport_code'].isin(SETTINGS_ML['airports_mandatory']) |
                            df['flightlegs_depinfo_airport_code'].isin(SETTINGS_ML['airports_mandatory'])]
            
    logger.info(f"Step: Airport_filtering | shape: {df.shape} | null values: {df.isnull().sum().sum()}")

    return df




