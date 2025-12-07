from config.config_ml import SETTINGS_ML
from src.utils.logger import get_logger

logger = get_logger()


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





def engineer_classification_targets(df, SETTINGS_ML):
    logger.info(f"================================== Target engineering ==================================")
    df = df.copy()

    logger.info("Categorizing flights into 'CANCELLED' (also if delay > 360min), 'LATE', 'ONTIME'")

    df[SETTINGS_ML['TARGET_CLASSIFICATION_STATUS']] = df.apply(categorize_status, axis=1)
    logger.info(f"Step: categorize delay status | shape: {df.shape} | null values: {df.isnull().sum().sum()}")



    df[SETTINGS_ML['TARGET_CLASSIFICATION_DELAY']] = df.apply(categorize_delay, axis=1)
    logger.info(f'Step: categorize delay brackets | {set(df[SETTINGS_ML['TARGET_CLASSIFICATION_DELAY']])}')
    logger.info(f"Step: categorize delay brackets | shape: {df.shape} | null values: {df.isnull().sum().sum()}")




    return df
