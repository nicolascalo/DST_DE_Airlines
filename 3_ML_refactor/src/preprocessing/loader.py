import os
import pandas as pd
from config.config_ml import SETTINGS_ML
from src.utils.logger import get_logger

logger = get_logger()

def load_latest_csv(SETTINGS_ML):
    logger.info(f"================================== Data loading ==================================")


    data_dir = SETTINGS_ML['DATA_DIR']
    logger.info(f"Looking for .csv.gz files in {data_dir}")

    files = sorted([f for f in os.listdir(data_dir) if f.lower().endswith('.csv.zip')])
    logger.info(f"Found files {files}")
    
    if not files:
        raise FileNotFoundError(f"No CSVs found in {data_dir}")
    latest = files[-1]
    logger.info(f"Loading {latest}")
    df = pd.read_csv(os.path.join(data_dir, latest), low_memory=False)
    logger.info(f"Loaded dataset: {latest}, shape={df.shape}")
    return df


def load_best_models(SETTINGS_ML):
    logger.info(f"================================== Model loading ==================================")
    try:
        best_models = pd.read_csv(f"{SETTINGS_ML['OUTPUT_DIR']}/best_models/best_models.csv")
        best_model_classification_status_score = best_models[best_models['problem_type'] == 'classification_status']['accuracy'].item()
        best_model_classification_delay_score = best_models[best_models['problem_type'] == 'classification_delay']['accuracy'].item()
        best_model_regression_score = best_models[best_models['problem_type'] == 'regression']['accuracy'].item()
    except:
        best_model_classification_status_score = -100
        best_model_classification_delay_score = -100
        best_model_regression_score = -100
        best_models = pd.DataFrame()
    
    return {"classification_status":best_model_classification_status_score, "classification_delay":best_model_classification_delay_score, "regression":best_model_regression_score, "df_best_models":best_models}
    
