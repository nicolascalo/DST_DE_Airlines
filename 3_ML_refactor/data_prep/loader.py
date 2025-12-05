import os
import pandas as pd
from settings_ml import SETTINGS
from logger import get_logger

logger = get_logger()

def load_latest_csv(settings):
    data_dir = settings['DATA_DIR']
    files = sorted([f for f in os.listdir(data_dir) if f.lower().endswith('.csv')])
    if not files:
        raise FileNotFoundError(f"No CSVs found in {data_dir}")
    latest = files[-1]
    df = pd.read_csv(os.path.join(data_dir, latest), low_memory=False)
    logger.info(f"Loaded dataset: {latest}, shape={df.shape}")
    return df


def load_best_models(settings):
    return "test"
    try:
        best_models = pd.read_csv(f"{settings['DATA_DIR']}/best_models.csv")
        best_model_classification_status_score = best_models[best_models['problem_type'] == 'classification_status']['accuracy'].item()
        best_model_classification_delay_score = best_models[best_models['problem_type'] == 'classification_delay']['accuracy'].item()
        best_model_regression_score = best_models[best_models['problem_type'] == 'regression']['accuracy'].item()
    except:
        best_model_classification_status_score = -100
        best_model_classification_delay_score = -100
        best_model_regression_score = -100
    
    return {"best_model_classification_status_score":best_model_classification_status_score, "best_model_classification_delay_score":best_model_classification_delay_score, "best_model_regression_score":best_model_regression_score}
    
