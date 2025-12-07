import os
import pickle
from config.config_ml import SETTINGS_ML
import pandas as pd

# Folder where best models are stored
BEST_MODEL_DIR = os.path.join(SETTINGS_ML["OUTPUT_DIR"], "best_models")

def _load_model(keyword: str):
    """Load the first .pkl file in BEST_MODEL_DIR containing the keyword."""
    for fname in os.listdir(BEST_MODEL_DIR):
        if fname.endswith(".pkl") and keyword in fname:
            with open(os.path.join(BEST_MODEL_DIR, fname), "rb") as f:
                return pickle.load(f)
    return None

# Load models once at import
classification_status_model = _load_model("classification_status")
classification_delay_model = _load_model("classification_delay")
regression_model = _load_model("regression")



def _load_model_metrics(keyword: str):
    try:
        best_models_metrics = pd.read_csv(f'{BEST_MODEL_DIR}/best_models.csv')
        best_models_metrics_json = ([row.dropna().to_dict() for index,row in best_models_metrics.iterrows()])
    except:
        print("No model metrics found")
