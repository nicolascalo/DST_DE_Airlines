
import json, os
from datetime import datetime
from logger import get_logger

logger = get_logger()



DEFAULTS = {
    "GRID_LEVEL": "balanced",
    "MODEL_LIST_TO_TEST": {
        "classification": [
            "Logistic_OVR",
            "RandomForest",
            "XGBoostClassifier",
            "DecisionTree",
            "LightGBMClassifier"
        ],
        "regression": [
            "LinearRegression",
            "RandomForestRegressor",
            "XGBRegressor",
            "GradientBoostingRegressor"
        ]
    },
    "PARALLEL_JOBS": 6,
    "TEST_SIZE": 0.2,
    "RANDOM_STATE": 42,
    "DATA_DIR":"data",
    "OUTPUT_DIR":"outputs"
}

cfg_path = os.path.join("config","afklm_ml_training_settings.json")
if os.path.exists(cfg_path):
    try:
        with open(cfg_path) as f:
            DEFAULTS.update(json.load(f))
            logger.info(f"ML pipeline SETTINGS_ML loaded from afklm_ml_training_settings.json")
    except: 
        logger.warning(f"afklm_ml_training_settings.json not found. ML pipeline SETTINGS_ML loaded from default parameters")
        pass

SETTINGS_ML = DEFAULTS
SETTINGS_ML["RUN_TIMESTAMP"] = datetime.now().strftime("%Y%m%d_%H%M%S")
