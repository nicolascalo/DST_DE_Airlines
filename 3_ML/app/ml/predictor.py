import pandas as pd
from src.preprocessing.feature_engineering import add_features
from config.config_ml import SETTINGS_ML
from app.ml.model_loader import (
    classification_status_model,
    classification_delay_model,
    regression_model,
)


def predict_flight(entry_dict: dict):

    entry = pd.DataFrame([entry_dict])
    entry_cleaned = add_features(entry, SETTINGS_ML)

    status = classification_status_model.predict(entry_cleaned)[0]

    if status != "LATE":
        return {
            "predicted_flightLeg_status": status,
            "predicted_delay_min_classification": "NA",
            "predicted_delay_min_regression": "NA",
        }

    delay_class = classification_delay_model.predict(entry_cleaned)[0]
    delay_reg = regression_model.predict(entry_cleaned)[0].item()

    return {
        "predicted_flightLeg_status": status,
        "predicted_delay_min_classification": delay_class,
        "predicted_delay_min_regression": delay_reg,
    }
