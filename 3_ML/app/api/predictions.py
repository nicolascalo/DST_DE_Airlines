from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd

from app.schemas.flight import PayloadFlight
from app.ml.model_loader import classification_status_model, classification_delay_model, regression_model
from src.preprocessing.feature_engineering import add_features
from config.config_ml import SETTINGS_ML

router = APIRouter(
    prefix="/predictions",
    tags=["predictions"],
    responses={404: {"description": "Not found"}},
)


@router.post("/get_delay_predictions", name="Get flight delay predictions")
def get_delay_predictions(payload: PayloadFlight):
    """
    Predict flight status and delay duration.
    - Status model predicts ONTIME / LATE / CANCELLED
    - Delay models only run if flight predicted as LATE
    """

    # Convert payload to DataFrame
    try:
        entry_dict = payload.model_dump()
        entry_df = pd.DataFrame([entry_dict])
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    # Feature engineering
    try:
        entry_cleaned = add_features(entry_df, SETTINGS_ML, create_log = False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feature engineering failed: {e}")

    # Predict flight status
    if classification_status_model is None:
        raise HTTPException(status_code=500, detail="Status model not loaded")

    try:
        prediction_status = classification_status_model.predict(entry_cleaned)[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status prediction failed: {e}")

    # Predict delay (if late)
    if prediction_status == "LATE":

        # Classification of delay bucket
        if classification_delay_model is None:
            prediction_delay_class = "No delay classification model"
        else:
            try:
                prediction_delay_class = classification_delay_model.predict(entry_cleaned)[0]
            except Exception as e:
                prediction_delay_class = f"Error: {e}"

        # Regression for delay duration
        if regression_model is None:
            prediction_delay_regression = "No regression model"
        else:
            try:
                prediction_delay_regression = regression_model.predict(entry_cleaned)[0].item()
            except Exception as e:
                prediction_delay_regression = f"Error: {e}"

    else:
        prediction_delay_class = "NA"
        prediction_delay_regression = "NA"

    # Return response
    return JSONResponse(
        content={
            "predicted_flight_status": prediction_status,
            "predicted_delay_min_classification": prediction_delay_class,
            "predicted_delay_min_regression": prediction_delay_regression
        }
    )
