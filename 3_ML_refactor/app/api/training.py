from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from app.ml.orchestrator import run_training_pipeline
from app.ml.settings_loader import load_training_settings
from app.schemas.training import PayloadTrainingParameters
import json
import shutil

router = APIRouter(tags=["training"])


@router.get("/retrain_models", response_class=PlainTextResponse)
def retrain_models():
    """Retrain the machine learning models with the current dataset"""
    try:
        run_training_pipeline()
        return "Model training completed successfully"
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/training_parameters_show")
def get_training_parameters():
    """Get current parameters for model training"""
    return JSONResponse(load_training_settings())


@router.post("/training_parameters_upload", response_class=PlainTextResponse)
def set_training_parameters(parameters: PayloadTrainingParameters):
    """Set current parameters for model training"""

    with open("./config/afklm_ml_training_settings.json", "w") as f:
        json.dump(parameters.model_dump(), f, indent=4)

    return "Training parameters updated"


@router.get("/training_parameters_defaults", response_class=PlainTextResponse)
def reset_training_parameters():
    """Reset model parameters to default"""
    shutil.copyfile(
        "./config/afklm_ml_training_settings_default.json",
        "./config/afklm_ml_training_settings.json"
    )
    return "Training parameters reset to defaults"
