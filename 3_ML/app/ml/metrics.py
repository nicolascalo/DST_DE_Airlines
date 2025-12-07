from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.ml.model_loader import classification_status_model, classification_delay_model, regression_model, BEST_MODEL_DIR
import os
import pandas as pd

router = APIRouter(
    prefix="/metrics",
    tags=["metrics"],
    responses={404: {"description": "Not found"}},
)


@router.get("/get_model_metrics", name="Retrieve metrics of the current best models")
def get_model_metrics():
    """
    Returns the metrics of the current best models as JSON.
    Expects a CSV named `best_models.csv` in the best_model folder.
    """
    metrics_file = os.path.join(BEST_MODEL_DIR, "best_models.csv")

    if not os.path.exists(metrics_file):
        raise HTTPException(status_code=404, detail="No metrics file found")

    try:
        df = pd.read_csv(metrics_file)
        metrics_json = [row.dropna().to_dict() for _, row in df.iterrows()]
        return JSONResponse(content=metrics_json)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read metrics file: {e}")
