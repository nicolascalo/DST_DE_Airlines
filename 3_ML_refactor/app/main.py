from fastapi import FastAPI
from app.api.health import router as health_router
from app.api.predictions import router as predictions_router
from app.api.training import router as training_router
from app.api.artifacts import router as artifacts_router
from app.api.artifacts import router as artifacts_router
from app.ml.metrics import router as metrics_router


app = FastAPI(
    title="Air France KLM - ML API",
    description="Flight delay prediction API",
    docs_url="/"
)

app.include_router(health_router)
app.include_router(predictions_router)
app.include_router(training_router)
app.include_router(artifacts_router)
app.include_router(metrics_router)
