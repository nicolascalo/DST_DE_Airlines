from fastapi import APIRouter

router = APIRouter(tags=["tests"])

@router.get("/health")
def health_check():
    return "API active"
