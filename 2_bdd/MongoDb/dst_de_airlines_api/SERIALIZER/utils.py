from bson import ObjectId
from datetime import datetime
import json
from typing import Any

def mongo_to_json(obj: Any) -> Any:
    """Convertit les objets MongoDB (ObjectId, datetime) en types JSON compatibles."""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: mongo_to_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [mongo_to_json(v) for v in obj]
    return obj