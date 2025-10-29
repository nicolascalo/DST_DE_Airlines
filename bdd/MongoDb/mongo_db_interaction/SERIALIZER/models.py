from bson import ObjectId
from pydantic import BaseModel, field_serializer
from typing import Any

class MongoModel(BaseModel):
    id: Any 

    @field_serializer('id')
    def serialize_objectid(self, _id: ObjectId, _info):
        return str(_id)  # Convertit ObjectId en chaîne
