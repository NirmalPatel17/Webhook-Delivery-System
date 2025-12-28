from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, model_validator


class WebhookEvent(BaseModel):
    payload: Dict[Any, Any]
    status: str = "RECEIVED"
    received_at: datetime = Field(default_factory=datetime.utcnow)
    event_type: Optional[str] = None
    delivery_attempts: List[Dict[str, Any]] = Field(default_factory=list)
    idempotency_key: Optional[str] = None


class SearchWebhooksBody(BaseModel):
    status: Optional[str] = None
    event_type: Optional[str] = None
    from_timestamp: Optional[datetime] = None
    to_timestamp: Optional[datetime] = None
    skip: int = 0
    limit: int = 10

    @model_validator(mode="after")
    def check_timestamps(cls, model):
        if model.from_timestamp and model.to_timestamp:
            if model.to_timestamp <= model.from_timestamp:
                raise ValueError("`to_timestamp` must be greater than `from_timestamp`")
        return model
    
