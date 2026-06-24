from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl


class EventIngestSchema(BaseModel):
    event_id: str = Field(..., description="Unique event identifier")
    event_name: str = Field(..., description="Name of the event")
    timestamp: datetime = Field(..., description="Event timestamp in ISO 8601 format")
    source: str = Field(..., description="Source system or service")
    payload: dict = Field(..., description="Event payload data")
    callback_url: HttpUrl | None = Field(None, description="Optional callback URL for processing results")
