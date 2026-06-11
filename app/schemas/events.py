from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class EventInSchema(BaseModel):
    event_id: UUID
    event_type: str = Field(min_length=1)
    source: str = Field(min_length=1)
    event_time: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class EventAcceptedSchema(BaseModel):
    status: str
    event_id: UUID


class EventOutSchema(BaseModel):
    event_id: UUID
    event_type: str
    source: str
    event_time: datetime
    payload: dict[str, Any]
    created_at: datetime


class EventStatsOutSchema(BaseModel):
    event_type: str
    count: int


class EventRetryMessageSchema(BaseModel):
    event: EventInSchema
    retry_count: int = Field(default=0, ge=0)
    error: str | None = None
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EventDLQMessageSchema(BaseModel):
    event: EventInSchema
    retry_count: int = Field(ge=0)
    error: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
