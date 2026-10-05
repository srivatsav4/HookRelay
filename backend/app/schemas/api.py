from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

EVENT_TYPE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,119}$")


class LoginIn(BaseModel):
    email: str
    password: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class EventIn(BaseModel):
    type: str = Field(max_length=120)
    payload: dict[str, Any]
    timestamp: datetime | None = None

    @field_validator("type")
    @classmethod
    def _type(cls, v: str) -> str:
        if not EVENT_TYPE_RE.match(v):
            raise ValueError("type must match [a-zA-Z0-9._:-]{1,120}")
        return v


class EventAccepted(BaseModel):
    event_id: uuid.UUID
    duplicate: bool
    deliveries: int
    delivery_ids: list[uuid.UUID]


class EndpointIn(BaseModel):
    url: str = Field(max_length=2048)
    description: str | None = Field(default=None, max_length=255)
    event_types: list[str] = Field(default_factory=lambda: ["*"], max_length=50)
    rate_limit_per_sec: float | None = Field(default=None, gt=0, le=10000)
    retry_schedule: list[float] | None = Field(default=None, max_length=12)
    timeout_s: float | None = Field(default=None, gt=0, le=60)


class EndpointPatch(BaseModel):
    url: str | None = Field(default=None, max_length=2048)
    description: str | None = None
    event_types: list[str] | None = None
    active: bool | None = None
    rate_limit_per_sec: float | None = Field(default=None, gt=0, le=10000)
    reset_circuit: bool | None = None


class ExperimentIn(BaseModel):
    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class OrmModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
