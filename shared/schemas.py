"""
Cross-service Pydantic models.
These define the data contracts between all services.
"""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field
import time
import uuid


class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str


# ---------------------------------------------------------------------------
# External: Client → API Gateway
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    messages: list[Message]
    max_tokens: int = Field(default=200, ge=1, le=2048)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)


# ---------------------------------------------------------------------------
# Internal: API Gateway → Inference Gateway
# ---------------------------------------------------------------------------

class InternalInferRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    tier: Literal["premium", "standard"]
    messages: list[Message]
    max_tokens: int = 200
    temperature: float = 0.7
    enqueue_timestamp: float = Field(default_factory=time.time)
    ttl_seconds: int = 30

    def is_expired(self) -> bool:
        return (time.time() - self.enqueue_timestamp) > self.ttl_seconds

    def build_prompt(self) -> str:
        """
        Converts messages list into a single prompt string.
        In production this would use a proper chat template.
        """
        parts = []
        for msg in self.messages:
            if msg.role == "system":
                parts.append(f"[SYSTEM]: {msg.content}")
            elif msg.role == "user":
                parts.append(f"[USER]: {msg.content}")
            elif msg.role == "assistant":
                parts.append(f"[ASSISTANT]: {msg.content}")
        parts.append("[ASSISTANT]:")
        return "\n".join(parts)


# ---------------------------------------------------------------------------
# Internal: Worker → Redis pub/sub (per token event)
# ---------------------------------------------------------------------------

class TokenEvent(BaseModel):
    request_id: str
    token: str
    done: bool
    worker_id: str = "unknown"
    error: str | None = None


# ---------------------------------------------------------------------------
# Tenant configuration (used by Control Plane and cached by other services)
# ---------------------------------------------------------------------------

class TenantConfig(BaseModel):
    tenant_id: str
    api_key: str
    tier: Literal["premium", "standard"]
    rate_limit_rps: int = Field(default=10, ge=1)
    system_prompt: str = ""


# ---------------------------------------------------------------------------
# Worker health report
# ---------------------------------------------------------------------------

class WorkerHealth(BaseModel):
    worker_id: str
    status: Literal["healthy", "unhealthy"]
    queue_depth: int = 0
    last_seen: float = Field(default_factory=time.time)