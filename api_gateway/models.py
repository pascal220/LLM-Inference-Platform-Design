"""
API Gateway local models.
Re-exports shared schemas and defines gateway-specific response models.
"""

from shared.schemas import ChatRequest, Message, TenantConfig

__all__ = ["ChatRequest", "Message", "TenantConfig"]


class ErrorResponse:
    def __init__(self, detail: str, status_code: int):
        self.detail = detail
        self.status_code = status_code