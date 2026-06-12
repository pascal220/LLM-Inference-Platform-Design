"""
Authentication module for the API Gateway.

Validates API keys against the Control Plane and attaches
TenantContext to each request. In production this would
validate JWTs or call a dedicated auth service.
"""

import os
import logging
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import httpx

from shared.schemas import TenantConfig
from shared.metrics import auth_failures

logger = logging.getLogger(__name__)
security = HTTPBearer()

CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://localhost:8003")

# ---------------------------------------------------------------------------
# In-memory cache: api_key → TenantConfig
# Avoids hitting Control Plane on every request.
# TTL-based invalidation is handled by periodic refresh.
# ---------------------------------------------------------------------------
_tenant_cache: dict[str, TenantConfig] = {}


async def _fetch_tenant_by_key(api_key: str) -> TenantConfig | None:
    """
    Fetches tenant configuration from the Control Plane by API key.
    Returns None if the key is not found.
    """
    if api_key in _tenant_cache:
        return _tenant_cache[api_key]

    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(
                f"{CONTROL_PLANE_URL}/tenants/by-key/{api_key}"
            )
            if response.status_code == 200:
                config = TenantConfig(**response.json())
                _tenant_cache[api_key] = config
                return config
            return None
    except httpx.RequestError as e:
        logger.error(f"Failed to reach Control Plane for auth: {e}")
        # Fail open with cache on Control Plane outage if key was seen before
        return None


async def validate_api_key(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> TenantConfig:
    """
    FastAPI dependency. Validates the Bearer token and returns TenantConfig.
    Raises HTTP 401 if the key is invalid or not found.
    """
    api_key = credentials.credentials

    tenant = await _fetch_tenant_by_key(api_key)

    if tenant is None:
        auth_failures.inc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or unknown API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    logger.info(f"Authenticated tenant={tenant.tenant_id} tier={tenant.tier}")
    return tenant


def invalidate_cache(api_key: str) -> None:
    """Removes a key from the local cache (e.g. after tenant update)."""
    _tenant_cache.pop(api_key, None)