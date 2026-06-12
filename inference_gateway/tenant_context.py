"""
Tenant Context Injector.

Fetches the tenant's system prompt from the Control Plane and prepends
it to the messages list. Results are cached locally with a TTL to avoid
hitting the Control Plane on every request.
"""

import os
import time
import logging
import httpx

from shared.schemas import InternalInferRequest, Message

logger = logging.getLogger(__name__)

CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://localhost:8003")
CACHE_TTL_SECONDS = 60  # Refresh tenant config every 60 seconds

# Cache structure: tenant_id → {"system_prompt": str, "cached_at": float}
_prompt_cache: dict[str, dict] = {}


async def inject_tenant_context(
    request: InternalInferRequest,
) -> InternalInferRequest:
    """
    Fetches the tenant's system prompt and prepends it to the messages list.
    Returns a new InternalInferRequest with the injected context.
    If no system prompt is configured, the request is returned unchanged.
    """
    system_prompt = await _get_system_prompt(request.tenant_id)

    if not system_prompt:
        return request

    # Check if a system message already exists (e.g., passed by the client)
    has_system = any(m.role == "system" for m in request.messages)

    if has_system:
        # Replace the existing system message with the tenant's configured one
        messages = [
            Message(role="system", content=system_prompt)
            if m.role == "system"
            else m
            for m in request.messages
        ]
    else:
        # Prepend a new system message
        messages = [
            Message(role="system", content=system_prompt),
            *request.messages,
        ]

    logger.debug(
        f"Injected system prompt for tenant={request.tenant_id} "
        f"({len(system_prompt)} chars)"
    )

    return request.model_copy(update={"messages": messages})


async def _get_system_prompt(tenant_id: str) -> str:
    """
    Returns the system prompt for a tenant, using a local TTL cache.
    Falls back to empty string on Control Plane errors.
    """
    cached = _prompt_cache.get(tenant_id)
    if cached and (time.time() - cached["cached_at"]) < CACHE_TTL_SECONDS:
        return cached["system_prompt"]

    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(
                f"{CONTROL_PLANE_URL}/tenants/{tenant_id}"
            )
            if response.status_code == 200:
                data = response.json()
                prompt = data.get("system_prompt", "")
                _prompt_cache[tenant_id] = {
                    "system_prompt": prompt,
                    "cached_at": time.time(),
                }
                return prompt
    except httpx.RequestError as e:
        logger.warning(
            f"Could not fetch tenant config for {tenant_id}: {e}. "
            f"Using cached value."
        )
        if cached:
            return cached["system_prompt"]

    return ""


def invalidate_tenant_cache(tenant_id: str) -> None:
    """Removes a tenant from the local prompt cache."""
    _prompt_cache.pop(tenant_id, None)