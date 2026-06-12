"""
Control Plane — Entry point.

Provides:
  1. Tenant CRUD API (used by API Gateway for auth, Inference Gateway for prompts)
  2. Worker health status API
  3. Background health monitoring loop
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, status
from prometheus_client import make_asgi_app

from shared.schemas import TenantConfig
from shared.redis_client import close_redis

from tenant_store import (
    init_db,
    get_tenant,
    get_tenant_by_key,
    upsert_tenant,
    delete_tenant,
    list_tenants,
)
from health_monitor import (
    health_monitor_loop,
    get_all_worker_health,
    get_healthy_workers,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Control Plane starting up...")
    await init_db()
    monitor_task = asyncio.create_task(health_monitor_loop())
    yield
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass
    await close_redis()
    logger.info("Control Plane shut down.")


app = FastAPI(
    title="LLM Inference Platform — Control Plane",
    version="0.1.0",
    lifespan=lifespan,
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "control_plane"}


# ---------------------------------------------------------------------------
# Tenant API
# ---------------------------------------------------------------------------

@app.get("/tenants")
async def list_all_tenants() -> list[TenantConfig]:
    """Returns all registered tenants."""
    return await list_tenants()


@app.get("/tenants/{tenant_id}")
async def get_tenant_by_id(tenant_id: str) -> TenantConfig:
    """Fetches a tenant by ID. Used by Inference Gateway for system prompts."""
    tenant = await get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant '{tenant_id}' not found.",
        )
    return tenant


@app.get("/tenants/by-key/{api_key}")
async def get_tenant_by_api_key(api_key: str) -> TenantConfig:
    """Fetches a tenant by API key. Used by API Gateway for authentication."""
    tenant = await get_tenant_by_key(api_key)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found.",
        )
    return tenant


@app.post("/tenants", status_code=status.HTTP_201_CREATED)
async def create_or_update_tenant(tenant: TenantConfig) -> TenantConfig:
    """Creates or updates a tenant configuration."""
    return await upsert_tenant(tenant)


@app.delete("/tenants/{tenant_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_tenant(tenant_id: str):
    """Deletes a tenant."""
    deleted = await delete_tenant(tenant_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant '{tenant_id}' not found.",
        )


# ---------------------------------------------------------------------------
# Worker Health API
# ---------------------------------------------------------------------------

@app.get("/workers/health")
async def worker_health():
    """Returns health status for all monitored workers."""
    return {
        "workers": [w.model_dump() for w in get_all_worker_health()],
        "healthy_count": len(get_healthy_workers()),
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8003,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )