"""
Health Monitor.

Runs as a background asyncio task. Periodically polls the /health
endpoint of each registered worker and maintains a registry of
healthy vs. unhealthy workers.

In production this would trigger autoscaling events or page on-call.
For the prototype it logs status and exposes it via the REST API.
"""

import asyncio
import logging
import time
from typing import Dict

import httpx

from shared.schemas import WorkerHealth

logger = logging.getLogger(__name__)

HEALTH_CHECK_INTERVAL_SECONDS = 10
HEALTH_CHECK_TIMEOUT_SECONDS = 3

# Registry: worker_id → WorkerHealth
_worker_registry: Dict[str, WorkerHealth] = {}

# Known worker endpoints: worker_id → base URL
# In production this would be populated from Kubernetes service discovery.
_worker_endpoints: Dict[str, str] = {}


def register_worker(worker_id: str, base_url: str) -> None:
    """Registers a worker endpoint for health monitoring."""
    _worker_endpoints[worker_id] = base_url
    _worker_registry[worker_id] = WorkerHealth(
        worker_id=worker_id, status="healthy"
    )
    logger.info(f"Registered worker {worker_id} at {base_url}")


def deregister_worker(worker_id: str) -> None:
    """Removes a worker from monitoring."""
    _worker_endpoints.pop(worker_id, None)
    _worker_registry.pop(worker_id, None)
    logger.info(f"Deregistered worker {worker_id}")


def get_all_worker_health() -> list[WorkerHealth]:
    """Returns health status for all registered workers."""
    return list(_worker_registry.values())


def get_healthy_workers() -> list[str]:
    """Returns IDs of currently healthy workers."""
    return [
        wid
        for wid, health in _worker_registry.items()
        if health.status == "healthy"
    ]


async def _check_worker(worker_id: str, url: str) -> None:
    """Polls a single worker's /health endpoint and updates the registry."""
    try:
        async with httpx.AsyncClient(
            timeout=HEALTH_CHECK_TIMEOUT_SECONDS
        ) as client:
            response = await client.get(f"{url}/health")
            if response.status_code == 200:
                _worker_registry[worker_id] = WorkerHealth(
                    worker_id=worker_id,
                    status="healthy",
                    last_seen=time.time(),
                )
                logger.debug(f"Worker {worker_id} is healthy")
            else:
                _mark_unhealthy(worker_id, f"HTTP {response.status_code}")
    except httpx.RequestError as e:
        _mark_unhealthy(worker_id, str(e))


def _mark_unhealthy(worker_id: str, reason: str) -> None:
    """Marks a worker as unhealthy and logs an alert."""
    previous = _worker_registry.get(worker_id)
    if previous and previous.status == "healthy":
        logger.warning(
            f"⚠️  Worker {worker_id} is now UNHEALTHY: {reason}"
        )
    _worker_registry[worker_id] = WorkerHealth(
        worker_id=worker_id,
        status="unhealthy",
        last_seen=time.time(),
    )


async def health_monitor_loop() -> None:
    """
    Background task. Polls all registered workers every
    HEALTH_CHECK_INTERVAL_SECONDS seconds.
    """
    # Auto-register the default worker from Docker Compose
    register_worker("worker-1", "http://worker:8002")

    logger.info("Health monitor started.")
    while True:
        if _worker_endpoints:
            tasks = [
                _check_worker(wid, url)
                for wid, url in _worker_endpoints.items()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)