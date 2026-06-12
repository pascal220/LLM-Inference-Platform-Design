"""
Inference Gateway — Entry point.

Responsibilities:
  1. Receive enriched InternalInferRequests from the API Gateway
  2. Inject tenant-specific system prompt (via tenant_context module)
  3. Enqueue the request onto the appropriate Redis Stream (via router module)
  4. Open and manage the SSE stream back to the API Gateway (via sse_manager)
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from prometheus_client import make_asgi_app

from shared.schemas import InternalInferRequest
from shared.redis_client import get_redis, close_redis

from tenant_context import inject_tenant_context
from router import enqueue_request, get_queue_depths
from sse_manager import stream_response

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Inference Gateway starting up...")
    # Ensure Redis consumer groups exist for the worker pool
    redis = await get_redis()
    for queue in ["queue:premium", "queue:standard"]:
        try:
            await redis.xgroup_create(queue, "workers", id="0", mkstream=True)
            logger.info(f"Created consumer group 'workers' on {queue}")
        except Exception:
            # Group already exists — safe to ignore
            pass
    yield
    logger.info("Inference Gateway shutting down...")
    await close_redis()


app = FastAPI(
    title="LLM Inference Platform — Inference Gateway",
    version="0.1.0",
    lifespan=lifespan,
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health():
    depths = await get_queue_depths()
    return {
        "status": "healthy",
        "service": "inference_gateway",
        "queue_depths": depths,
    }


@app.post("/v1/infer")
async def infer(request: InternalInferRequest):
    """
    Internal endpoint called by the API Gateway.
    Not exposed to external clients directly.

    Flow:
      1. Inject tenant system prompt into messages
      2. Enqueue onto Redis Stream (raises 503 if full)
      3. Return SSE StreamingResponse backed by Redis pub/sub
    """
    # Step 1: Inject tenant context (system prompt)
    enriched_request = await inject_tenant_context(request)

    logger.info(
        f"Processing request_id={enriched_request.request_id} "
        f"tenant={enriched_request.tenant_id} "
        f"messages={len(enriched_request.messages)}"
    )

    # Step 2: Enqueue to Redis Stream (may raise HTTP 503)
    await enqueue_request(enriched_request)

    # Step 3: Return SSE stream — tokens arrive via Redis pub/sub
    return StreamingResponse(
        stream_response(
            request_id=enriched_request.request_id,
            tenant_id=enriched_request.tenant_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Request-ID": enriched_request.request_id,
        },
    )


@app.get("/queues")
async def queue_status():
    """Returns current queue depths. Useful for monitoring and autoscaling."""
    return await get_queue_depths()