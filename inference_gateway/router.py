"""
Request Router.

Serialises enriched InternalInferRequests and pushes them onto
the appropriate Redis Stream based on tenant tier.

Queue names:
    queue:premium  — polled first by workers
    queue:standard — polled when premium queue is empty

Uses XADD with MAXLEN to enforce backpressure. If the queue is full,
the request is rejected immediately with a 503 rather than hanging.
"""

import json
import logging

from fastapi import HTTPException, status

from shared.redis_client import get_redis
from shared.schemas import InternalInferRequest
from shared.metrics import queue_enqueue_total, queue_depth

logger = logging.getLogger(__name__)

QUEUE_PREMIUM = "queue:premium"
QUEUE_STANDARD = "queue:standard"
MAX_QUEUE_LENGTH = 1000  # Hard cap per queue — tune based on worker capacity


def _queue_name(tier: str) -> str:
    return QUEUE_PREMIUM if tier == "premium" else QUEUE_STANDARD


async def enqueue_request(request: InternalInferRequest) -> str:
    """
    Pushes the request onto the appropriate Redis Stream.
    Returns the Redis stream entry ID on success.
    Raises HTTP 503 if the queue is at capacity.
    """
    redis = await get_redis()
    queue = _queue_name(request.tier)

    # Check current queue length before enqueuing
    current_length = await redis.xlen(queue)
    if current_length >= MAX_QUEUE_LENGTH:
        logger.warning(
            f"Queue {queue} is full ({current_length} items). "
            f"Rejecting request {request.request_id}."
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server is at capacity. Please retry shortly.",
            headers={"Retry-After": "5"},
        )

    # Serialise the full request as a single field in the stream entry
    entry_id = await redis.xadd(
        queue,
        {"payload": request.model_dump_json()},
        maxlen=MAX_QUEUE_LENGTH,
        approximate=True,  # MAXLEN ~ (approximate trim, more efficient)
    )

    queue_enqueue_total.labels(tier=request.tier).inc()
    queue_depth.labels(tier=request.tier).set(current_length + 1)

    logger.info(
        f"Enqueued request_id={request.request_id} "
        f"tenant={request.tenant_id} tier={request.tier} "
        f"queue={queue} entry_id={entry_id}"
    )

    return entry_id


async def get_queue_depths() -> dict[str, int]:
    """Returns current depth of both queues. Used by health/metrics endpoints."""
    redis = await get_redis()
    premium_depth = await redis.xlen(QUEUE_PREMIUM)
    standard_depth = await redis.xlen(QUEUE_STANDARD)
    return {"premium": premium_depth, "standard": standard_depth}