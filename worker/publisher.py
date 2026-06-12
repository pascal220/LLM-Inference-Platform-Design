"""
Token Publisher.

Publishes generated tokens to a Redis pub/sub channel so the
Inference Gateway's SSE Stream Manager can forward them to the client.

Channel naming convention: response:{request_id}
"""

import logging
import json

from shared.redis_client import get_redis
from shared.schemas import TokenEvent
from shared.metrics import tokens_generated_total

logger = logging.getLogger(__name__)

PUBSUB_CHANNEL_PREFIX = "response:"


async def publish_token(
    request_id: str,
    token: str,
    worker_id: str,
    done: bool = False,
    error: str | None = None,
) -> None:
    """
    Publishes a single token event to the Redis pub/sub channel
    for this request. The SSE Stream Manager is subscribed to this channel.
    """
    redis = await get_redis()
    channel = f"{PUBSUB_CHANNEL_PREFIX}{request_id}"

    event = TokenEvent(
        request_id=request_id,
        token=token,
        done=done,
        worker_id=worker_id,
        error=error,
    )

    await redis.publish(channel, event.model_dump_json())

    if not done and not error:
        tokens_generated_total.labels(worker_id=worker_id).inc()

    logger.debug(
        f"Published token request_id={request_id} "
        f"done={done} token='{token[:20]}'"
    )


async def publish_error(
    request_id: str, worker_id: str, error_message: str
) -> None:
    """Convenience wrapper to publish an error event."""
    await publish_token(
        request_id=request_id,
        token="",
        worker_id=worker_id,
        done=True,
        error=error_message,
    )