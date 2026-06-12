"""
SSE Stream Manager.

After a request is enqueued, this module:
  1. Subscribes to a Redis pub/sub channel keyed by request_id
  2. Holds the HTTP connection open as a Server-Sent Events stream
  3. Forwards each token to the client as:  data: {"token": "..."}\n\n
  4. Sends data: [DONE]\n\n when generation is complete
  5. Enforces a hard timeout (default 30s) to prevent hanging connections
  6. Cleans up the pub/sub subscription on client disconnect or timeout

The pub/sub channel name convention: response:{request_id}
"""

import asyncio
import json
import logging

from shared.redis_client import get_redis
from shared.schemas import TokenEvent
from shared.metrics import active_sse_streams, sse_timeout_total

logger = logging.getLogger(__name__)

STREAM_TIMEOUT_SECONDS = 30
HEARTBEAT_INTERVAL_SECONDS = 5
PUBSUB_CHANNEL_PREFIX = "response:"


async def stream_response(request_id: str, tenant_id: str):
    """
    Async generator that yields SSE-formatted strings.

    Subscribes to Redis pub/sub channel `response:{request_id}`,
    yields tokens as they arrive, and cleans up on completion,
    timeout, or client disconnect.
    """
    redis = await get_redis()
    channel = f"{PUBSUB_CHANNEL_PREFIX}{request_id}"

    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)

    active_sse_streams.inc()
    logger.info(f"SSE stream opened request_id={request_id} tenant={tenant_id}")

    try:
        async for event in _listen_with_timeout(
            pubsub, request_id, tenant_id
        ):
            yield event

    except asyncio.CancelledError:
        # Client disconnected before generation completed
        logger.info(
            f"Client disconnected request_id={request_id} tenant={tenant_id}"
        )
        raise

    finally:
        active_sse_streams.dec()
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()
        logger.info(f"SSE stream closed request_id={request_id}")


async def _listen_with_timeout(pubsub, request_id: str, tenant_id: str):
    """
    Internal generator. Listens to pub/sub messages with:
      - Per-message timeout (STREAM_TIMEOUT_SECONDS)
      - Periodic heartbeat comments to keep the connection alive
      - Graceful handling of error events from the worker
    """
    heartbeat_task = None

    try:
        deadline = asyncio.get_event_loop().time() + STREAM_TIMEOUT_SECONDS

        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                logger.warning(
                    f"SSE stream timed out request_id={request_id}"
                )
                sse_timeout_total.labels(tenant_id=tenant_id).inc()
                yield 'data: {"error": "Request timed out"}\n\n'
                return

            try:
                # Wait for the next pub/sub message with a timeout
                message = await asyncio.wait_for(
                    pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    ),
                    timeout=min(remaining, HEARTBEAT_INTERVAL_SECONDS),
                )
            except asyncio.TimeoutError:
                # No message yet — send a heartbeat comment to keep connection alive
                yield ": heartbeat\n\n"
                continue

            if message is None:
                # No message in this poll cycle — send heartbeat and continue
                yield ": heartbeat\n\n"
                continue

            if message["type"] != "message":
                continue

            try:
                event = TokenEvent.model_validate_json(message["data"])
            except Exception as e:
                logger.error(
                    f"Failed to parse token event for {request_id}: {e}"
                )
                continue

            if event.error:
                yield f'data: {{"error": "{event.error}"}}\n\n'
                return

            if event.done:
                yield "data: [DONE]\n\n"
                return

            # Yield the token as an SSE event
            payload = json.dumps({"token": event.token})
            yield f"data: {payload}\n\n"

    except asyncio.CancelledError:
        raise