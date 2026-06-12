"""
Queue Consumer.

Polls Redis Streams using consumer groups (XREADGROUP).
Priority order: always drain queue:premium before queue:standard.

Key behaviours:
  - Uses XREADGROUP so multiple worker instances share the load
  - Checks TTL on each job and discards expired requests
  - ACKs messages after successful processing (or on expiry)
  - On crash/restart, uses XAUTOCLAIM to reclaim stale pending messages
"""

import os
import json
import asyncio
import logging
import time

from shared.redis_client import get_redis
from shared.schemas import InternalInferRequest

logger = logging.getLogger(__name__)

QUEUE_PREMIUM = "queue:premium"
QUEUE_STANDARD = "queue:standard"
CONSUMER_GROUP = "workers"
WORKER_ID = os.getenv("WORKER_ID", "worker-1")

# How long before a pending (unACKed) message is reclaimed
PENDING_RECLAIM_TIMEOUT_MS = 60_000  # 60 seconds
POLL_BLOCK_MS = 1000  # Block for up to 1s waiting for new messages

async def ensure_consumer_groups() -> None:
    """
    Ensures consumer groups exist on both queues.
    Called on worker startup as a safety net in case the
    Inference Gateway has not created them yet.
    """
    redis = await get_redis()
    for queue in [QUEUE_PREMIUM, QUEUE_STANDARD]:
        try:
            await redis.xgroup_create(queue, CONSUMER_GROUP, id="$", mkstream=True)
            logger.info(f"Worker created consumer group on {queue}")
        except Exception:
            logger.info(f"Consumer group already exists on {queue}")

async def reclaim_stale_messages() -> None:
    """
    On startup, reclaim any messages that were claimed by a crashed worker
    and have been pending for longer than PENDING_RECLAIM_TIMEOUT_MS.
    """
    redis = await get_redis()
    for queue in [QUEUE_PREMIUM, QUEUE_STANDARD]:
        try:
            result = await redis.xautoclaim(
                queue,
                CONSUMER_GROUP,
                WORKER_ID,
                min_idle_time=PENDING_RECLAIM_TIMEOUT_MS,
                start_id="0-0",
                count=100,
            )
            reclaimed = result[1] if result else []
            if reclaimed:
                logger.info(
                    f"Reclaimed {len(reclaimed)} stale messages from {queue}"
                )
        except Exception as e:
            logger.warning(f"Could not reclaim from {queue}: {e}")


async def poll_next_job() -> InternalInferRequest | None:
    """
    Polls for the next available job, checking premium queue first.
    Returns a deserialized InternalInferRequest or None if no jobs available.
    The caller is responsible for ACKing the message after processing.
    """
    redis = await get_redis()

    for queue in [QUEUE_PREMIUM, QUEUE_STANDARD]:
        try:
            results = await redis.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=WORKER_ID,
                streams={queue: ">"},
                count=1,
                block=POLL_BLOCK_MS,
            )

            if not results:
                continue

            stream_name, messages = results[0]
            entry_id, fields = messages[0]

            payload = fields.get("payload")
            if not payload:
                logger.error(f"Empty payload in {queue} entry {entry_id}")
                await redis.xack(queue, CONSUMER_GROUP, entry_id)
                continue

            request = InternalInferRequest.model_validate_json(payload)

            if request.is_expired():
                logger.warning(
                    f"Discarding expired request_id={request.request_id} "
                    f"tenant={request.tenant_id} "
                    f"age={time.time() - request.enqueue_timestamp:.1f}s"
                )
                await redis.xack(queue, CONSUMER_GROUP, entry_id)
                return None

            request._stream_entry_id = entry_id     # type: ignore[attr-defined]
            request._stream_queue = queue           # type: ignore[attr-defined]

            logger.info(
                f"Dequeued request_id={request.request_id} "
                f"tenant={request.tenant_id} from {queue}"
            )
            return request

        except Exception as e:
            logger.error(f"Error polling {queue}: {e}", exc_info=True)
            continue

    return None


async def ack_job(request: InternalInferRequest) -> None:
    """
    Acknowledges a job as processed, removing it from the pending list.
    Must be called after the job is fully handled (success or error).
    """
    redis = await get_redis()
    entry_id = getattr(request, "_stream_entry_id", None)
    queue = getattr(request, "_stream_queue", None)

    if entry_id and queue:
        await redis.xack(queue, CONSUMER_GROUP, entry_id)
        logger.debug(
            f"ACKed request_id={request.request_id} entry_id={entry_id}"
        )