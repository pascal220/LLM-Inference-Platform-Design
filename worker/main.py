"""
Worker — Entry point.

Runs two concurrent tasks:
  1. The inference loop: continuously polls the queue and processes jobs
  2. A lightweight HTTP server: exposes /health and /metrics endpoints

The inference loop is the core of the worker. It:
  - Polls Redis Streams for jobs (priority: premium > standard)
  - Runs inference via the configured engine (mock or vLLM)
  - Publishes tokens to Redis pub/sub as they are generated
  - ACKs the job on completion
"""

import os
import asyncio
import logging
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from prometheus_client import make_asgi_app

from shared.redis_client import get_redis, close_redis
from shared.metrics import (
    jobs_processed_total,
    job_processing_time,
    queue_depth,
)

from engine import create_engine
from queue_consumer import poll_next_job, ack_job, reclaim_stale_messages
from publisher import publish_token, publish_error

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

WORKER_ID = os.getenv("WORKER_ID", "worker-1")

# Initialise the inference engine (mock or vLLM)
engine = create_engine()


async def inference_loop() -> None:
    """
    Main worker loop. Runs indefinitely, processing one job at a time.

    In production, multiple concurrent jobs can be processed using
    asyncio.gather() or a semaphore-bounded task pool. For the prototype,
    we process sequentially to keep the logic clear.
    """
    logger.info(f"Worker {WORKER_ID} inference loop started.")

    # Reclaim any messages left pending by a previously crashed worker
    await reclaim_stale_messages()

    while True:
        request = await poll_next_job()

        if request is None:
            # No jobs available — brief sleep before next poll
            await asyncio.sleep(0.1)
            continue

        start_time = time.time()
        logger.info(
            f"[{WORKER_ID}] Starting inference for "
            f"request_id={request.request_id} "
            f"tenant={request.tenant_id}"
        )

        try:
            prompt = request.build_prompt()
            token_count = 0

            async for token in engine.generate(
                prompt=prompt,
                request_id=request.request_id,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
            ):
                await publish_token(
                    request_id=request.request_id,
                    token=token,
                    worker_id=WORKER_ID,
                    done=False,
                )
                token_count += 1

            # Signal completion
            await publish_token(
                request_id=request.request_id,
                token="",
                worker_id=WORKER_ID,
                done=True,
            )

            elapsed = time.time() - start_time
            logger.info(
                f"[{WORKER_ID}] Completed request_id={request.request_id} "
                f"tokens={token_count} elapsed={elapsed:.2f}s"
            )

            jobs_processed_total.labels(
                worker_id=WORKER_ID, status="success"
            ).inc()
            job_processing_time.labels(worker_id=WORKER_ID).observe(elapsed)

        except Exception as e:
            logger.error(
                f"[{WORKER_ID}] Inference failed for "
                f"request_id={request.request_id}: {e}",
                exc_info=True,
            )
            await publish_error(
                request_id=request.request_id,
                worker_id=WORKER_ID,
                error_message=str(e),
            )
            jobs_processed_total.labels(
                worker_id=WORKER_ID, status="error"
            ).inc()

        finally:
            # Always ACK the job, even on failure, to prevent reprocessing
            await ack_job(request)


# ---------------------------------------------------------------------------
# Lightweight HTTP server for health checks and Prometheus metrics
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Worker {WORKER_ID} HTTP server starting...")
    # Start the inference loop as a background task
    loop_task = asyncio.create_task(inference_loop())
    yield
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass
    await close_redis()
    logger.info(f"Worker {WORKER_ID} shut down.")


app = FastAPI(
    title=f"LLM Worker — {WORKER_ID}",
    version="0.1.0",
    lifespan=lifespan,
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health():
    """Health check endpoint polled by the Control Plane."""
    return {
        "status": "healthy",
        "worker_id": WORKER_ID,
        "engine": "mock" if os.getenv("USE_MOCK", "true").lower() == "true" else "vllm",
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )