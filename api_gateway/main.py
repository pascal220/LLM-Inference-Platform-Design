import os
import logging
import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Depends, Request
from fastapi.responses import StreamingResponse
from prometheus_client import make_asgi_app

from shared.schemas import ChatRequest, TenantConfig, InternalInferRequest
from shared.redis_client import get_redis, close_redis
from shared.metrics import gateway_requests_total

from auth import validate_api_key
from rate_limiter import check_rate_limit

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

INFERENCE_GATEWAY_URL = os.getenv(
    "INFERENCE_GATEWAY_URL", "http://localhost:8001"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API Gateway starting up...")
    yield
    logger.info("API Gateway shutting down...")
    await close_redis()


app = FastAPI(
    title="LLM Inference Platform — API Gateway",
    version="0.1.0",
    lifespan=lifespan,
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "api_gateway"}


@app.post("/v1/chat")
async def chat(
    chat_request: ChatRequest,                          # ← renamed to chat_request
    tenant: TenantConfig = Depends(validate_api_key),
):
    """
    Main chat endpoint. Accepts a list of messages and streams back
    generated tokens as Server-Sent Events (SSE).
    """
    # Run rate limit check manually so it has access to tenant
    await check_rate_limit(tenant)

    gateway_requests_total.labels(
        tenant_id=tenant.tenant_id, tier=tenant.tier
    ).inc()

    internal_request = InternalInferRequest(
        tenant_id=tenant.tenant_id,
        tier=tenant.tier,
        messages=chat_request.messages,             # ← use chat_request
        max_tokens=chat_request.max_tokens,         # ← use chat_request
        temperature=chat_request.temperature,       # ← use chat_request
    )

    logger.info(
        f"Forwarding request request_id={internal_request.request_id} "
        f"tenant={tenant.tenant_id} tier={tenant.tier}"
    )

    return StreamingResponse(
        _proxy_sse_stream(internal_request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Request-ID": internal_request.request_id,
        },
    )


async def _proxy_sse_stream(internal_request: InternalInferRequest):
    """
    Async generator that forwards the SSE stream from the Inference Gateway
    to the client.
    """
    url = f"{INFERENCE_GATEWAY_URL}/v1/infer"

    try:
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                url,
                json=internal_request.model_dump(),
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status_code != 200:
                    error_body = await response.aread()
                    logger.error(
                        f"Inference Gateway returned {response.status_code}: "
                        f"{error_body.decode()}"
                    )
                    yield f"data: {{\"error\": \"Upstream error {response.status_code}\"}}\n\n"
                    return

                async for chunk in response.aiter_text():
                    if chunk:
                        yield chunk

    except httpx.ConnectError:
        logger.error("Could not connect to Inference Gateway")
        yield 'data: {"error": "Inference service unavailable"}\n\n'

    except asyncio.CancelledError:
        logger.info(
            f"Client disconnected for request {internal_request.request_id}"
        )
        raise