"""
Unit tests for the SSE Stream Manager.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from shared.schemas import TokenEvent


def make_pubsub_message(event: TokenEvent) -> dict:
    """Helper: wraps a TokenEvent as a Redis pub/sub message dict."""
    return {
        "type": "message",
        "channel": f"response:{event.request_id}",
        "data": event.model_dump_json(),
    }


@pytest.mark.asyncio
async def test_tokens_arrive_in_order():
    """Tokens should be yielded in the order they are published."""
    request_id = "test-req-001"
    tokens = ["Hello", " world", " from", " the", " model"]

    events = [
        make_pubsub_message(
            TokenEvent(
                request_id=request_id,
                token=t,
                done=False,
                worker_id="worker-1",
            )
        )
        for t in tokens
    ]
    events.append(
        make_pubsub_message(
            TokenEvent(
                request_id=request_id,
                token="",
                done=True,
                worker_id="worker-1",
            )
        )
    )

    call_count = 0

    async def mock_get_message(**kwargs):
        nonlocal call_count
        if call_count < len(events):
            msg = events[call_count]
            call_count += 1
            return msg
        await asyncio.sleep(0.01)
        return None

    mock_pubsub = AsyncMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()
    mock_pubsub.aclose = AsyncMock()
    mock_pubsub.get_message = mock_get_message

    mock_redis = AsyncMock()
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

    collected_tokens = []

    with patch("sse_manager.get_redis", new=AsyncMock(return_value=mock_redis)):
        from sse_manager import stream_response
        async for chunk in stream_response(request_id, "acme"):
            if chunk.startswith("data:") and "[DONE]" not in chunk:
                data = json.loads(chunk.replace("data: ", "").strip())
                collected_tokens.append(data["token"])
            elif "[DONE]" in chunk:
                break

    assert collected_tokens == tokens


@pytest.mark.asyncio
async def test_done_event_closes_stream():
    """A done=True event should terminate the SSE stream."""
    request_id = "test-req-002"
    events = [
        make_pubsub_message(
            TokenEvent(
                request_id=request_id,
                token="Final",
                done=False,
                worker_id="worker-1",
            )
        ),
        make_pubsub_message(
            TokenEvent(
                request_id=request_id,
                token="",
                done=True,
                worker_id="worker-1",
            )
        ),
    ]

    call_count = 0

    async def mock_get_message(**kwargs):
        nonlocal call_count
        if call_count < len(events):
            msg = events[call_count]
            call_count += 1
            return msg
        return None

    mock_pubsub = AsyncMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()
    mock_pubsub.aclose = AsyncMock()
    mock_pubsub.get_message = mock_get_message

    mock_redis = AsyncMock()
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

    chunks = []
    with patch("sse_manager.get_redis", new=AsyncMock(return_value=mock_redis)):
        from sse_manager import stream_response
        async for chunk in stream_response(request_id, "acme"):
            chunks.append(chunk)
            if "[DONE]" in chunk:
                break

    assert any("[DONE]" in c for c in chunks)