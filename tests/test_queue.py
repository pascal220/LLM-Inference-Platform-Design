"""
Unit tests for the Inference Gateway queue router.
"""

import pytest
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException

from shared.schemas import InternalInferRequest, Message


@pytest.fixture
def premium_request():
    return InternalInferRequest(
        tenant_id="acme",
        tier="premium",
        messages=[Message(role="user", content="Hello")],
        max_tokens=100,
    )


@pytest.fixture
def standard_request():
    return InternalInferRequest(
        tenant_id="beta-corp",
        tier="standard",
        messages=[Message(role="user", content="Hi there")],
        max_tokens=50,
    )


@pytest.mark.asyncio
async def test_premium_request_enqueued_to_premium_queue(premium_request):
    """Premium tier requests must go to queue:premium."""
    mock_redis = AsyncMock()
    mock_redis.xlen = AsyncMock(return_value=0)
    mock_redis.xadd = AsyncMock(return_value="1234-0")

    with patch("router.get_redis", new=AsyncMock(return_value=mock_redis)):
        from router import enqueue_request
        await enqueue_request(premium_request)
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "queue:premium"


@pytest.mark.asyncio
async def test_standard_request_enqueued_to_standard_queue(standard_request):
    """Standard tier requests must go to queue:standard."""
    mock_redis = AsyncMock()
    mock_redis.xlen = AsyncMock(return_value=0)
    mock_redis.xadd = AsyncMock(return_value="1234-0")

    with patch("router.get_redis", new=AsyncMock(return_value=mock_redis)):
        from router import enqueue_request
        await enqueue_request(standard_request)
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "queue:standard"


@pytest.mark.asyncio
async def test_full_queue_raises_503(premium_request):
    """When the queue is at max capacity, a 503 should be raised."""
    mock_redis = AsyncMock()
    mock_redis.xlen = AsyncMock(return_value=1000)  # At max capacity

    with patch("router.get_redis", new=AsyncMock(return_value=mock_redis)):
        from router import enqueue_request
        with pytest.raises(HTTPException) as exc_info:
            await enqueue_request(premium_request)
        assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_expired_request_is_discarded():
    """A request past its TTL should be discarded by the queue consumer."""
    import time
    expired_request = InternalInferRequest(
        tenant_id="acme",
        tier="premium",
        messages=[Message(role="user", content="Old request")],
        enqueue_timestamp=time.time() - 60,  # 60 seconds ago
        ttl_seconds=30,
    )
    assert expired_request.is_expired() is True