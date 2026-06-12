"""
Unit tests for the sliding window rate limiter.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException

from shared.schemas import TenantConfig


@pytest.fixture
def standard_tenant():
    return TenantConfig(
        tenant_id="beta-corp",
        api_key="key-beta-standard",
        tier="standard",
        rate_limit_rps=5,
        system_prompt="",
    )


@pytest.mark.asyncio
async def test_request_under_limit_passes(standard_tenant):
    """A request well under the rate limit should pass without error."""
    mock_redis = AsyncMock()
    # Simulate: curr_count=1, prev_count=0 → rate=1, limit=5
    mock_pipeline = AsyncMock()
    mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
    mock_pipeline.__aexit__ = AsyncMock(return_value=False)
    mock_pipeline.execute = AsyncMock(return_value=[1, True, None])
    mock_redis.pipeline = MagicMock(return_value=mock_pipeline)

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        # Should not raise
        await check_rate_limit(standard_tenant)


@pytest.mark.asyncio
async def test_request_over_limit_raises_429(standard_tenant):
    """A request exceeding the rate limit should raise HTTP 429."""
    mock_redis = AsyncMock()
    # Simulate: curr_count=10, prev_count=10 → rate >> 5
    mock_pipeline = AsyncMock()
    mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
    mock_pipeline.__aexit__ = AsyncMock(return_value=False)
    mock_pipeline.execute = AsyncMock(return_value=[10, True, "10"])
    mock_redis.pipeline = MagicMock(return_value=mock_pipeline)

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(standard_tenant)
        assert exc_info.value.status_code == 429
        assert "Retry-After" in exc_info.value.headers


@pytest.mark.asyncio
async def test_retry_after_header_is_present(standard_tenant):
    """The 429 response must include a Retry-After header."""
    mock_redis = AsyncMock()
    mock_pipeline = AsyncMock()
    mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
    mock_pipeline.__aexit__ = AsyncMock(return_value=False)
    mock_pipeline.execute = AsyncMock(return_value=[100, True, "100"])
    mock_redis.pipeline = MagicMock(return_value=mock_pipeline)

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(standard_tenant)
        assert "Retry-After" in exc_info.value.headers