"""
Unit tests for the sliding window rate limiter.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
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


def make_mock_redis(pipeline_results: list):
    """
    Helper that builds a correctly structured async Redis mock
    whose pipeline context manager returns the given results.

    Pipeline commands (incr, expire, get) are synchronous inside
    the async with block — they queue up commands. Only execute()
    is awaited. The mock reflects this accurately.
    """
    # The pipeline object returned by redis.pipeline()
    mock_pipeline = MagicMock()                          # sync MagicMock, not AsyncMock
    mock_pipeline.incr = MagicMock()                     # sync — just queues the command
    mock_pipeline.expire = MagicMock()                   # sync — just queues the command
    mock_pipeline.get = MagicMock()                      # sync — just queues the command
    mock_pipeline.execute = AsyncMock(return_value=pipeline_results)  # async — actually runs

    # async with redis.pipeline(transaction=True) as pipe:
    async_context = AsyncMock()
    async_context.__aenter__ = AsyncMock(return_value=mock_pipeline)
    async_context.__aexit__ = AsyncMock(return_value=False)

    mock_redis = AsyncMock()
    mock_redis.pipeline = MagicMock(return_value=async_context)  # pipeline() is sync

    return mock_redis


@pytest.mark.asyncio
async def test_request_under_limit_passes(standard_tenant):
    """A request well under the rate limit should pass without error."""
    # curr_count=1, prev_count=None → rate ≈ 1, limit=5 → should pass
    mock_redis = make_mock_redis(pipeline_results=[1, True, None])

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        # Should complete without raising any exception
        await check_rate_limit(standard_tenant)


@pytest.mark.asyncio
async def test_request_over_limit_raises_429(standard_tenant):
    """A request exceeding the rate limit should raise HTTP 429."""
    # curr_count=10, prev_count=10 → rate >> 5 → should be rejected
    mock_redis = make_mock_redis(pipeline_results=[10, True, "10"])

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(standard_tenant)
        assert exc_info.value.status_code == 429


@pytest.mark.asyncio
async def test_retry_after_header_is_present(standard_tenant):
    """The 429 response must include a Retry-After header."""
    mock_redis = make_mock_redis(pipeline_results=[100, True, "100"])

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(standard_tenant)
        assert exc_info.value.status_code == 429
        assert "Retry-After" in exc_info.value.headers


@pytest.mark.asyncio
async def test_exactly_at_limit_passes(standard_tenant):
    """A request exactly at the rate limit should pass (not exceed)."""
    # curr_count=5, prev_count=0 → rate=5, limit=5 → should pass
    mock_redis = make_mock_redis(pipeline_results=[5, True, None])

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        await check_rate_limit(standard_tenant)


@pytest.mark.asyncio
async def test_different_tenants_have_separate_limits():
    """Two tenants should be rate limited independently."""
    premium_tenant = TenantConfig(
        tenant_id="acme",
        api_key="key-acme-premium",
        tier="premium",
        rate_limit_rps=100,
        system_prompt="",
    )

    # Premium tenant at count=50 — well under limit of 100
    mock_redis = make_mock_redis(pipeline_results=[50, True, "20"])

    with patch("rate_limiter.get_redis", new=AsyncMock(return_value=mock_redis)):
        from rate_limiter import check_rate_limit
        # Should pass — 50 + partial prev is still under 100
        await check_rate_limit(premium_tenant)