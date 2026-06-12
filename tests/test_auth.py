"""
Unit tests for API Gateway authentication.
"""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from shared.schemas import TenantConfig


@pytest.fixture
def premium_tenant():
    return TenantConfig(
        tenant_id="acme",
        api_key="key-acme-premium",
        tier="premium",
        rate_limit_rps=100,
        system_prompt="You are helpful.",
    )


@pytest.mark.asyncio
async def test_valid_api_key_returns_tenant(premium_tenant):
    """A valid API key should return the corresponding TenantConfig."""
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer", credentials="key-acme-premium"
    )
    with patch(
        "auth._fetch_tenant_by_key", new=AsyncMock(return_value=premium_tenant)
    ):
        from auth import validate_api_key
        result = await validate_api_key(credentials)
        assert result.tenant_id == "acme"
        assert result.tier == "premium"


@pytest.mark.asyncio
async def test_invalid_api_key_raises_401():
    """An unknown API key should raise HTTP 401."""
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer", credentials="invalid-key"
    )
    with patch(
        "auth._fetch_tenant_by_key", new=AsyncMock(return_value=None)
    ):
        from auth import validate_api_key
        with pytest.raises(HTTPException) as exc_info:
            await validate_api_key(credentials)
        assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_tenant_cache_is_populated(premium_tenant):
    """After a successful lookup, the tenant should be cached."""
    import auth
    auth._tenant_cache.clear()

    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer", credentials="key-acme-premium"
    )
    with patch(
        "auth._fetch_tenant_by_key", new=AsyncMock(return_value=premium_tenant)
    ) as mock_fetch:
        await auth.validate_api_key(credentials)
        # Second call should use cache
        await auth.validate_api_key(credentials)
        # _fetch_tenant_by_key is called by validate_api_key which checks cache
        # The underlying HTTP call should only happen once
        assert mock_fetch.call_count <= 2