"""
Sliding window rate limiter backed by Redis.

Uses two counters per tenant (current window + previous window) to
implement a smooth sliding window algorithm without Lua scripts.

Rate formula:
    rate = prev_count * ((window - elapsed) / window) + curr_count

If rate exceeds the tenant's limit, the request is rejected with HTTP 429.
"""

import time
import logging
from fastapi import HTTPException, status

from shared.redis_client import get_redis
from shared.schemas import TenantConfig
from shared.metrics import rate_limit_hits

logger = logging.getLogger(__name__)

WINDOW_SECONDS = 1  # 1-second sliding window


async def check_rate_limit(tenant: TenantConfig) -> None:
    """
    FastAPI dependency (called after auth).
    Raises HTTP 429 if the tenant has exceeded their rate limit.
    """
    redis = await get_redis()
    now = time.time()
    window = WINDOW_SECONDS

    # Keys for current and previous time windows
    current_window = int(now // window)
    previous_window = current_window - 1

    curr_key = f"rl:{tenant.tenant_id}:{current_window}"
    prev_key = f"rl:{tenant.tenant_id}:{previous_window}"

    # Atomic pipeline: increment current window counter
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(curr_key)
        pipe.expire(curr_key, window * 2)  # keep for 2 windows
        pipe.get(prev_key)
        results = await pipe.execute()

    curr_count = int(results[0])
    prev_count = int(results[2]) if results[2] else 0

    # Sliding window rate estimate
    elapsed_in_window = now % window
    rate = prev_count * ((window - elapsed_in_window) / window) + curr_count

    logger.debug(
        f"Rate check tenant={tenant.tenant_id} "
        f"rate={rate:.2f} limit={tenant.rate_limit_rps}"
    )

    if rate > tenant.rate_limit_rps:
        rate_limit_hits.labels(tenant_id=tenant.tenant_id).inc()
        retry_after = int(window - elapsed_in_window) + 1
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Limit: {tenant.rate_limit_rps} req/s.",
            headers={"Retry-After": str(retry_after)},
        )