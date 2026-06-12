import time
import logging
from fastapi import HTTPException, status

from shared.redis_client import get_redis
from shared.schemas import TenantConfig
from shared.metrics import rate_limit_hits

logger = logging.getLogger(__name__)

WINDOW_SECONDS = 1


async def check_rate_limit(tenant: TenantConfig) -> None:   # ← plain argument, not Depends
    """
    Checks the sliding window rate limit for the given tenant.
    Raises HTTP 429 if the tenant has exceeded their rate limit.
    """
    redis = await get_redis()
    now = time.time()
    window = WINDOW_SECONDS

    current_window = int(now // window)
    previous_window = current_window - 1

    curr_key = f"rl:{tenant.tenant_id}:{current_window}"
    prev_key = f"rl:{tenant.tenant_id}:{previous_window}"

    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(curr_key)
        pipe.expire(curr_key, window * 2)
        pipe.get(prev_key)
        results = await pipe.execute()

    curr_count = int(results[0])
    prev_count = int(results[2]) if results[2] else 0

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