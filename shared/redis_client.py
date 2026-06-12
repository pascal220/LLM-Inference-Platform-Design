"""
Shared Redis connection factory.
All services import get_redis() to obtain a connection pool instance.
"""

import os
import redis.asyncio as aioredis
from redis.asyncio import Redis

_redis_instance: Redis | None = None


async def get_redis() -> Redis:
    """
    Returns a shared async Redis client instance.
    Creates the connection pool on first call (singleton pattern).
    """
    global _redis_instance
    if _redis_instance is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        _redis_instance = aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50,
        )
    return _redis_instance


async def close_redis() -> None:
    """Gracefully close the Redis connection pool on shutdown."""
    global _redis_instance
    if _redis_instance is not None:
        await _redis_instance.aclose()
        _redis_instance = None