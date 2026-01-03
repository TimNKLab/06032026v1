"""Redis caching utilities for ETL operations."""
import json
from typing import Any, Optional

from etl.config import CACHE_TTL


def get_redis_client():
    """Get Redis client from Celery backend."""
    # Import here to avoid circular import
    from etl_tasks import app
    return app.backend.client


def cache_get(key: str) -> Optional[Any]:
    """Get value from Redis cache."""
    try:
        redis = get_redis_client()
        value = redis.get(f'etl:cache:{key}')
        return json.loads(value) if value else None
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Cache get error for {key}: {e}")
        return None


def cache_set(key: str, value: Any, ttl: int = CACHE_TTL):
    """Set value in Redis cache with TTL."""
    try:
        redis = get_redis_client()
        redis.setex(f'etl:cache:{key}', ttl, json.dumps(value))
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Cache set error for {key}: {e}")


def cache_delete(key: str):
    """Delete key from Redis cache."""
    try:
        redis = get_redis_client()
        redis.delete(f'etl:cache:{key}')
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Cache delete error for {key}: {e}")
