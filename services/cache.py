import os

from flask_caching import Cache

cache = Cache()


def init_cache(server):
    ttl_seconds = int(os.environ.get('DASH_CACHE_TTL_SECONDS', '600'))
    redis_url = os.environ.get('REDIS_URL')

    if redis_url:
        config = {
            'CACHE_TYPE': 'RedisCache',
            'CACHE_REDIS_URL': redis_url,
            'CACHE_DEFAULT_TIMEOUT': ttl_seconds,
        }
    else:
        config = {
            'CACHE_TYPE': 'SimpleCache',
            'CACHE_DEFAULT_TIMEOUT': ttl_seconds,
        }

    cache.init_app(server, config=config)
    return cache
