"""Versioned caching for query results with automatic ETL invalidation."""
import hashlib
import json
import os
from functools import wraps
from typing import Any, Callable, Optional

from .cache import cache


def get_etl_version() -> str:
    """Get current ETL version for cache invalidation.
    
    Uses max(refresh_date) from DuckDB metadata, or falls back to file-based version.
    """
    # Try DuckDB metadata first (lazy import to avoid circular dependency)
    try:
        from .duckdb_connector import DuckDBManager
        conn = DuckDBManager().get_connection()
        result = conn.execute("""
            SELECT COALESCE(MAX(last_refresh_date), 'v1') 
            FROM mv_refresh_metadata
        """).fetchone()
        if result and result[0]:
            # Hash the timestamp to create a short version string
            return hashlib.md5(str(result[0]).encode()).hexdigest()[:8]
    except Exception:
        pass
    
    # Fallback: use file modification time of latest parquet
    try:
        data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
        import glob
        files = glob.glob(f"{data_lake}/star-schema/agg_sales_daily/**/*.parquet", recursive=True)
        if files:
            latest = max(files, key=os.path.getmtime)
            mtime = os.path.getmtime(latest)
            return hashlib.md5(str(mtime).encode()).hexdigest()[:8]
    except Exception:
        pass
    
    return "v1"


def build_versioned_key(base_key: str, *args, **kwargs) -> str:
    """Build a cache key that includes ETL version for automatic invalidation."""
    version = get_etl_version()
    
    # Normalize args/kwargs into a stable string
    key_parts = [base_key, version]
    
    if args:
        key_parts.append(str(args))
    if kwargs:
        # Sort kwargs for consistency
        key_parts.append(str(sorted(kwargs.items())))
    
    # Create deterministic key
    raw_key = ":".join(key_parts)
    return f"v1:{hashlib.md5(raw_key.encode()).hexdigest()[:16]}"


def versioned_cache(ttl: int = 3600, key_prefix: str = ""):
    """Decorator for versioned caching with automatic ETL invalidation.
    
    Args:
        ttl: Cache time-to-live in seconds
        key_prefix: Prefix for cache key (e.g., 'sales_trends')
    
    Example:
        @versioned_cache(ttl=3600, key_prefix="revenue_comparison")
        def query_revenue_comparison(start_date, end_date):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Build versioned cache key
            cache_key = build_versioned_key(
                key_prefix or func.__name__,
                *args,
                **kwargs
            )
            
            # Try to get from cache
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            cache.set(cache_key, result, timeout=ttl)
            
            return result
        
        return wrapper
    return decorator


def invalidate_cache_by_pattern(pattern: str) -> int:
    """Invalidate cache entries matching a pattern.
    
    Returns number of keys invalidated.
    """
    # Note: Redis supports pattern deletion, SimpleCache doesn't
    try:
        if hasattr(cache, '_cache'):
            # SimpleCache - iterate and delete matching keys
            keys_to_delete = [
                k for k in cache._cache.keys() 
                if pattern in k
            ]
            for k in keys_to_delete:
                cache.delete(k)
            return len(keys_to_delete)
        elif hasattr(cache, 'delete_many'):
            # Redis - use scan and delete
            # This is a simplified version
            return 0
    except Exception:
        pass
    return 0
