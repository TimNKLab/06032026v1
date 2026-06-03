"""Versioned caching for query results with automatic ETL invalidation."""
import hashlib
import json
import os
from functools import wraps
from typing import Any, Callable, Optional

from .cache import cache


def get_data_freshness() -> dict:
    """Get ETL data freshness info for display in dashboard badges.

    Reads ``admin/etl_state.json`` and returns a dict with:
      - status: 'fresh', 'stale', 'unknown', or 'error'
      - label: human-readable string (e.g. "Updated 2h ago")
      - color: DMC badge color
      - last_run: ISO timestamp or None
      - age_hours: float or None
    """
    import time as _time
    from datetime import datetime as _dt

    try:
        data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
        state_file = os.path.join(data_lake, 'admin', 'etl_state.json')
        if not os.path.exists(state_file):
            return _freshness_unknown()

        with open(state_file) as f:
            state = json.load(f)

        last_run_str = state.get('last_run_time', '')
        last_status = state.get('last_status', 'unknown')

        if not last_run_str:
            return _freshness_unknown()

        # Parse timestamp — try ISO format first, then common formats
        try:
            last_run = _dt.fromisoformat(last_run_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            try:
                last_run = _dt.strptime(last_run_str, '%Y-%m-%d %H:%M:%S')
            except (ValueError, AttributeError):
                return _freshness_unknown()

        age_seconds = _time.time() - last_run.timestamp()
        age_hours = age_seconds / 3600

        if age_hours < 1:
            age_min = int(age_seconds / 60)
            label = f"Updated {age_min}m ago"
        elif age_hours < 24:
            label = f"Updated {int(age_hours)}h ago"
        else:
            age_days = int(age_hours / 24)
            label = f"Updated {age_days}d ago"

        # Fresh: <6h, Stale: 6-24h, Unknown: >24h
        if age_hours <= 6:
            status, color = 'fresh', 'green'
        elif age_hours <= 24:
            status, color = 'stale', 'yellow'
        else:
            status, color = 'stale', 'orange'

        if last_status == 'failure':
            label = f"ETL failed · {label}"
            color = 'red'

        return {
            'status': status,
            'label': label,
            'color': color,
            'last_run': last_run_str,
            'age_hours': round(age_hours, 1),
        }

    except Exception:
        return _freshness_unknown()


def _freshness_unknown() -> dict:
    return {
        'status': 'unknown',
        'label': 'No ETL data',
        'color': 'gray',
        'last_run': None,
        'age_hours': None,
    }


def get_etl_version() -> str:
    """Get current ETL version for cache invalidation.
    
    Uses scheduler state file (admin/etl_state.json) or falls back to 
    file modification time of the latest aggregate parquet.
    """
    # Try scheduler state file first
    try:
        data_lake = os.environ.get('DATA_LAKE_ROOT', '/data-lake')
        state_file = os.path.join(data_lake, 'admin', 'etl_state.json')
        if os.path.exists(state_file):
            with open(state_file) as f:
                state = json.load(f)
            last_run = state.get('last_run_time', '')
            if last_run:
                return hashlib.md5(str(last_run).encode()).hexdigest()[:8]
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
