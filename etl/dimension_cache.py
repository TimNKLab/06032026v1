"""Dimension loader with lazy loading and caching for efficient joins."""
import os
import threading
from typing import Dict, Optional

import polars as pl


class DimensionLoader:
    """Lazy-load and cache dimension tables for efficient joins."""

    _cache: Dict[str, pl.LazyFrame] = {}
    _lock = threading.Lock()

    @classmethod
    def get(cls, file_path: str) -> Optional[pl.LazyFrame]:
        """Get cached LazyFrame or load from file."""
        with cls._lock:
            if file_path in cls._cache:
                return cls._cache[file_path]

            if not os.path.exists(file_path):
                return None

            lf = pl.scan_parquet(file_path)
            cls._cache[file_path] = lf
            return lf

    @classmethod
    def clear_cache(cls):
        """Clear the dimension cache."""
        with cls._lock:
            cls._cache.clear()
