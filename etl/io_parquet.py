"""Atomic parquet I/O utilities."""
import os
import polars as pl

def atomic_write_parquet(df: pl.DataFrame, file_path: str):
    """Atomically write DataFrame to parquet file."""
    from etl.config import PARQUET_COMPRESSION
    temp_path = f"{file_path}.tmp"
    try:
        df.write_parquet(temp_path, compression=PARQUET_COMPRESSION)
        os.replace(temp_path, file_path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
