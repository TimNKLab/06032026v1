"""Small Polars-expression utilities shared across transform modules."""
import os

import polars as pl

LOCAL_TZ = os.environ.get('TZ', 'Asia/Jakarta')


def to_local_datetime(col_name: str) -> pl.Expr:
    """Convert UTC datetime string to local timezone (default Asia/Jakarta).

    Replaces the original implementation that reached into the Celery app
    object (``app.conf.timezone``).  Timezone is now resolved from the
    ``TZ`` environment variable at import time.
    """
    return (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False)
        .dt.replace_time_zone('UTC')
        .dt.convert_time_zone(LOCAL_TZ)
        .dt.replace_time_zone(None)
    )
