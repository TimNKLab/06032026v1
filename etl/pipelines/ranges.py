"""Date range pipeline implementations."""
from datetime import datetime, timedelta
from typing import Dict, Any

import logging

logger = logging.getLogger(__name__)


def date_range_etl_pipeline_impl(start_date: str, end_date: str) -> Dict[str, Any]:
    """Process date range in parallel implementation."""
    from etl_tasks import app, group
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    logger.info(f"Starting parallel ETL for {start_date} to {end_date}")

    delta = end_dt - start_dt
    date_range = [
        (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(delta.days + 1)
    ]

    # Import here to avoid circular imports
    from etl.pipelines.daily import daily_etl_pipeline_impl
    
    job = group(daily_etl_pipeline_impl.si(date_str) for date_str in date_range)
    result = job.apply_async()

    return {
        "status": "queued",
        "start_date": start_date,
        "end_date": end_date,
        "total_days": len(date_range),
        "group_id": result.id,
        "message": f"Parallel ETL for {len(date_range)} days"
    }
