"""Health check and catch-up pipeline implementations."""
from datetime import date, timedelta
from typing import Dict, Any

import logging

logger = logging.getLogger(__name__)


def catch_up_etl_impl() -> Dict[str, Any]:
    """Auto-catch up missed dates implementation."""
    from etl.metadata import ETLMetadata
    
    last_processed = ETLMetadata.get_last_processed_date()
    today = date.today()

    if not last_processed:
        logger.warning("No last processed date found")
        return {"status": "no_baseline"}

    if last_processed >= today:
        logger.info("ETL is up to date")
        return {"status": "up_to_date"}

    delta = today - last_processed
    if delta.days > 1:
        logger.info(f"Catching up {delta.days - 1} days")

        start_date = (last_processed + timedelta(days=1)).isoformat()
        end_date = (today - timedelta(days=1)).isoformat()

        from etl.pipelines.ranges import date_range_etl_pipeline_impl
        return date_range_etl_pipeline_impl(start_date, end_date)

    return {"status": "up_to_date"}


def health_check_impl() -> Dict[str, Any]:
    """Health check with auto-recovery implementation."""
    from etl.metadata import ETLMetadata
    
    try:
        last_processed = ETLMetadata.get_last_processed_date()
        today = date.today()

        if not last_processed:
            return {"status": "unknown", "message": "No metadata found"}

        days_behind = (today - last_processed).days

        if days_behind <= 1:
            return {"status": "healthy", "last_processed": last_processed.isoformat()}

        logger.warning(f"ETL is {days_behind} days behind, triggering catch-up")
        
        # Trigger catch-up asynchronously
        from etl_tasks import app
        catch_up_etl_impl.delay()

        return {
            "status": "unhealthy",
            "days_behind": days_behind,
            "action": "triggered_catch_up"
        }

    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "error", "error": str(e)}
