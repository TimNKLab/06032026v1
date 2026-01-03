"""ETL metadata management for tracking processed dates and dimension updates."""
import json
import os
from datetime import date, datetime
from typing import Optional

from etl.config import METADATA_PATH


class ETLMetadata:
    """Manage ETL metadata for tracking processed dates and dimension updates."""

    @staticmethod
    def get_last_processed_date() -> Optional[date]:
        """Get last successfully processed date from metadata."""
        try:
            metadata_file = f'{METADATA_PATH}/etl_status.json'
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    data = json.load(f)
                    if 'last_processed_date' in data:
                        return date.fromisoformat(data['last_processed_date'])
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error reading metadata: {e}")
        return None

    @staticmethod
    def set_last_processed_date(process_date: date):
        """Update last processed date in metadata."""
        try:
            metadata_file = f'{METADATA_PATH}/etl_status.json'
            data = {}
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    data = json.load(f)

            data['last_processed_date'] = process_date.isoformat()
            data['last_updated'] = datetime.now().isoformat()

            temp_file = f'{metadata_file}.tmp'
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_file, metadata_file)

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error writing metadata: {e}")

    @staticmethod
    def get_dimension_last_sync(dimension: str) -> Optional[datetime]:
        """Get last sync time for a dimension."""
        try:
            metadata_file = f'{METADATA_PATH}/dimension_sync.json'
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    data = json.load(f)
                    if dimension in data:
                        return datetime.fromisoformat(data[dimension])
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error reading dimension sync metadata: {e}")
        return None

    @staticmethod
    def set_dimension_last_sync(dimension: str, sync_time: datetime):
        """Update last sync time for a dimension."""
        try:
            metadata_file = f'{METADATA_PATH}/dimension_sync.json'
            data = {}
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    data = json.load(f)

            data[dimension] = sync_time.isoformat()

            temp_file = f'{metadata_file}.tmp'
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_file, metadata_file)

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error writing dimension sync metadata: {e}")
