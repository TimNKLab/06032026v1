"""Odoo RPC helper utilities."""
from typing import Any, Dict, Iterator, List, Optional, Set

from etl.config import ODOO_BATCH_SIZE


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def batch_ids(ids: Set[int], batch_size: int = ODOO_BATCH_SIZE) -> Iterator[List[int]]:
    """Yield batches of IDs without allocating all at once."""
    id_list = sorted(ids)
    for i in range(0, len(id_list), batch_size):
        yield id_list[i:i + batch_size]


def safe_extract_m2o(value: Any, get_id: bool = True) -> Optional[Any]:
    """Safely extract Many2One field value."""
    if isinstance(value, (list, tuple)) and value:
        return value[0] if get_id else (value[1] if len(value) >= 2 else None)
    elif isinstance(value, (int, str)):
        return value
    return None


def format_m2o(value: Any) -> Dict[str, Optional[Any]]:
    """Format Many2One field to dict with id and name."""
    if isinstance(value, (list, tuple)) and value:
        return {"id": value[0], "name": value[1] if len(value) >= 2 else None}
    if isinstance(value, dict):
        return {"id": value.get("id"), "name": value.get("name")}
    if isinstance(value, int):
        return {"id": value, "name": None}
    return {"id": None, "name": None}


def format_m2m(value: Any) -> List[Dict[str, Optional[Any]]]:
    """Format Many2Many field to list of {id, name}."""
    if isinstance(value, (list, tuple)) and value:
        if all(isinstance(x, (list, tuple)) and len(x) >= 2 for x in value):
            return [
                {"id": x[0], "name": x[1] if len(x) > 1 else None}
                for x in value
                if x and x[0] is not None
            ]
    if isinstance(value, list) and value and isinstance(value[0], int):
        return [{"id": item, "name": None} for item in value if item is not None]
    return []


def extract_o2m_ids(value: Any) -> List[int]:
    """Extract One2many/Many2many IDs returned by Odoo RPC."""
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        ids: List[int] = []
        for item in value:
            if isinstance(item, int):
                ids.append(item)
            elif isinstance(item, (list, tuple)) and item and isinstance(item[0], int):
                ids.append(item[0])
        return ids
    return []


def get_model_fields(odoo, model_name: str, candidates: List[str]) -> List[str]:
    """Get available fields from model."""
    try:
        Model = odoo.env[model_name]
        meta = Model.fields_get(candidates)
        return [field for field in candidates if field in meta]
    except Exception:
        return []


def read_all_records(odoo, model_name: str, fields: List[str], domain: Optional[List] = None) -> List[Dict[str, Any]]:
    """Read all records from a model."""
    Model = odoo.env[model_name]
    if Model is None:
        return []
    ids = Model.search(domain or [])
    if not ids:
        return []

    records: List[Dict[str, Any]] = []
    for batch in batch_ids(set(ids)):
        try:
            records.extend(Model.read(batch, fields))
        except Exception as exc:
            logger = __import__('logging').getLogger(__name__)
            logger.error(f"Error reading {model_name} batch: {exc}")
    return records
