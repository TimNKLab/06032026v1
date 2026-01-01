import os
import json
import logging
import time
import threading
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Iterator
from enum import Enum
from contextlib import contextmanager
from dataclasses import dataclass
import polars as pl
from pydantic import BaseModel, Field
from celery import Celery, group, chord, chain
from celery.exceptions import Ignore
from celery.schedules import crontab
from odoorpc_connector import get_odoo_connection, retry_odoo

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

os.makedirs('/app/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/app/logs/etl.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CELERY CONFIGURATION
# ============================================================================

redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
app = Celery('etl_tasks', broker=redis_url, backend=redis_url)

app.conf.update(
    timezone=os.environ.get('TZ', 'Asia/Jakarta'),
    enable_utc=False,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    result_expires=3600,
    task_time_limit=1800,
    task_soft_time_limit=1500,
)

# ============================================================================
# CONSTANTS
# ============================================================================

MAX_RETRIES = 3
RETRY_DELAY = 5
ODOO_BATCH_SIZE = 500
PARQUET_COMPRESSION = 'zstd'
CONNECTION_TIMEOUT = 300
CACHE_TTL = 3600

# Data lake paths
DATA_LAKE_ROOT = os.environ.get('DATA_LAKE_ROOT', '/app/data-lake')
RAW_PATH = f'{DATA_LAKE_ROOT}/raw/pos_order_lines'
CLEAN_PATH = f'{DATA_LAKE_ROOT}/clean/pos_order_lines'
RAW_SALES_INVOICE_PATH = f'{DATA_LAKE_ROOT}/raw/account_move_out_invoice_lines'
RAW_PURCHASES_PATH = f'{DATA_LAKE_ROOT}/raw/account_move_in_invoice_lines'
CLEAN_SALES_INVOICE_PATH = f'{DATA_LAKE_ROOT}/clean/account_move_out_invoice_lines'
CLEAN_PURCHASES_PATH = f'{DATA_LAKE_ROOT}/clean/account_move_in_invoice_lines'
RAW_INVENTORY_MOVES_PATH = f'{DATA_LAKE_ROOT}/raw/inventory_moves'
CLEAN_INVENTORY_MOVES_PATH = f'{DATA_LAKE_ROOT}/clean/inventory_moves'
STAR_SCHEMA_PATH = f'{DATA_LAKE_ROOT}/star-schema'
METADATA_PATH = f'{DATA_LAKE_ROOT}/metadata'

DIM_PRODUCTS_FILE = f'{STAR_SCHEMA_PATH}/dim_products.parquet'
DIM_LOCATIONS_FILE = f'{STAR_SCHEMA_PATH}/dim_locations.parquet'
DIM_UOMS_FILE = f'{STAR_SCHEMA_PATH}/dim_uoms.parquet'
DIM_PARTNERS_FILE = f'{STAR_SCHEMA_PATH}/dim_partners.parquet'
DIM_USERS_FILE = f'{STAR_SCHEMA_PATH}/dim_users.parquet'
DIM_COMPANIES_FILE = f'{STAR_SCHEMA_PATH}/dim_companies.parquet'
DIM_LOTS_FILE = f'{STAR_SCHEMA_PATH}/dim_lots.parquet'

# Create directories
for path in [
    RAW_PATH, CLEAN_PATH, RAW_SALES_INVOICE_PATH, RAW_PURCHASES_PATH,
    CLEAN_SALES_INVOICE_PATH, CLEAN_PURCHASES_PATH, RAW_INVENTORY_MOVES_PATH,
    CLEAN_INVENTORY_MOVES_PATH, STAR_SCHEMA_PATH, METADATA_PATH,
]:
    os.makedirs(path, exist_ok=True)

# ============================================================================
# ENUMS AND MODELS
# ============================================================================

class ETLStatus(str, Enum):
    PENDING = 'PENDING'
    EXTRACTING = 'EXTRACTING'
    TRANSFORMING = 'TRANSFORMING'
    LOADING = 'LOADING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class ETLTaskStatus(BaseModel):
    task_id: str
    status: ETLStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    records_processed: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = {}

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ============================================================================
# THREAD-SAFE CONNECTION POOLING
# ============================================================================

@dataclass
class ConnectionState:
    connection: Optional[Any] = None
    last_used: Optional[float] = None


_thread_local = threading.local()


def _get_connection_state() -> ConnectionState:
    """Get thread-local connection state."""
    if not hasattr(_thread_local, 'conn_state'):
        _thread_local.conn_state = ConnectionState()
    return _thread_local.conn_state


@contextmanager
def get_pooled_odoo_connection():
    """Thread-safe connection pooling for Odoo."""
    state = _get_connection_state()
    current_time = time.time()

    # Reuse if valid
    if (state.connection is not None and
        state.last_used is not None and
        current_time - state.last_used < CONNECTION_TIMEOUT):
        try:
            # Verify connection is still alive
            state.connection.env['res.users'].search([], limit=1)
            state.last_used = current_time
            yield state.connection
            return
        except Exception:
            state.connection = None

    # Create new connection
    state.connection = get_odoo_connection()
    state.last_used = current_time

    if state.connection is None:
        raise Exception("Failed to establish Odoo connection")

    yield state.connection


# ============================================================================
# CACHING UTILITIES
# ============================================================================

def get_redis_client():
    """Get Redis client from Celery backend."""
    return app.backend.client


def cache_get(key: str) -> Optional[Any]:
    """Get value from Redis cache."""
    try:
        redis = get_redis_client()
        value = redis.get(f'etl:cache:{key}')
        return json.loads(value) if value else None
    except Exception as e:
        logger.warning(f"Cache get error for {key}: {e}")
        return None


def cache_set(key: str, value: Any, ttl: int = CACHE_TTL):
    """Set value in Redis cache with TTL."""
    try:
        redis = get_redis_client()
        redis.setex(f'etl:cache:{key}', ttl, json.dumps(value))
    except Exception as e:
        logger.warning(f"Cache set error for {key}: {e}")


def cache_delete(key: str):
    """Delete key from Redis cache."""
    try:
        redis = get_redis_client()
        redis.delete(f'etl:cache:{key}')
    except Exception as e:
        logger.warning(f"Cache delete error for {key}: {e}")


# ============================================================================
# DIMENSION LOADER (CACHED)
# ============================================================================

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


# ============================================================================
# METADATA MANAGEMENT
# ============================================================================

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
            logger.error(f"Error writing dimension sync metadata: {e}")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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


def atomic_write_parquet(df: pl.DataFrame, file_path: str):
    """Atomically write DataFrame to parquet file."""
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


def to_local_datetime(col_name: str) -> pl.Expr:
    """Convert UTC datetime string to local timezone."""
    return (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False)
        .dt.replace_time_zone('UTC')
        .dt.convert_time_zone(app.conf.timezone)
        .dt.replace_time_zone(None)
    )


# ============================================================================
# BATCH READ WITH BULK CACHING
# ============================================================================

def _batch_read_products(odoo, product_ids: Set[int]) -> Dict[int, Dict]:
    """Batch read products with bulk caching."""
    if not product_ids:
        return {}

    redis = get_redis_client()
    cache_keys = [f'etl:cache:product:{pid}' for pid in product_ids]

    # Bulk cache lookup
    try:
        cached_values = redis.mget(cache_keys)
        cached_data = {}
        uncached_ids = set()

        for pid, cached in zip(product_ids, cached_values):
            if cached:
                cached_data[pid] = json.loads(cached)
            else:
                uncached_ids.add(pid)
    except Exception:
        uncached_ids = product_ids
        cached_data = {}

    if not uncached_ids:
        return cached_data

    # Fetch uncached products
    Product = odoo.env['product.product']
    fields = ['name', 'categ_id', 'x_studio_brand_id']

    product_data = {}
    cache_pipeline = redis.pipeline()

    for batch in batch_ids(uncached_ids):
        try:
            products = Product.read(batch, fields)
            for prod in products:
                categ_value = prod.get('categ_id')
                categ_name = safe_extract_m2o(categ_value, get_id=False)

                parent_category = None
                leaf_category = None
                if isinstance(categ_name, str):
                    segments = [s.strip() for s in categ_name.split('/') if s.strip()]
                    if segments:
                        parent_category = segments[0]
                        leaf_category = segments[-1]

                brand_value = prod.get('x_studio_brand_id')

                prod_info = {
                    'name': prod.get('name'),
                    'category': leaf_category,
                    'parent_category': parent_category,
                    'brand_name': safe_extract_m2o(brand_value, get_id=False) or 'Unknown',
                    'brand_id': safe_extract_m2o(brand_value, get_id=True),
                }

                product_data[prod['id']] = prod_info

                cache_pipeline.setex(
                    f'etl:cache:product:{prod["id"]}',
                    CACHE_TTL,
                    json.dumps(prod_info)
                )

        except Exception as e:
            logger.error(f"Error reading product batch: {e}")

    # Execute bulk cache insert
    try:
        cache_pipeline.execute()
    except Exception as e:
        logger.warning(f"Cache bulk insert error: {e}")

    return {**cached_data, **product_data}


def _get_model_fields(odoo, model_name: str, candidates: List[str]) -> List[str]:
    """Get available fields from model."""
    try:
        Model = odoo.env[model_name]
        meta = Model.fields_get(candidates)
        return [field for field in candidates if field in meta]
    except Exception:
        return []


def _read_all_records(odoo, model_name: str, fields: List[str], domain: Optional[List] = None) -> List[Dict[str, Any]]:
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
            logger.error(f"Error reading {model_name} batch: {exc}")
    return records


def _locations_internal_usage(odoo, location_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
    """Get location usage info."""
    if not location_ids:
        return {}
    Location = odoo.env['stock.location']
    if Location is None:
        return {}

    fields = _get_model_fields(odoo, 'stock.location', ['id', 'usage', 'scrap_location', 'name'])
    if 'id' not in fields:
        fields = ['id'] + [f for f in fields if f != 'id']

    results: Dict[int, Dict[str, Any]] = {}
    for batch in batch_ids(location_ids):
        try:
            recs = Location.read(batch, fields)
        except Exception as e:
            logger.error(f"Error reading stock.location batch: {e}")
            continue
        for rec in recs:
            lid = rec.get('id')
            if isinstance(lid, int):
                results[lid] = rec
    return results


def _picking_type_code_to_movement_type(code: Optional[str]) -> Optional[str]:
    """Convert picking type code to movement type."""
    if not isinstance(code, str):
        return None
    mapping = {
        'incoming': 'receipt',
        'outgoing': 'delivery',
        'internal': 'internal_transfer',
    }
    return mapping.get(code)


# ============================================================================
# EXTRACTION TASKS
# ============================================================================

@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_pos_order_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract POS order lines with optimized batched API calls."""

    target_dt = date.fromisoformat(target_date)

    with get_pooled_odoo_connection() as odoo:
        start_dt = datetime.combine(target_dt, datetime.min.time())
        end_dt = start_dt.replace(hour=23, minute=59, second=59)

        PosOrder = odoo.env['pos.order']
        PosOrderLine = odoo.env['pos.order.line']

        if PosOrder is None or PosOrderLine is None:
            logger.warning("Missing required Odoo models: pos.order and/or pos.order.line")
            return {'lines': [], 'target_date': target_date}

        domain = [
            ('date_order', '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
            ('date_order', '<=', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
        ]

        order_fields = [
            'date_order', 'config_id', 'employee_id', 'partner_id',
            'name', 'amount_total', 'lines', 'payment_ids',
        ]

        orders = PosOrder.search_read(domain, order_fields)

        if not orders:
            logger.info(f"No pos.order found for {target_date}")
            return {'lines': [], 'target_date': target_date}

        order_ids: Set[int] = set()
        line_ids: Set[int] = set()
        payment_ids: Set[int] = set()
        payment_id_to_order_id: Dict[int, int] = {}

        for order in orders:
            if isinstance(order.get('id'), int):
                order_ids.add(order['id'])

            for lid in extract_o2m_ids(order.get('lines')):
                line_ids.add(lid)

            for pid in extract_o2m_ids(order.get('payment_ids')):
                payment_ids.add(pid)
                if isinstance(order.get('id'), int):
                    payment_id_to_order_id[pid] = order['id']

        if not line_ids:
            logger.info(f"No pos.order.line IDs found for {target_date}")
            return {'lines': [], 'target_date': target_date}

        # Read all lines
        line_fields = ['id', 'order_id', 'product_id', 'qty', 'price_subtotal_incl', 'x_studio_discount_amount']
        lines_by_order: Dict[int, List[Dict[str, Any]]] = {}
        product_ids: Set[int] = set()

        for batch in batch_ids(line_ids):
            try:
                line_recs = PosOrderLine.read(batch, line_fields)
            except Exception as e:
                logger.error(f"Error reading pos.order.line batch: {e}")
                continue

            for line in line_recs:
                order_id = safe_extract_m2o(line.get('order_id'))
                if not isinstance(order_id, int):
                    continue
                lines_by_order.setdefault(order_id, []).append(line)

                product_id = safe_extract_m2o(line.get('product_id'))
                if isinstance(product_id, int):
                    product_ids.add(product_id)

        if not product_ids:
            return {'lines': [], 'target_date': target_date}

        # Read payments
        payment_method_ids_by_order: Dict[int, List[int]] = {}
        PaymentModel = odoo.env['pos.payment']
        if payment_ids and PaymentModel is not None:
            payment_fields = ['id', 'amount', 'payment_method_id']
            for batch in batch_ids(payment_ids):
                try:
                    payment_recs = PaymentModel.read(batch, payment_fields)
                except Exception as e:
                    logger.error(f"Error reading pos.payment batch: {e}")
                    continue

                for pay in payment_recs:
                    pay_id = pay.get('id')
                    if not isinstance(pay_id, int):
                        continue
                    order_id = payment_id_to_order_id.get(pay_id)
                    if not isinstance(order_id, int):
                        continue
                    amount = safe_float(pay.get('amount'))
                    if amount <= 0:
                        continue
                    method_id = safe_extract_m2o(pay.get('payment_method_id'))
                    if isinstance(method_id, int):
                        payment_method_ids_by_order.setdefault(order_id, []).append(method_id)

        payment_method_ids_json_by_order: Dict[int, str] = {}
        for oid in order_ids:
            method_ids = payment_method_ids_by_order.get(oid, [])
            method_ids = sorted(set([m for m in method_ids if isinstance(m, int)]))
            payment_method_ids_json_by_order[oid] = json.dumps(method_ids)

        # Batch read product enrichment
        product_data = _batch_read_products(odoo, product_ids)

        # Produce line-grain rows
        processed_lines: List[Dict[str, Any]] = []
        for order in orders:
            order_id = order.get('id')
            if not isinstance(order_id, int):
                continue

            order_lines = lines_by_order.get(order_id, [])
            if not order_lines:
                continue

            pos_config_id = safe_extract_m2o(order.get('config_id'))
            cashier_id = safe_extract_m2o(order.get('employee_id'))
            customer_id = safe_extract_m2o(order.get('partner_id'))
            order_ref = order.get('name')
            amount_total = safe_float(order.get('amount_total'))
            payment_method_ids = payment_method_ids_json_by_order.get(order_id, '[]')

            for line in order_lines:
                product_id = safe_extract_m2o(line.get('product_id'))
                if not isinstance(product_id, int):
                    continue

                product = product_data.get(product_id, {})

                processed_lines.append({
                    'order_date': order.get('date_order'),
                    'order_id': order_id,
                    'order_ref': order_ref,
                    'pos_config_id': pos_config_id,
                    'cashier_id': cashier_id,
                    'customer_id': customer_id,
                    'amount_total': amount_total,
                    'payment_method_ids': payment_method_ids,
                    'line_id': line.get('id'),
                    'product_id': product_id,
                    'qty': safe_float(line.get('qty')),
                    'price_subtotal_incl': safe_float(line.get('price_subtotal_incl')),
                    'discount_amount': safe_float(line.get('x_studio_discount_amount')),
                    'product_brand': product.get('brand_name', 'Unknown'),
                    'product_brand_id': product.get('brand_id'),
                    'product_name': product.get('name'),
                    'product_category': product.get('category'),
                    'product_parent_category': product.get('parent_category'),
                })

        return {
            'lines': processed_lines,
            'target_date': target_date,
            'count': len(processed_lines)
        }


def _extract_account_move_lines(target_date: str, move_type: str, partner_role: str) -> Dict[str, Any]:
    """Shared extractor for account.move invoice lines."""
    target_dt = date.fromisoformat(target_date)
    with get_pooled_odoo_connection() as odoo:
        start_dt = datetime.combine(target_dt, datetime.min.time())
        end_dt = start_dt.replace(hour=23, minute=59, second=59)

        Move = odoo.env['account.move']
        MoveLine = odoo.env['account.move.line']
        if Move is None or MoveLine is None:
            logger.warning("Missing required Odoo models: account.move and/or account.move.line")
            return {'lines': [], 'target_date': target_date}

        domain = [
            ('date', '>=', start_dt.strftime('%Y-%m-%d')),
            ('date', '<=', end_dt.strftime('%Y-%m-%d')),
            ('move_type', '=', move_type),
            ('state', '=', 'posted'),
        ]

        move_fields = ['id', 'date', 'name', 'partner_id', 'invoice_line_ids']
        moves = Move.search_read(domain, move_fields)
        if not moves:
            logger.info(f"No account.move ({move_type}) found for {target_date}")
            return {'lines': [], 'target_date': target_date}

        move_ids: Set[int] = set()
        line_ids: Set[int] = set()
        move_partner: Dict[int, Tuple[Optional[int], Optional[str]]] = {}
        move_dates: Dict[int, str] = {}
        move_names: Dict[int, str] = {}

        for mv in moves:
            mid = mv.get('id')
            if isinstance(mid, int):
                move_ids.add(mid)
                move_dates[mid] = mv.get('date')
                move_names[mid] = mv.get('name')
                partner_id = safe_extract_m2o(mv.get('partner_id'), get_id=True)
                partner_name = safe_extract_m2o(mv.get('partner_id'), get_id=False)
                move_partner[mid] = (partner_id, partner_name)
            for lid in extract_o2m_ids(mv.get('invoice_line_ids')):
                line_ids.add(lid)

        if not line_ids:
            logger.info(f"No invoice lines found for {target_date} ({move_type})")
            return {'lines': [], 'target_date': target_date}

        line_fields = ['id', 'move_id', 'product_id', 'price_unit', 'quantity', 'tax_ids']
        processed_lines: List[Dict[str, Any]] = []

        for batch in batch_ids(line_ids):
            try:
                lines = MoveLine.read(batch, line_fields)
            except Exception as e:
                logger.error(f"Error reading account.move.line batch: {e}")
                continue

            for ln in lines:
                move_id_val = safe_extract_m2o(ln.get('move_id'))
                if not isinstance(move_id_val, int):
                    continue

                product_id = safe_extract_m2o(ln.get('product_id'))
                price_unit = safe_float(ln.get('price_unit'))
                quantity = safe_float(ln.get('quantity'))

                tax_ids = extract_o2m_ids(ln.get('tax_ids'))
                tax_ids_list = sorted(set([tid for tid in tax_ids if isinstance(tid, int)]))
                tax_id = tax_ids_list[0] if tax_ids_list else None
                tax_ids_json = json.dumps(tax_ids_list)

                partner_id, partner_name = move_partner.get(move_id_val, (None, None))
                processed_lines.append({
                    'move_id': move_id_val,
                    'move_name': move_names.get(move_id_val),
                    'move_date': move_dates.get(move_id_val),
                    f'{partner_role}_id': partner_id,
                    f'{partner_role}_name': partner_name,
                    'move_line_id': ln.get('id'),
                    'product_id': product_id,
                    'price_unit': price_unit,
                    'quantity': quantity,
                    'tax_id': tax_id,
                    'tax_ids_json': tax_ids_json,
                })

        return {
            'lines': processed_lines,
            'target_date': target_date,
            'count': len(processed_lines),
            'move_type': move_type,
        }


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_sales_invoice_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract posted customer invoices (out_invoice) lines."""
    return _extract_account_move_lines(target_date, move_type='out_invoice', partner_role='customer')


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_purchase_invoice_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract posted vendor bills (in_invoice) lines."""
    return _extract_account_move_lines(target_date, move_type='in_invoice', partner_role='vendor')


@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_inventory_moves(self, target_date: str) -> Dict[str, Any]:
    """Extract executed inventory moves (stock.move.line) for a target date."""

    target_dt = date.fromisoformat(target_date)

    with get_pooled_odoo_connection() as odoo:
        MoveLine = odoo.env['stock.move.line']
        Move = odoo.env['stock.move']
        Picking = odoo.env['stock.picking']
        PickingType = odoo.env['stock.picking.type']

        if MoveLine is None or Move is None:
            logger.warning("Missing required Odoo models: stock.move.line and/or stock.move")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        start_dt = datetime.combine(target_dt, datetime.min.time())
        end_dt = start_dt.replace(hour=23, minute=59, second=59)

        date_field = 'date'
        try:
            meta = MoveLine.fields_get(['date', 'date_done'])
            if 'date' in meta:
                date_field = 'date'
            elif 'date_done' in meta:
                date_field = 'date_done'
        except Exception:
            date_field = 'date'

        domain = [
            (date_field, '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
            (date_field, '<=', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
            ('qty_done', '!=', 0),
        ]
        try:
            domain.append(('move_id.state', '=', 'done'))
        except Exception:
            pass

        line_candidates = [
            'id', 'move_id', date_field, 'product_id', 'location_id',
            'location_dest_id', 'qty_done', 'product_uom_id', 'lot_id',
            'owner_id', 'picking_id', 'create_uid', 'create_date',
        ]
        line_fields = _get_model_fields(odoo, 'stock.move.line', line_candidates)
        if 'id' not in line_fields:
            line_fields = ['id'] + [f for f in line_fields if f != 'id']

        try:
            move_lines = MoveLine.search_read(domain, line_fields)
        except Exception as exc:
            logger.error(f"Error fetching stock.move.line data: {exc}")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        if not move_lines:
            logger.info(f"No stock.move.line found for {target_date}")
            return {'lines': [], 'target_date': target_date, 'count': 0}

        move_ids: Set[int] = set()
        picking_ids: Set[int] = set()
        location_ids: Set[int] = set()
        for ml in move_lines:
            mid = safe_extract_m2o(ml.get('move_id'))
            if isinstance(mid, int):
                move_ids.add(mid)
            pid = safe_extract_m2o(ml.get('picking_id'))
            if isinstance(pid, int):
                picking_ids.add(pid)
            src = safe_extract_m2o(ml.get('location_id'))
            dst = safe_extract_m2o(ml.get('location_dest_id'))
            if isinstance(src, int):
                location_ids.add(src)
            if isinstance(dst, int):
                location_ids.add(dst)

        move_candidates = [
            'id', 'name', 'reference', 'picking_id', 'picking_type_id',
            'origin', 'company_id', 'create_uid', 'create_date',
            'inventory_id', 'raw_material_production_id', 'production_id',
        ]
        move_fields = _get_model_fields(odoo, 'stock.move', move_candidates)
        if 'id' not in move_fields:
            move_fields = ['id'] + [f for f in move_fields if f != 'id']

        moves_by_id: Dict[int, Dict[str, Any]] = {}
        if move_ids:
            for batch in batch_ids(move_ids):
                try:
                    recs = Move.read(batch, move_fields)
                except Exception as e:
                    logger.error(f"Error reading stock.move batch: {e}")
                    continue
                for rec in recs:
                    rid = rec.get('id')
                    if isinstance(rid, int):
                        moves_by_id[rid] = rec

        pickings_by_id: Dict[int, Dict[str, Any]] = {}
        if Picking is not None and picking_ids:
            picking_candidates = [
                'id', 'name', 'partner_id', 'picking_type_id', 'origin', 'company_id',
            ]
            picking_fields = _get_model_fields(odoo, 'stock.picking', picking_candidates)
            if 'id' not in picking_fields:
                picking_fields = ['id'] + [f for f in picking_fields if f != 'id']

            for batch in batch_ids(picking_ids):
                try:
                    recs = Picking.read(batch, picking_fields)
                except Exception as e:
                    logger.error(f"Error reading stock.picking batch: {e}")
                    continue
                for rec in recs:
                    rid = rec.get('id')
                    if isinstance(rid, int):
                        pickings_by_id[rid] = rec

        picking_type_ids: Set[int] = set()
        for mv in moves_by_id.values():
            ptid = safe_extract_m2o(mv.get('picking_type_id'))
            if isinstance(ptid, int):
                picking_type_ids.add(ptid)
        for pk in pickings_by_id.values():
            ptid = safe_extract_m2o(pk.get('picking_type_id'))
            if isinstance(ptid, int):
                picking_type_ids.add(ptid)

        picking_type_by_id: Dict[int, Dict[str, Any]] = {}
        if PickingType is not None and picking_type_ids:
            pt_candidates = ['id', 'code', 'name']
            pt_fields = _get_model_fields(odoo, 'stock.picking.type', pt_candidates)
            if 'id' not in pt_fields:
                pt_fields = ['id'] + [f for f in pt_fields if f != 'id']
            for batch in batch_ids(picking_type_ids):
                try:
                    recs = PickingType.read(batch, pt_fields)
                except Exception as e:
                    logger.error(f"Error reading stock.picking.type batch: {e}")
                    continue
                for rec in recs:
                    rid = rec.get('id')
                    if isinstance(rid, int):
                        picking_type_by_id[rid] = rec

        locations_by_id = _locations_internal_usage(odoo, location_ids)

        processed: List[Dict[str, Any]] = []
        for ml in move_lines:
            ml_id = ml.get('id')
            if not isinstance(ml_id, int):
                continue

            move_id = safe_extract_m2o(ml.get('move_id'))
            if not isinstance(move_id, int):
                continue

            mv = moves_by_id.get(move_id, {})
            picking_id = safe_extract_m2o(ml.get('picking_id'))
            if not isinstance(picking_id, int):
                picking_id = safe_extract_m2o(mv.get('picking_id'))

            picking = pickings_by_id.get(picking_id, {}) if isinstance(picking_id, int) else {}

            src_id = safe_extract_m2o(ml.get('location_id'))
            dst_id = safe_extract_m2o(ml.get('location_dest_id'))

            qty_done = safe_float(ml.get('qty_done'))
            if qty_done == 0:
                continue

            src_usage = None
            dst_usage = None
            src_scrap = False
            dst_scrap = False

            if isinstance(src_id, int):
                loc = locations_by_id.get(src_id, {})
                src_usage = loc.get('usage')
                src_scrap = bool(loc.get('scrap_location') or False)
            if isinstance(dst_id, int):
                loc = locations_by_id.get(dst_id, {})
                dst_usage = loc.get('usage')
                dst_scrap = bool(loc.get('scrap_location') or False)

            src_internal = src_usage == 'internal'
            dst_internal = dst_usage == 'internal'

            qty_moved = qty_done
            if src_internal and not dst_internal:
                qty_moved = -abs(qty_done)
            elif not src_internal and dst_internal:
                qty_moved = abs(qty_done)

            picking_type_id = safe_extract_m2o(picking.get('picking_type_id'))
            if not isinstance(picking_type_id, int):
                picking_type_id = safe_extract_m2o(mv.get('picking_type_id'))
            picking_type_code = None
            if isinstance(picking_type_id, int):
                picking_type_code = picking_type_by_id.get(picking_type_id, {}).get('code')

            movement_type = _picking_type_code_to_movement_type(picking_type_code)
            inventory_adjustment_flag = False

            picking_type_name = None
            if isinstance(picking_type_id, int):
                picking_type_name = picking_type_by_id.get(picking_type_id, {}).get('name')

            manufacturing_order_id = None
            raw_mo = safe_extract_m2o(mv.get('raw_material_production_id'))
            prod_mo = safe_extract_m2o(mv.get('production_id'))
            if isinstance(raw_mo, int):
                movement_type = 'manufacturing_consumption'
                manufacturing_order_id = raw_mo
            elif isinstance(prod_mo, int):
                movement_type = 'manufacturing_output'
                manufacturing_order_id = prod_mo

            if dst_scrap:
                movement_type = 'scrap'
            elif (src_usage == 'inventory' or dst_usage == 'inventory') and movement_type not in {
                'manufacturing_consumption', 'manufacturing_output',
            }:
                movement_type = 'adjustment'
                inventory_adjustment_flag = True

            if isinstance(picking_type_name, str) and 'return' in picking_type_name.lower():
                if movement_type == 'receipt':
                    movement_type = 'return_from_customer'
                elif movement_type == 'delivery':
                    movement_type = 'return_to_vendor'

            origin_reference = picking.get('origin') or mv.get('origin')
            reference = picking.get('name') or mv.get('reference') or mv.get('name') or origin_reference

            company_id = safe_extract_m2o(picking.get('company_id'))
            if not isinstance(company_id, int):
                company_id = safe_extract_m2o(mv.get('company_id'))

            partner_id = safe_extract_m2o(picking.get('partner_id'))

            source_partner_id = None
            destination_partner_id = None
            if isinstance(partner_id, int):
                if not src_internal and dst_internal:
                    source_partner_id = partner_id
                elif src_internal and not dst_internal:
                    destination_partner_id = partner_id

            create_uid = safe_extract_m2o(ml.get('create_uid'))
            if not isinstance(create_uid, int):
                create_uid = safe_extract_m2o(mv.get('create_uid'))

            create_date_val = ml.get('create_date') or mv.get('create_date')

            processed.append({
                'move_id': move_id,
                'move_line_id': ml_id,
                'movement_date': ml.get(date_field),
                'product_id': safe_extract_m2o(ml.get('product_id')),
                'location_src_id': src_id,
                'location_dest_id': dst_id,
                'qty_moved': qty_moved,
                'uom_id': safe_extract_m2o(ml.get('product_uom_id')),
                'movement_type': movement_type,
                'picking_id': picking_id,
                'picking_type_code': picking_type_code,
                'reference': reference,
                'origin_reference': origin_reference,
                'company_id': company_id,
                'lot_id': safe_extract_m2o(ml.get('lot_id')),
                'owner_id': safe_extract_m2o(ml.get('owner_id')),
                'source_partner_id': source_partner_id,
                'destination_partner_id': destination_partner_id,
                'cost_per_unit': None,
                'inventory_adjustment_flag': inventory_adjustment_flag,
                'manufacturing_order_id': manufacturing_order_id,
                'created_by_user': create_uid,
                'create_date': create_date_val,
            })

        return {
            'lines': processed,
            'target_date': target_date,
            'count': len(processed),
        }


# ============================================================================
# SAVE RAW DATA TASKS
# ============================================================================

@app.task
def save_raw_data(extraction_result: Dict[str, Any]) -> Optional[str]:
    """Save raw POS order line extraction result to partitioned parquet."""
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result.get('target_date')

        if not target_date:
            logger.warning("Missing target_date in extraction result")
            return None

        year, month, day = target_date.split('-')
        partition_path = f'{RAW_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'order_date': pl.Utf8,
            'order_id': pl.Int64,
            'order_ref': pl.Utf8,
            'pos_config_id': pl.Int64,
            'cashier_id': pl.Int64,
            'customer_id': pl.Int64,
            'amount_total': pl.Float64,
            'payment_method_ids': pl.Utf8,
            'line_id': pl.Int64,
            'product_id': pl.Int64,
            'qty': pl.Float64,
            'price_subtotal_incl': pl.Float64,
            'discount_amount': pl.Float64,
            'product_brand': pl.Utf8,
            'product_brand_id': pl.Int64,
            'product_name': pl.Utf8,
            'product_category': pl.Utf8,
            'product_parent_category': pl.Utf8,
        }

        if not lines:
            logger.info(f"No data for {target_date} (pos_order_lines)")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('payment_method_ids').fill_null('[]'),
                pl.col('discount_amount').fill_null(0),
            ])

        output_file = f'{partition_path}/pos_order_lines_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw POS for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


def _save_raw_account_move_lines(extraction_result: Dict[str, Any], raw_base_path: str, dataset_prefix: str) -> Optional[str]:
    """Save raw account.move lines to partitioned parquet."""
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result['target_date']

        year, month, day = target_date.split('-')
        partition_path = f'{raw_base_path}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'move_id': pl.Int64,
            'move_name': pl.Utf8,
            'move_date': pl.Utf8,
            'customer_id': pl.Int64,
            'customer_name': pl.Utf8,
            'vendor_id': pl.Int64,
            'vendor_name': pl.Utf8,
            'move_line_id': pl.Int64,
            'product_id': pl.Int64,
            'price_unit': pl.Float64,
            'quantity': pl.Float64,
            'tax_id': pl.Int64,
            'tax_ids_json': pl.Utf8,
        }

        if not lines:
            logger.info(f"No data for {target_date} ({dataset_prefix})")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('tax_ids_json').fill_null('[]'),
            ])

        output_file = f'{partition_path}/{dataset_prefix}_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw {dataset_prefix} for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


@app.task
def save_raw_sales_invoice_lines(extraction_result: Dict[str, Any]) -> Optional[str]:
    return _save_raw_account_move_lines(extraction_result, RAW_SALES_INVOICE_PATH, 'account_move_out_invoice_lines')


@app.task
def save_raw_purchase_invoice_lines(extraction_result: Dict[str, Any]) -> Optional[str]:
    return _save_raw_account_move_lines(extraction_result, RAW_PURCHASES_PATH, 'account_move_in_invoice_lines')


@app.task
def save_raw_inventory_moves(extraction_result: Dict[str, Any]) -> Optional[str]:
    try:
        lines = extraction_result.get('lines', [])
        target_date = extraction_result.get('target_date')
        if not target_date:
            logger.warning("Missing target_date in extraction result")
            return None

        year, month, day = target_date.split('-')
        partition_path = f'{RAW_INVENTORY_MOVES_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(partition_path, exist_ok=True)

        raw_schema = {
            'move_id': pl.Int64,
            'move_line_id': pl.Int64,
            'movement_date': pl.Utf8,
            'product_id': pl.Int64,
            'location_src_id': pl.Int64,
            'location_dest_id': pl.Int64,
            'qty_moved': pl.Float64,
            'uom_id': pl.Int64,
            'picking_id': pl.Int64,
            'picking_type_code': pl.Utf8,
            'reference': pl.Utf8,
            'origin_reference': pl.Utf8,
            'source_partner_id': pl.Int64,
            'destination_partner_id': pl.Int64,
            'created_by_user': pl.Int64,
            'create_date': pl.Utf8,
        }

        if not lines:
            logger.info(f"No data for {target_date} (inventory_moves)")
            df = pl.DataFrame(schema=raw_schema)
        else:
            normalized = [
                {k: row.get(k) for k in raw_schema.keys()}
                for row in lines if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema_overrides=raw_schema, strict=False)
            df = df.with_columns([
                pl.col('qty_moved').fill_null(0),
            ])

        output_file = f'{partition_path}/inventory_moves_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Error saving raw inventory moves for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


# ============================================================================
# CLEAN DATA TASKS
# ============================================================================

def _clean_account_move_lines(
    raw_file_path: Optional[str],
    target_date: str,
    clean_base_path: str,
    dataset_prefix: str,
) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .filter(
                (pl.col('product_id').is_not_null()) &
                (pl.col('quantity').is_not_null()) &
                (pl.col('quantity') != 0) &
                (pl.col('price_unit').is_not_null())
            )
            .with_columns(
                pl.col('move_id', 'customer_id', 'vendor_id', 'move_line_id', 'product_id', 'tax_id')
                    .cast(pl.Int64, strict=False),
                pl.col('move_name', 'move_date', 'customer_name', 'vendor_name', 'tax_ids_json')
                    .cast(pl.Utf8, strict=False),
                pl.col('price_unit', 'quantity').cast(pl.Float64, strict=False),
                pl.col('tax_ids_json').fill_null('[]'),
            )
        )

        year, month, day = target_date.split('-')
        clean_path = f'{clean_base_path}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/{dataset_prefix}_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)

        logger.info(f"Cleaned invoice data saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning {dataset_prefix} for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_sales_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    return _clean_account_move_lines(raw_file_path, target_date, CLEAN_SALES_INVOICE_PATH, 'account_move_out_invoice_lines')


@app.task
def clean_purchase_invoice_lines(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    return _clean_account_move_lines(raw_file_path, target_date, CLEAN_PURCHASES_PATH, 'account_move_in_invoice_lines')


@app.task
def clean_inventory_moves(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .filter(pl.col('qty_moved').is_not_null() & (pl.col('qty_moved') != 0))
            .with_columns(
                pl.col('move_id', 'move_line_id', 'product_id', 'location_src_id',
                       'location_dest_id', 'uom_id', 'picking_id', 'source_partner_id',
                       'destination_partner_id', 'created_by_user').cast(pl.Int64, strict=False),
                pl.col('qty_moved').cast(pl.Float64, strict=False),
                pl.col('picking_type_code', 'reference', 'origin_reference').cast(pl.Utf8, strict=False),
                to_local_datetime('movement_date'),
                to_local_datetime('create_date'),
            )
        )

        # Load dimensions once
        dim_products = DimensionLoader.get(DIM_PRODUCTS_FILE)
        dim_locations = DimensionLoader.get(DIM_LOCATIONS_FILE)
        dim_uoms = DimensionLoader.get(DIM_UOMS_FILE)
        dim_partners = DimensionLoader.get(DIM_PARTNERS_FILE)

        # Products join
        if dim_products is not None:
            df_clean = df_clean.join(
                dim_products.select('product_id', 'product_name', 'product_brand'),
                on='product_id', how='left'
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('product_name'),
                pl.lit(None, dtype=pl.Utf8).alias('product_brand'),
            )

        # Locations join (single load, two projections)
        if dim_locations is not None:
            df_clean = (
                df_clean
                .join(
                    dim_locations.select(
                        pl.col('location_id').alias('location_src_id'),
                        pl.col('location_name').alias('location_src_name'),
                        pl.col('location_usage').alias('location_src_usage'),
                    ),
                    on='location_src_id', how='left'
                )
                .join(
                    dim_locations.select(
                        pl.col('location_id').alias('location_dest_id'),
                        pl.col('location_name').alias('location_dest_name'),
                        pl.col('location_usage').alias('location_dest_usage'),
                    ),
                    on='location_dest_id', how='left'
                )
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('location_src_name'),
                pl.lit(None, dtype=pl.Utf8).alias('location_src_usage'),
                pl.lit(None, dtype=pl.Utf8).alias('location_dest_name'),
                pl.lit(None, dtype=pl.Utf8).alias('location_dest_usage'),
            )

        # UOMs join
        if dim_uoms is not None:
            df_clean = df_clean.join(
                dim_uoms.select('uom_id', 'uom_name', 'uom_category'),
                on='uom_id', how='left'
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('uom_name'),
                pl.lit(None, dtype=pl.Utf8).alias('uom_category'),
            )

        # Partners join (single load, two projections)
        if dim_partners is not None:
            df_clean = (
                df_clean
                .join(
                    dim_partners.select(
                        pl.col('partner_id').alias('source_partner_id'),
                        pl.col('partner_name').alias('source_partner_name'),
                    ),
                    on='source_partner_id', how='left'
                )
                .join(
                    dim_partners.select(
                        pl.col('partner_id').alias('destination_partner_id'),
                        pl.col('partner_name').alias('destination_partner_name'),
                    ),
                    on='destination_partner_id', how='left'
                )
            )
        else:
            df_clean = df_clean.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('source_partner_name'),
                pl.lit(None, dtype=pl.Utf8).alias('destination_partner_name'),
            )

        # Filter and select
        output_columns = [
            'move_id', 'move_line_id', 'movement_date', 'product_id', 'product_name',
            'product_brand', 'location_src_id', 'location_src_name', 'location_dest_id',
            'location_dest_name', 'qty_moved', 'uom_id', 'uom_name', 'uom_category',
            'picking_id', 'picking_type_code', 'reference', 'origin_reference',
            'source_partner_id', 'source_partner_name', 'destination_partner_id',
            'destination_partner_name', 'created_by_user', 'create_date',
        ]

        df_clean = (
            df_clean
            .filter(~(
                (pl.col('location_src_usage') == 'internal') &
                (pl.col('location_dest_usage') == 'internal')
            ))
            .select(output_columns)
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_INVENTORY_MOVES_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/inventory_moves_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)

        logger.info(f"Cleaned inventory moves saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning inventory moves for {target_date}: {e}", exc_info=True)
        return None


@app.task
def clean_pos_data(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Clean and validate POS data with lazy evaluation."""
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None

        df_clean = (
            pl.scan_parquet(raw_file_path)
            .filter(
                (pl.col('product_id').is_not_null()) &
                (pl.col('qty').is_not_null()) &
                (pl.col('qty') != 0) &
                (pl.col('price_subtotal_incl').is_not_null())
            )
            .with_columns(
                pl.col('order_date')
                    .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S')
                    .dt.replace_time_zone('UTC')
                    .dt.convert_time_zone(app.conf.timezone)
                    .dt.replace_time_zone(None),
                pl.col('order_id', 'pos_config_id', 'cashier_id', 'customer_id',
                       'line_id', 'product_brand_id').cast(pl.Int64, strict=False),
                pl.col('order_ref', 'payment_method_ids').cast(pl.Utf8, strict=False),
                pl.col('amount_total', 'qty', 'price_subtotal_incl', 'discount_amount')
                    .cast(pl.Float64, strict=False),
                pl.col('payment_method_ids').fill_null('[]'),
                pl.col('discount_amount').fill_null(0),
                pl.col('product_brand').fill_null('Unknown'),
                pl.col('product_category').fill_null('Unknown'),
                pl.col('product_parent_category').fill_null('Unknown'),
            )
        )

        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/pos_order_lines_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(streaming=True), output_file)

        logger.info(f"Cleaned data saved to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error cleaning data for {target_date}: {e}", exc_info=True)
        return None


# ============================================================================
# STAR SCHEMA UPDATE TASKS
# ============================================================================

def _update_fact_invoice_sales(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('move_date').alias('date'),
        'move_id', 'move_name', 'customer_id', 'customer_name',
        'move_line_id', 'product_id', 'price_unit', 'quantity', 'tax_ids_json',
        pl.lit(False).alias('is_free_item'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_invoice_sales'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_invoice_sales_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_purchases(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('move_date').alias('date'),
        'move_id', 'move_name', 'vendor_id', 'vendor_name',
        'move_line_id', 'product_id', 'price_unit', 'quantity',
        'tax_id', 'tax_ids_json',
        pl.lit(False).cast(pl.Boolean).alias('is_free_item'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_purchases'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_purchases_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_sales_pos(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('order_date').alias('date'),
        'order_id', 'order_ref', 'pos_config_id', 'cashier_id',
        'customer_id', 'payment_method_ids', 'line_id', 'product_id',
        pl.col('qty').alias('quantity'),
        pl.col('price_subtotal_incl').alias('revenue'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_sales'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_sales_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


def _update_fact_inventory_moves(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('movement_date').alias('date'),
        'move_id', 'move_line_id', 'product_id', 'product_name', 'product_brand',
        'location_src_id', 'location_src_name', 'location_dest_id', 'location_dest_name',
        'qty_moved', 'uom_id', 'uom_name', 'uom_category', 'picking_id', 'picking_type_code',
        'reference', 'origin_reference', 'source_partner_id', 'source_partner_name',
        'destination_partner_id', 'destination_partner_name', 'created_by_user', 'create_date',
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_inventory_moves'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_inventory_moves_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)
    return fact_output


@app.task
def update_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None

        df = pl.read_parquet(clean_file_path)
        output = _update_fact_sales_pos(df, target_date)
        ETLMetadata.set_last_processed_date(date.fromisoformat(target_date))
        return output

    except Exception as e:
        logger.error(f"Error updating star schema for {target_date}: {e}", exc_info=True)
        return None


@app.task
def update_invoice_sales_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_invoice_sales(df, target_date)
    except Exception as e:
        logger.error(f"Error updating invoice sales star schema for {target_date}: {e}", exc_info=True)
        return None


@app.task
def update_purchase_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_purchases(df, target_date)
    except Exception as e:
        logger.error(f"Error updating purchases star schema for {target_date}: {e}", exc_info=True)
        return None


@app.task
def update_inventory_moves_star_schema(clean_file_path: Optional[str], target_date: str) -> Optional[str]:
    try:
        if not clean_file_path or not os.path.isfile(clean_file_path):
            logger.warning(f"Invalid file path: {clean_file_path}")
            return None
        df = pl.read_parquet(clean_file_path)
        return _update_fact_inventory_moves(df, target_date)
    except Exception as e:
        logger.error(f"Error updating inventory moves star schema for {target_date}: {e}", exc_info=True)
        return None


# ============================================================================
# DIMENSION REFRESH
# ============================================================================

@app.task
def refresh_dimensions_incremental(targets: Optional[List[str]] = None) -> Dict[str, Any]:
    """Build/update dimension parquet files used for enrichment."""
    try:
        target_list = targets or [
            'products', 'locations', 'uoms', 'partners', 'users', 'companies', 'lots',
        ]

        results: Dict[str, int] = {}
        now = datetime.now()

        # Clear dimension cache before refresh
        DimensionLoader.clear_cache()

        with get_pooled_odoo_connection() as odoo:
            if 'products' in target_list:
                Product = odoo.env['product.product']
                if Product is not None:
                    fields = _get_model_fields(odoo, 'product.product', ['id', 'name', 'categ_id', 'x_studio_brand_id'])
                    records = _read_all_records(odoo, 'product.product', fields)
                    rows = []
                    for prod in records:
                        pid = prod.get('id')
                        if not isinstance(pid, int):
                            continue
                        categ_val = prod.get('categ_id')
                        categ_name = safe_extract_m2o(categ_val, get_id=False)
                        parent_category = None
                        leaf_category = None
                        if isinstance(categ_name, str):
                            parts = [p.strip() for p in categ_name.split('/') if p.strip()]
                            if parts:
                                parent_category = parts[0]
                                leaf_category = parts[-1]
                        brand_val = prod.get('x_studio_brand_id')
                        rows.append({
                            'product_id': pid,
                            'product_name': prod.get('name'),
                            'product_category': leaf_category,
                            'product_parent_category': parent_category,
                            'product_brand': safe_extract_m2o(brand_val, get_id=False) or 'Unknown',
                            'product_brand_id': safe_extract_m2o(brand_val, get_id=True),
                        })
                    if rows:
                        atomic_write_parquet(pl.DataFrame(rows), DIM_PRODUCTS_FILE)
                    results['products'] = len(rows)

            if 'locations' in target_list and 'stock.location' in odoo.env:
                fields = _get_model_fields(odoo, 'stock.location', ['id', 'complete_name', 'name', 'usage', 'scrap_location'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = _read_all_records(odoo, 'stock.location', fields)
                rows = []
                for loc in records:
                    lid = loc.get('id')
                    if not isinstance(lid, int):
                        continue
                    rows.append({
                        'location_id': lid,
                        'location_name': loc.get('complete_name') or loc.get('name'),
                        'location_usage': loc.get('usage'),
                        'scrap_location': bool(loc.get('scrap_location') or False),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_LOCATIONS_FILE)
                results['locations'] = len(rows)

            if 'uoms' in target_list and 'uom.uom' in odoo.env:
                fields = _get_model_fields(odoo, 'uom.uom', ['id', 'name', 'category_id'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = _read_all_records(odoo, 'uom.uom', fields)
                rows = []
                for uom in records:
                    uid = uom.get('id')
                    if not isinstance(uid, int):
                        continue
                    rows.append({
                        'uom_id': uid,
                        'uom_name': uom.get('name'),
                        'uom_category': safe_extract_m2o(uom.get('category_id'), get_id=False),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_UOMS_FILE)
                results['uoms'] = len(rows)

            if 'partners' in target_list and 'res.partner' in odoo.env:
                fields = _get_model_fields(odoo, 'res.partner', ['id', 'name', 'ref', 'email', 'phone', 'is_company'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = _read_all_records(odoo, 'res.partner', fields)
                rows = []
                for partner in records:
                    pid = partner.get('id')
                    if not isinstance(pid, int):
                        continue
                    rows.append({
                        'partner_id': pid,
                        'partner_name': partner.get('name'),
                        'partner_ref': partner.get('ref'),
                        'partner_email': partner.get('email'),
                        'partner_phone': partner.get('phone'),
                        'is_company': bool(partner.get('is_company') or False),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_PARTNERS_FILE)
                results['partners'] = len(rows)

            if 'users' in target_list and 'res.users' in odoo.env:
                fields = _get_model_fields(odoo, 'res.users', ['id', 'name', 'partner_id', 'login'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = _read_all_records(odoo, 'res.users', fields)
                rows = []
                for user in records:
                    uid = user.get('id')
                    if not isinstance(uid, int):
                        continue
                    rows.append({
                        'user_id': uid,
                        'user_name': user.get('name'),
                        'user_login': user.get('login'),
                        'partner_id': safe_extract_m2o(user.get('partner_id'), get_id=True),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_USERS_FILE)
                results['users'] = len(rows)

            if 'companies' in target_list and 'res.company' in odoo.env:
                fields = _get_model_fields(odoo, 'res.company', ['id', 'name', 'partner_id'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = _read_all_records(odoo, 'res.company', fields)
                rows = []
                for comp in records:
                    cid = comp.get('id')
                    if not isinstance(cid, int):
                        continue
                    rows.append({
                        'company_id': cid,
                        'company_name': comp.get('name'),
                        'partner_id': safe_extract_m2o(comp.get('partner_id'), get_id=True),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_COMPANIES_FILE)
                results['companies'] = len(rows)

            if 'lots' in target_list and 'stock.lot' in odoo.env:
                fields = _get_model_fields(odoo, 'stock.lot', ['id', 'name', 'product_id'])
                if 'id' not in fields:
                    fields = ['id'] + [f for f in fields if f != 'id']
                records = _read_all_records(odoo, 'stock.lot', fields)
                rows = []
                for lot in records:
                    lid = lot.get('id')
                    if not isinstance(lid, int):
                        continue
                    rows.append({
                        'lot_id': lid,
                        'lot_name': lot.get('name'),
                        'product_id': safe_extract_m2o(lot.get('product_id'), get_id=True),
                    })
                if rows:
                    atomic_write_parquet(pl.DataFrame(rows), DIM_LOTS_FILE)
                results['lots'] = len(rows)

        for dim in results.keys():
            ETLMetadata.set_dimension_last_sync(dim, now)

        return {'updated': True, 'targets': results}
    except Exception as exc:
        logger.error(f"Error refreshing dimensions: {exc}", exc_info=True)
        return {'updated': False, 'error': str(exc)}


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================

@app.task
def daily_etl_pipeline(target_date: Optional[str] = None) -> str:
    """Optimized daily ETL pipeline."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting ETL pipeline for {target_date}")

    pipeline = chain(
        extract_pos_order_lines.s(target_date),
        save_raw_data.s(),
        clean_pos_data.s(target_date),
        update_star_schema.s(target_date)
    )

    result = pipeline.apply_async()
    logger.info(f"ETL pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


@app.task
def daily_invoice_sales_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for invoice-based sales (out_invoice)."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting invoice sales pipeline for {target_date}")
    pipeline = chain(
        extract_sales_invoice_lines.s(target_date),
        save_raw_sales_invoice_lines.s(),
        clean_sales_invoice_lines.s(target_date),
        update_invoice_sales_star_schema.s(target_date),
    )
    result = pipeline.apply_async()
    logger.info(f"Invoice sales pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


@app.task
def daily_invoice_purchases_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for purchases (vendor bills, in_invoice)."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting purchases pipeline for {target_date}")
    pipeline = chain(
        extract_purchase_invoice_lines.s(target_date),
        save_raw_purchase_invoice_lines.s(),
        clean_purchase_invoice_lines.s(target_date),
        update_purchase_star_schema.s(target_date),
    )
    result = pipeline.apply_async()
    logger.info(f"Purchases pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


@app.task
def daily_inventory_moves_pipeline(target_date: Optional[str] = None) -> str:
    """Daily pipeline for inventory moves (stock.move.line)."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting inventory moves pipeline for {target_date}")
    pipeline = chain(
        refresh_dimensions_incremental.si(['products', 'locations', 'uoms', 'partners', 'users', 'companies', 'lots']),
        extract_inventory_moves.si(target_date),
        save_raw_inventory_moves.s(),
        clean_inventory_moves.s(target_date),
        update_inventory_moves_star_schema.s(target_date),
    )
    result = pipeline.apply_async()
    logger.info(f"Inventory moves pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


@app.task
def date_range_etl_pipeline(start_date: str, end_date: Optional[str] = None) -> Dict[str, Any]:
    """Process date range in parallel."""
    if end_date is None:
        end_date = start_date

    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    logger.info(f"Starting parallel ETL for {start_date} to {end_date}")

    delta = end_dt - start_dt
    date_range = [
        (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(delta.days + 1)
    ]

    job = group(daily_etl_pipeline.s(date_str) for date_str in date_range)
    result = job.apply_async()

    return {
        "status": "queued",
        "start_date": start_date,
        "end_date": end_date,
        "total_days": len(date_range),
        "group_id": result.id,
        "message": f"Parallel ETL for {len(date_range)} days"
    }


@app.task
def catch_up_etl() -> Dict[str, Any]:
    """Auto-catch up missed dates."""
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

        return date_range_etl_pipeline.delay(start_date, end_date).get()

    return {"status": "up_to_date"}


@app.task
def health_check() -> Dict[str, Any]:
    """Health check with auto-recovery."""
    try:
        last_processed = ETLMetadata.get_last_processed_date()
        today = date.today()

        if not last_processed:
            return {"status": "unknown", "message": "No metadata found"}

        days_behind = (today - last_processed).days

        if days_behind <= 1:
            return {"status": "healthy", "last_processed": last_processed.isoformat()}

        logger.warning(f"ETL is {days_behind} days behind, triggering catch-up")
        catch_up_etl.delay()

        return {
            "status": "unhealthy",
            "days_behind": days_behind,
            "action": "triggered_catch_up"
        }

    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# CELERY BEAT SCHEDULE
# ============================================================================

app.conf.beat_schedule = {
    'daily-etl': {
        'task': 'etl_tasks.daily_etl_pipeline',
        'schedule': crontab(hour=2, minute=0),
    },
    'daily-invoice-sales-etl': {
        'task': 'etl_tasks.daily_invoice_sales_pipeline',
        'schedule': crontab(hour=2, minute=5),
    },
    'daily-invoice-purchases-etl': {
        'task': 'etl_tasks.daily_invoice_purchases_pipeline',
        'schedule': crontab(hour=2, minute=10),
    },
    'daily-inventory-moves-etl': {
        'task': 'etl_tasks.daily_inventory_moves_pipeline',
        'schedule': crontab(hour=2, minute=15),
    },
    'incremental-dimension-refresh': {
        'task': 'etl_tasks.refresh_dimensions_incremental',
        'schedule': crontab(hour='*/4', minute=0),
    },
    'health-check': {
        'task': 'etl_tasks.health_check',
        'schedule': crontab(hour='*/6', minute=0),
    },
}

app.conf.task_routes = {
    'etl_tasks.extract_pos_order_lines': {'queue': 'extraction'},
    'etl_tasks.extract_sales_invoice_lines': {'queue': 'extraction'},
    'etl_tasks.extract_purchase_invoice_lines': {'queue': 'extraction'},
    'etl_tasks.extract_inventory_moves': {'queue': 'extraction'},
    'etl_tasks.clean_pos_data': {'queue': 'transformation'},
    'etl_tasks.clean_sales_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_purchase_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_inventory_moves': {'queue': 'transformation'},
    'etl_tasks.update_star_schema': {'queue': 'loading'},
    'etl_tasks.update_invoice_sales_star_schema': {'queue': 'loading'},
    'etl_tasks.update_purchase_star_schema': {'queue': 'loading'},
    'etl_tasks.update_inventory_moves_star_schema': {'queue': 'loading'},
    'etl_tasks.save_raw_data': {'queue': 'loading'},
    'etl_tasks.save_raw_sales_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_purchase_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_inventory_moves': {'queue': 'loading'},
    'etl_tasks.refresh_dimensions_incremental': {'queue': 'dimensions'},
}