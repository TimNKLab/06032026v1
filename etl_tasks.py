import os
import json
import logging
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple
from enum import Enum
from contextlib import contextmanager
from functools import wraps
import polars as pl
from pydantic import BaseModel, Field
from celery import Celery, group, chord, chain
from celery.exceptions import Ignore
from odoorpc_connector import get_odoo_connection, retry_odoo
import hashlib

# Configure logging
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

# Redis and Celery setup
redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
app = Celery('etl_tasks', broker=redis_url, backend=redis_url)

# Celery configuration
app.conf.update(
    timezone=os.environ.get('TZ', 'Asia/Jakarta'),
    enable_utc=False,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    result_expires=3600,
    task_time_limit=1800,  # 30 minutes hard limit
    task_soft_time_limit=1500,  # 25 minutes soft limit
)

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 5
ODOO_BATCH_SIZE = 500
PARQUET_COMPRESSION = 'zstd'
CONNECTION_TIMEOUT = 300  # 5 minutes
CACHE_TTL = 3600  # 1 hour for Redis cache

# Data lake paths
DATA_LAKE_ROOT = os.environ.get('DATA_LAKE_ROOT', '/app/data-lake')
RAW_PATH = f'{DATA_LAKE_ROOT}/raw/pos_order_lines'
CLEAN_PATH = f'{DATA_LAKE_ROOT}/clean/pos_order_lines'
RAW_SALES_INVOICE_PATH = f'{DATA_LAKE_ROOT}/raw/account_move_out_invoice_lines'
RAW_PURCHASES_PATH = f'{DATA_LAKE_ROOT}/raw/account_move_in_invoice_lines'
CLEAN_SALES_INVOICE_PATH = f'{DATA_LAKE_ROOT}/clean/account_move_out_invoice_lines'
CLEAN_PURCHASES_PATH = f'{DATA_LAKE_ROOT}/clean/account_move_in_invoice_lines'
STAR_SCHEMA_PATH = f'{DATA_LAKE_ROOT}/star-schema'
METADATA_PATH = f'{DATA_LAKE_ROOT}/metadata'

# Create directories
for path in [
    RAW_PATH,
    CLEAN_PATH,
    RAW_SALES_INVOICE_PATH,
    RAW_PURCHASES_PATH,
    CLEAN_SALES_INVOICE_PATH,
    CLEAN_PURCHASES_PATH,
    STAR_SCHEMA_PATH,
    METADATA_PATH,
]:
    os.makedirs(path, exist_ok=True)

# Connection pool
_odoo_connection = None
_connection_last_used = None
_connection_lock = None

try:
    import threading
    _connection_lock = threading.Lock()
except ImportError:
    pass

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
# CONNECTION POOLING
# ============================================================================

@contextmanager
def get_pooled_odoo_connection():
    """Thread-safe connection pooling for Odoo."""
    global _odoo_connection, _connection_last_used
    
    if _connection_lock:
        _connection_lock.acquire()
    
    try:
        current_time = time.time()
        
        # Reuse if valid
        if (_odoo_connection is not None and 
            _connection_last_used is not None and 
            current_time - _connection_last_used < CONNECTION_TIMEOUT):
            _connection_last_used = current_time
            yield _odoo_connection
            return
        
        # Create new connection
        _odoo_connection = get_odoo_connection()
        _connection_last_used = current_time
        
        if _odoo_connection is None:
            raise Exception("Failed to establish Odoo connection")
        
        yield _odoo_connection
        
    finally:
        if _connection_lock:
            _connection_lock.release()

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
            
            # Atomic write
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

def batch_ids(ids: Set[int], batch_size: int = ODOO_BATCH_SIZE) -> List[List[int]]:
    """Split IDs into batches."""
    id_list = sorted(ids)
    return [id_list[i:i + batch_size] for i in range(0, len(id_list), batch_size)]

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

def format_brand(value: Any) -> Dict[str, Optional[Any]]:
    """Format brand to dict with id, name, parent, and principal."""
    if isinstance(value, (list, tuple)) and value:
        return {
            "id": value[0],
            "name": value[1] if len(value) >= 2 else None,
            "parent": value[2] if len(value) >= 3 else None,
            "principal": value[3] if len(value) >= 4 else None,
        }
    if isinstance(value, dict):
        return {
            "id": value.get("id"),
            "name": value.get("name"),
            "parent": value.get("parent"),
            "principal": value.get("principal"),
        }
    if isinstance(value, int):
        return {"id": value, "name": None, "parent": None, "principal": None}
    return {"id": None, "name": None, "parent": None, "principal": None}

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
            except:
                pass

# ============================================================================
# EXTRACTION TASKS
# ============================================================================

@app.task(bind=True, max_retries=3)
@retry_odoo(max_retries=3, delay=2)
def extract_pos_order_lines(self, target_date: str) -> Dict[str, Any]:
    """Extract POS order lines derived from pos.order with optimized batched API calls."""
    
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
            'date_order',
            'config_id',
            'employee_id',
            'partner_id',
            'name',
            'amount_total',
            'lines',
            'payment_ids',
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

        # Read all lines referenced by orders
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

        # Read payments and compute multi-valued payment method IDs per order
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
                    try:
                        amount = float(pay.get('amount') or 0)
                    except Exception:
                        amount = 0.0
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

        # Produce line-grain rows with order header context
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

            try:
                amount_total = float(order.get('amount_total') or 0)
            except Exception:
                amount_total = 0.0

            payment_method_ids = payment_method_ids_json_by_order.get(order_id, '[]')

            for line in order_lines:
                product_id = safe_extract_m2o(line.get('product_id'))
                if not isinstance(product_id, int):
                    continue

                product = product_data.get(product_id, {})

                try:
                    qty = float(line.get('qty') or 0)
                except Exception:
                    qty = 0.0

                try:
                    price_subtotal_incl = float(line.get('price_subtotal_incl') or 0)
                except Exception:
                    price_subtotal_incl = 0.0

                try:
                    discount_amount = float(line.get('x_studio_discount_amount') or 0)
                except Exception:
                    discount_amount = 0.0

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
                    'qty': qty,
                    'price_subtotal_incl': price_subtotal_incl,
                    'discount_amount': discount_amount,
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
    """
    Shared extractor for account.move invoice lines.
    partner_role: "customer" for out_invoice, "vendor" for in_invoice
    """
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
                try:
                    price_unit = float(ln.get('price_unit') or 0)
                except Exception:
                    price_unit = 0.0
                try:
                    quantity = float(ln.get('quantity') or 0)
                except Exception:
                    quantity = 0.0

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


def _batch_read_products(odoo, product_ids: Set[int]) -> Dict[int, Dict]:
    """Batch read products with caching."""
    if not product_ids:
        return {}
    
    # Check cache first
    uncached_ids = set()
    cached_data = {}
    
    for prod_id in product_ids:
        cache_key = f'product:{prod_id}'
        cached = cache_get(cache_key)
        if cached:
            cached_data[prod_id] = cached
        else:
            uncached_ids.add(prod_id)
    
    if not uncached_ids:
        return cached_data
    
    # Fetch uncached products
    Product = odoo.env['product.product']
    fields = ['name', 'categ_id', 'x_studio_brand_id']
    
    product_data = {}
    for batch in batch_ids(uncached_ids):
        try:
            products = Product.read(batch, fields)
            for prod in products:
                # Parse category
                categ_value = prod.get('categ_id')
                categ_name = safe_extract_m2o(categ_value, get_id=False)
                
                parent_category = None
                leaf_category = None
                if isinstance(categ_name, str):
                    segments = [s.strip() for s in categ_name.split('/') if s.strip()]
                    if segments:
                        parent_category = segments[0]
                        leaf_category = segments[-1]
                
                # Parse brand
                brand_value = prod.get('x_studio_brand_id')
                brand_name = safe_extract_m2o(brand_value, get_id=False) or 'Unknown'
                brand_id = safe_extract_m2o(brand_value, get_id=True)
                
                prod_info = {
                    'name': prod.get('name'),
                    'category': leaf_category,
                    'parent_category': parent_category,
                    'brand_name': brand_name,
                    'brand_id': brand_id,
                }
                
                product_data[prod['id']] = prod_info
                
                # Cache for 1 hour
                cache_set(f'product:{prod["id"]}', prod_info, ttl=CACHE_TTL)
                
        except Exception as e:
            logger.error(f"Error reading product batch: {e}")
    
    # Merge cached and fetched data
    return {**cached_data, **product_data}

# SAVE & CLEAN TASKS
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
                for row in lines
                if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema=raw_schema)
            df = df.with_columns([
                pl.col('order_date').cast(pl.Utf8, strict=False),
                pl.col('order_id').cast(pl.Int64, strict=False),
                pl.col('order_ref').cast(pl.Utf8, strict=False),
                pl.col('pos_config_id').cast(pl.Int64, strict=False),
                pl.col('cashier_id').cast(pl.Int64, strict=False),
                pl.col('customer_id').cast(pl.Int64, strict=False),
                pl.col('amount_total').cast(pl.Float64, strict=False),
                pl.col('payment_method_ids').cast(pl.Utf8, strict=False).fill_null('[]'),
                pl.col('line_id').cast(pl.Int64, strict=False),
                pl.col('product_id').cast(pl.Int64, strict=False),
                pl.col('qty').cast(pl.Float64, strict=False),
                pl.col('price_subtotal_incl').cast(pl.Float64, strict=False),
                pl.col('discount_amount').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('product_brand').cast(pl.Utf8, strict=False),
                pl.col('product_brand_id').cast(pl.Int64, strict=False),
                pl.col('product_name').cast(pl.Utf8, strict=False),
                pl.col('product_category').cast(pl.Utf8, strict=False),
                pl.col('product_parent_category').cast(pl.Utf8, strict=False),
            ])

        output_file = f'{partition_path}/pos_order_lines_{target_date}.parquet'
        atomic_write_parquet(df, output_file)
        logger.info(f"Saved {len(lines)} records to {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error saving raw POS for {extraction_result.get('target_date')}: {e}", exc_info=True)
        return None


def _save_raw_account_move_lines(extraction_result: Dict[str, Any], raw_base_path: str, dataset_prefix: str) -> Optional[str]:
    """Save raw account.move lines (sales/purchases) to partitioned parquet."""
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
                for row in lines
                if isinstance(row, dict)
            ]
            df = pl.DataFrame(normalized, schema=raw_schema)
            df = df.with_columns([
                pl.col('move_id').cast(pl.Int64, strict=False),
                pl.col('move_name').cast(pl.Utf8, strict=False),
                pl.col('move_date').cast(pl.Utf8, strict=False),
                pl.col('customer_id').cast(pl.Int64, strict=False),
                pl.col('customer_name').cast(pl.Utf8, strict=False),
                pl.col('vendor_id').cast(pl.Int64, strict=False),
                pl.col('vendor_name').cast(pl.Utf8, strict=False),
                pl.col('move_line_id').cast(pl.Int64, strict=False),
                pl.col('product_id').cast(pl.Int64, strict=False),
                pl.col('price_unit').cast(pl.Float64, strict=False),
                pl.col('quantity').cast(pl.Float64, strict=False),
                pl.col('tax_id').cast(pl.Int64, strict=False),
                pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]'),
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

        df = pl.scan_parquet(raw_file_path)

        if 'tax_id' not in df.schema:
            df = df.with_columns(pl.lit(None).cast(pl.Int64).alias('tax_id'))

        is_purchase = dataset_prefix == 'account_move_in_invoice_lines'

        df_clean = (
            df.with_columns([
                pl.col('move_id').cast(pl.Int64, strict=False),
                pl.col('move_name').cast(pl.Utf8, strict=False),
                pl.col('move_date').cast(pl.Utf8, strict=False),
                pl.col('customer_id').cast(pl.Int64, strict=False),
                pl.col('customer_name').cast(pl.Utf8, strict=False),
                pl.col('vendor_id').cast(pl.Int64, strict=False),
                pl.col('vendor_name').cast(pl.Utf8, strict=False),
                pl.col('move_line_id').cast(pl.Int64, strict=False),
                pl.col('product_id').cast(pl.Int64, strict=False),
                pl.col('price_unit').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('tax_id').cast(pl.Int64, strict=False),
                pl.col('tax_ids_json').cast(pl.Utf8, strict=False).fill_null('[]'),
            ])
            .with_columns([
                pl.when(pl.lit(is_purchase))
                .then((pl.col('price_unit') == 0) | (pl.col('quantity') == 0))
                .otherwise(pl.lit(False))
                .alias('is_free_item')
            ])
        )

        if is_purchase:
            df_clean = df_clean.with_columns(
                pl.col('vendor_name')
                .cast(pl.Utf8, strict=False)
                .fill_null('')
                .str.replace(r",.*$", "")
                .alias('vendor_name')
            )

            tax_dim_path = f'{STAR_SCHEMA_PATH}/dim_taxes.parquet'
            if os.path.isfile(tax_dim_path):
                taxes = (
                    pl.scan_parquet(tax_dim_path)
                    .select([
                        pl.col('tax_id').cast(pl.Int64, strict=False),
                        pl.col('tax_name').cast(pl.Utf8, strict=False),
                    ])
                    .unique(subset=['tax_id'], keep='last')
                )
                df_clean = df_clean.join(taxes, on='tax_id', how='left')
            else:
                df_clean = df_clean.with_columns(pl.lit(None).cast(pl.Utf8).alias('tax_name'))
        else:
            df_clean = df_clean.with_columns(pl.lit(None).cast(pl.Utf8).alias('tax_name'))

        year, month, day = target_date.split('-')
        clean_path = f'{clean_base_path}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)

        output_file = f'{clean_path}/{dataset_prefix}_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(), output_file)

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
def clean_pos_data(raw_file_path: Optional[str], target_date: str) -> Optional[str]:
    """Clean and validate POS data with lazy evaluation."""
    try:
        if not raw_file_path or not os.path.isfile(raw_file_path):
            logger.warning(f"Invalid file path: {raw_file_path}")
            return None
        
        # Lazy read for memory efficiency
        df = pl.scan_parquet(raw_file_path)
        
        df_clean = (
            df.filter(
                (pl.col('product_id').is_not_null()) &
                (pl.col('qty').is_not_null()) &
                (pl.col('qty') != 0) &
                (pl.col('price_subtotal_incl').is_not_null())
            )
            .with_columns([
                pl.col('order_date')
                    .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S')
                    .dt.replace_time_zone('UTC')
                    .dt.convert_time_zone(app.conf.timezone)
                    .dt.replace_time_zone(None),
                pl.col('order_id').cast(pl.Int64, strict=False),
                pl.col('order_ref').cast(pl.Utf8, strict=False),
                pl.col('pos_config_id').cast(pl.Int64, strict=False),
                pl.col('cashier_id').cast(pl.Int64, strict=False),
                pl.col('customer_id').cast(pl.Int64, strict=False),
                pl.col('amount_total').cast(pl.Float64, strict=False),
                pl.col('payment_method_ids').cast(pl.Utf8, strict=False).fill_null('[]'),
                pl.col('line_id').cast(pl.Int64, strict=False),
                pl.col('product_id').cast(pl.Int64),
                pl.col('qty').cast(pl.Float64),
                pl.col('price_subtotal_incl').cast(pl.Float64),
                pl.col('discount_amount').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('product_brand').fill_null('Unknown'),
                pl.col('product_category').fill_null('Unknown'),
                pl.col('product_parent_category').fill_null('Unknown'),
                pl.col('product_brand_id').cast(pl.Int64, strict=False),
            ])
        )
        
        year, month, day = target_date.split('-')
        clean_path = f'{CLEAN_PATH}/year={year}/month={month}/day={day}'
        os.makedirs(clean_path, exist_ok=True)
        
        output_file = f'{clean_path}/pos_order_lines_clean_{target_date}.parquet'
        atomic_write_parquet(df_clean.collect(), output_file)
        
        logger.info(f"Cleaned data saved to {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"Error cleaning data for {target_date}: {e}", exc_info=True)
        return None


def _update_fact_invoice_sales(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('move_date').alias('date'),
        pl.col('move_id'),
        pl.col('move_name'),
        pl.col('customer_id'),
        pl.col('customer_name'),
        pl.col('move_line_id'),
        pl.col('product_id'),
        pl.col('price_unit'),
        pl.col('quantity'),
        pl.col('tax_ids_json'),
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
        pl.col('move_id'),
        pl.col('move_name'),
        pl.col('vendor_id'),
        pl.col('vendor_name'),
        pl.col('move_line_id'),
        pl.col('product_id'),
        pl.col('price_unit'),
        pl.col('quantity'),
        pl.col('tax_id'),
        pl.col('tax_name'),
        pl.col('tax_ids_json'),
        pl.col('is_free_item').cast(pl.Boolean, strict=False).fill_null(False),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_purchases'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_purchases_{target_date}.parquet'
    atomic_write_parquet(fact_df, fact_output)

    return fact_output


def _update_fact_sales_pos(df: pl.DataFrame, target_date: str) -> str:
    fact_df = df.select([
        pl.col('order_date').alias('date'),
        pl.col('order_id'),
        pl.col('order_ref'),
        pl.col('pos_config_id'),
        pl.col('cashier_id'),
        pl.col('customer_id'),
        pl.col('payment_method_ids'),
        pl.col('line_id'),
        pl.col('product_id'),
        pl.col('qty').alias('quantity'),
        pl.col('price_subtotal_incl').alias('revenue'),
    ])

    fact_path = f'{STAR_SCHEMA_PATH}/fact_sales'
    os.makedirs(fact_path, exist_ok=True)

    fact_output = f'{fact_path}/fact_sales_{target_date}.parquet'
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

def _merge_dimension_table(new_df: pl.DataFrame, file_path: str, merge_key: str):
    """Efficiently merge dimension table with schema alignment."""
    try:
        if os.path.exists(file_path):
            existing_df = pl.read_parquet(file_path)
            
            all_cols = sorted(set(existing_df.columns) | set(new_df.columns))

            existing_schema = existing_df.schema
            new_schema = new_df.schema

            def _desired_dtype(col: str) -> pl.DataType:
                dtype = existing_schema.get(col)
                if dtype is not None and dtype != pl.Null:
                    return dtype
                dtype = new_schema.get(col)
                if dtype is not None and dtype != pl.Null:
                    return dtype
                return pl.Utf8

            missing_in_existing = [c for c in all_cols if c not in existing_df.columns]
            missing_in_new = [c for c in all_cols if c not in new_df.columns]

            if missing_in_existing:
                existing_df = existing_df.with_columns([
                    pl.lit(None, dtype=_desired_dtype(c)).alias(c)
                    for c in missing_in_existing
                ])

            if missing_in_new:
                new_df = new_df.with_columns([
                    pl.lit(None, dtype=_desired_dtype(c)).alias(c)
                    for c in missing_in_new
                ])

            cast_existing = []
            cast_new = []
            for c in all_cols:
                desired = _desired_dtype(c)
                if existing_df.schema.get(c) != desired:
                    cast_existing.append(pl.col(c).cast(desired, strict=False).alias(c))
                if new_df.schema.get(c) != desired:
                    cast_new.append(pl.col(c).cast(desired, strict=False).alias(c))

            if cast_existing:
                existing_df = existing_df.with_columns(cast_existing)
            if cast_new:
                new_df = new_df.with_columns(cast_new)

            existing_df = existing_df.select(all_cols)
            new_df = new_df.select(all_cols)

            merged_df = pl.concat([existing_df, new_df]).unique(
                subset=[merge_key], keep='last'
            )
        else:
            merged_df = new_df
        
        atomic_write_parquet(merged_df, file_path)
        
    except Exception as e:
        logger.error(f"Error merging dimension {file_path}: {e}", exc_info=True)
# DIMENSION REFRESH WITH INCREMENTAL UPDATES
# ============================================================================

@app.task
def refresh_dimensions_incremental(targets: Optional[List[str]] = None) -> Dict[str, Any]:
    """Incrementally refresh dimensions based on write_date (only changed records)."""
    valid_targets = {"products", "categories", "brands", "cashiers", "vendors", "taxes"}
    target_set = None
    
    if targets:
        target_set = {t.lower() for t in targets if t}
        invalid = target_set - valid_targets
        if invalid:
            raise ValueError(f"Invalid targets: {sorted(invalid)}")
    
    try:
        results = {}
        
        with get_pooled_odoo_connection() as odoo:
            if not target_set or 'products' in target_set:
                count = _refresh_products_incremental(odoo)
                results['products'] = count
            
            if not target_set or 'categories' in target_set:
                count = _refresh_categories_incremental(odoo)
                results['categories'] = count
            
            if not target_set or 'brands' in target_set:
                count = _refresh_brands_incremental(odoo)
                results['brands'] = count
            
            if not target_set or 'cashiers' in target_set:
                count = _refresh_cashiers_incremental(odoo)
                results['cashiers'] = count
            
            if not target_set or 'vendors' in target_set:
                count = _refresh_vendors_incremental(odoo)
                results['vendors'] = count

            if not target_set or 'taxes' in target_set:
                count = _refresh_taxes_incremental(odoo)
                results['taxes'] = count
        
        return {
            "updated": True,
            "method": "incremental",
            "targets": results
        }
        
    except Exception as e:
        logger.error(f"Error refreshing dimensions: {e}", exc_info=True)
        return {"updated": False, "error": str(e)}

def _refresh_cashiers_incremental(odoo) -> int:
    """Incrementally refresh cashiers dimension from hr.employee model."""
    last_sync = ETLMetadata.get_dimension_last_sync('cashiers')
    
    if 'hr.employee' not in odoo.env:
        logger.warning("hr.employee model not found")
        return 0
    
    Employee = odoo.env['hr.employee']
    
    domain = [
        ('job_id', 'in', ['Cashier', 'Team Leader']),
        ('active', '=', True)
    ]
    if last_sync:
        domain.append(('write_date', '>', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
    
    fields = ['id', 'name', 'job_id']
    employees = Employee.search_read(domain, fields)
    
    if not employees:
        logger.info("No cashier/employee changes to sync")
        return 0
    
    processed = []
    for emp in employees:
        job_id = safe_extract_m2o(emp.get('job_id'), get_id=False)
        
        processed.append({
            'id': emp['id'],
            'name': emp.get('name'),
            'job_id': job_id,
        })
    
    new_df = pl.DataFrame(processed)
    _merge_dimension_table(
        new_df,
        f'{STAR_SCHEMA_PATH}/dim_cashier.parquet',
        merge_key='id'
    )
    
    ETLMetadata.set_dimension_last_sync('cashiers', datetime.now())
    logger.info(f"Synced {len(employees)} cashiers/employees")
    
    return len(employees)

def _refresh_vendors_incremental(odoo) -> int:
    """Incrementally refresh vendors dimension from res.partner model."""
    last_sync = ETLMetadata.get_dimension_last_sync('vendors')

    if 'res.partner' not in odoo.env:
        logger.warning("res.partner model not found")
        return 0

    Partner = odoo.env['res.partner']

    domain = ['|', ('is_company', '=', True), ('supplier_rank', '>', 0)]
    if last_sync:
        domain.append(('write_date', '>', last_sync.strftime('%Y-%m-%d %H:%M:%S')))

    fields = ['id', 'complete_name', 'child_ids', 'user_id', 'write_date']
    vendors = Partner.search_read(domain, fields)

    if not vendors:
        logger.info("No vendor changes to sync")
        return 0

    all_child_ids: Set[int] = set()
    for vendor in vendors:
        all_child_ids.update(extract_o2m_ids(vendor.get('child_ids')))

    child_contacts: Dict[int, str] = {}
    if all_child_ids:
        child_records = Partner.search_read(
            [('id', 'in', list(all_child_ids))],
            ['id', 'name']
        )
        child_contacts = {child['id']: child.get('name') for child in child_records}

    processed = []
    for vendor in vendors:
        child_ids = extract_o2m_ids(vendor.get('child_ids'))
        contact_names = [str(child_contacts.get(child_id, '')) for child_id in child_ids]  # Ensure all values are strings
        
        processed.append({
            'vendor_id': int(vendor['id']),
            'name': str(vendor.get('complete_name', '')),
            'contact_ids': child_ids,
            'contact_names': contact_names,
            'salesperson_id': int(safe_extract_m2o(vendor.get('user_id'), get_id=True) or 0),
            'salesperson_name': str(safe_extract_m2o(vendor.get('user_id'), get_id=False) or ''),
            'write_date': str(vendor.get('write_date', '')),
        })

    # Create schema explicitly to ensure consistent types
    schema = {
        'vendor_id': pl.Int64,
        'name': pl.Utf8,
        'contact_ids': pl.List(pl.Int64),
        'contact_names': pl.List(pl.Utf8),
        'salesperson_id': pl.Int64,
        'salesperson_name': pl.Utf8,
        'write_date': pl.Utf8
    }
    
    new_df = pl.DataFrame(processed, schema=schema)
    _merge_dimension_table(
        new_df,
        f'{STAR_SCHEMA_PATH}/dim_vendors.parquet',
        merge_key='vendor_id'
    )

    ETLMetadata.set_dimension_last_sync('vendors', datetime.now())
    logger.info(f"Synced {len(vendors)} vendors")

    return len(vendors)

def _refresh_taxes_incremental(odoo) -> int:
    """Incrementally refresh taxes dimension from account.tax model."""
    last_sync = ETLMetadata.get_dimension_last_sync('taxes')

    if 'account.tax' not in odoo.env:
        logger.warning("account.tax model not found")
        return 0

    Tax = odoo.env['account.tax']

    domain = []
    if last_sync:
        domain.append(('write_date', '>', last_sync.strftime('%Y-%m-%d %H:%M:%S')))

    fields = ['id', 'name', 'write_date']
    taxes = Tax.search_read(domain, fields)

    if not taxes:
        logger.info("No tax changes to sync")
        return 0

    processed = [
        {
            'tax_id': int(tax['id']),
            'tax_name': str(tax.get('name', '') or ''),
            'write_date': str(tax.get('write_date', '') or ''),
        }
        for tax in taxes
        if tax and isinstance(tax.get('id'), int)
    ]

    schema = {
        'tax_id': pl.Int64,
        'tax_name': pl.Utf8,
        'write_date': pl.Utf8,
    }

    new_df = pl.DataFrame(processed, schema=schema)
    _merge_dimension_table(
        new_df,
        f'{STAR_SCHEMA_PATH}/dim_taxes.parquet',
        merge_key='tax_id'
    )

    ETLMetadata.set_dimension_last_sync('taxes', datetime.now())
    logger.info(f"Synced {len(processed)} taxes")

    return len(processed)

@app.task
def refresh_dimension_tables(start_date: Optional[str] = None, 
                            end_date: Optional[str] = None, 
                            targets: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Backward compatibility wrapper for refresh_dimensions_incremental.
    This maintains the old API while using the new optimized implementation.
    
    Args:
        start_date: Ignored (kept for backward compatibility)
        end_date: Ignored (kept for backward compatibility)
        targets: List of dimension tables to refresh: ["products", "categories", "brands"]
    """
    logger.info("refresh_dimension_tables called (using incremental method)")
    return refresh_dimensions_incremental(targets=targets)

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
def date_range_etl_pipeline(start_date: str, end_date: Optional[str] = None) -> Dict[str, Any]:
    """Process date range in parallel."""
    if end_date is None:
        end_date = start_date
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    logger.info(f"Starting parallel ETL for {start_date} to {end_date}")
    
    # Generate date range
    delta = end_dt - start_dt
    date_range = [
        (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(delta.days + 1)
    ]
    
    # Process in parallel
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
    
    # Calculate missing dates
    delta = today - last_processed
    if delta.days > 1:
        logger.info(f"Catching up {delta.days - 1} days")
        
        # Process missing dates in parallel
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
        
        # Auto-trigger catch-up
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

from celery.schedules import crontab

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
    'incremental-dimension-refresh': {
        'task': 'etl_tasks.refresh_dimensions_incremental',
        'schedule': crontab(hour='*/4', minute=0),  # Every 4 hours
    },
    'health-check': {
        'task': 'etl_tasks.health_check',
        'schedule': crontab(hour='*/6', minute=0),  # Every 6 hours
    },
}

# Task routing for better resource allocation
app.conf.task_routes = {
    'etl_tasks.extract_pos_order_lines': {'queue': 'extraction'},
    'etl_tasks.extract_sales_invoice_lines': {'queue': 'extraction'},
    'etl_tasks.extract_purchase_invoice_lines': {'queue': 'extraction'},
    'etl_tasks.clean_pos_data': {'queue': 'transformation'},
    'etl_tasks.clean_sales_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.clean_purchase_invoice_lines': {'queue': 'transformation'},
    'etl_tasks.update_star_schema': {'queue': 'loading'},
    'etl_tasks.update_invoice_sales_star_schema': {'queue': 'loading'},
    'etl_tasks.update_purchase_star_schema': {'queue': 'loading'},
    'etl_tasks.save_raw_sales_invoice_lines': {'queue': 'loading'},
    'etl_tasks.save_raw_purchase_invoice_lines': {'queue': 'loading'},
    'etl_tasks.refresh_dimensions_incremental': {'queue': 'dimensions'},
}