"""ETL configuration constants and environment parsing."""
import os
from typing import Dict

# ============================================================================
# CONSTANTS
# ============================================================================

MAX_RETRIES = 3
RETRY_DELAY = 5
ODOO_BATCH_SIZE = 500
PARQUET_COMPRESSION = 'zstd'
CONNECTION_TIMEOUT = 300
CACHE_TTL = 3600

# ============================================================================
# DATA LAKE PATHS
# ============================================================================

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

# ============================================================================
# PATH INITIALIZATION
# ============================================================================

def ensure_directories() -> None:
    """Create all required data lake directories."""
    for path in [
        RAW_PATH, CLEAN_PATH, RAW_SALES_INVOICE_PATH, RAW_PURCHASES_PATH,
        CLEAN_SALES_INVOICE_PATH, CLEAN_PURCHASES_PATH, RAW_INVENTORY_MOVES_PATH,
        CLEAN_INVENTORY_MOVES_PATH, STAR_SCHEMA_PATH, METADATA_PATH,
    ]:
        os.makedirs(path, exist_ok=True)

# Ensure directories are created when module is imported
ensure_directories()
