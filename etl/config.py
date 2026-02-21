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
RAW_STOCK_QUANTS_PATH = f'{DATA_LAKE_ROOT}/raw/stock_quants'
CLEAN_INVENTORY_MOVES_PATH = f'{DATA_LAKE_ROOT}/clean/inventory_moves'
CLEAN_STOCK_QUANTS_PATH = f'{DATA_LAKE_ROOT}/clean/stock_quants'
STAR_SCHEMA_PATH = f'{DATA_LAKE_ROOT}/star-schema'
METADATA_PATH = f'{DATA_LAKE_ROOT}/metadata'

FACT_PRODUCT_COST_EVENTS_PATH = f'{STAR_SCHEMA_PATH}/fact_product_cost_events'
FACT_PRODUCT_COST_LATEST_DAILY_PATH = f'{STAR_SCHEMA_PATH}/fact_product_cost_latest_daily'
FACT_PRODUCT_BEGINNING_COSTS_PATH = f'{STAR_SCHEMA_PATH}/fact_product_beginning_costs'
FACT_PRODUCT_LEGACY_COSTS_PATH = f'{STAR_SCHEMA_PATH}/fact_product_legacy_costs'
FACT_PRODUCT_COSTS_UNIFIED_PATH = f'{STAR_SCHEMA_PATH}/fact_product_costs_unified'
FACT_SALES_LINES_PROFIT_PATH = f'{STAR_SCHEMA_PATH}/fact_sales_lines_profit'
AGG_PROFIT_DAILY_PATH = f'{STAR_SCHEMA_PATH}/agg_profit_daily'
AGG_PROFIT_DAILY_BY_PRODUCT_PATH = f'{STAR_SCHEMA_PATH}/agg_profit_daily_by_product'

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
        RAW_STOCK_QUANTS_PATH, CLEAN_INVENTORY_MOVES_PATH, CLEAN_STOCK_QUANTS_PATH,
        STAR_SCHEMA_PATH, METADATA_PATH,
        FACT_PRODUCT_COST_EVENTS_PATH, FACT_PRODUCT_COST_LATEST_DAILY_PATH,
        FACT_PRODUCT_BEGINNING_COSTS_PATH, FACT_PRODUCT_LEGACY_COSTS_PATH,
        FACT_PRODUCT_COSTS_UNIFIED_PATH, FACT_SALES_LINES_PROFIT_PATH, 
        AGG_PROFIT_DAILY_PATH, AGG_PROFIT_DAILY_BY_PRODUCT_PATH,
    ]:
        os.makedirs(path, exist_ok=True)

# Ensure directories are created when module is imported
ensure_directories()
