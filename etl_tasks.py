import os
import polars as pl
from datetime import date, timedelta
from celery import Celery
from odoorpc_connector import get_odoo_connection, retry_odoo

# Redis connection
redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
app = Celery('etl_tasks', broker=redis_url, backend=redis_url)

# Data lake paths
DATA_LAKE_ROOT = '/app/data-lake'
RAW_PATH = f'{DATA_LAKE_ROOT}/raw/pos_order_lines'
CLEAN_PATH = f'{DATA_LAKE_ROOT}/clean/pos_order_lines'
STAR_SCHEMA_PATH = f'{DATA_LAKE_ROOT}/star-schema'

@app.task
@retry_odoo(max_retries=3, delay=2)
def extract_pos_order_lines(target_date: str):
    """Extract POS order lines from Odoo for a specific date."""
    target_dt = date.fromisoformat(target_date)
    
    odoo = get_odoo_connection()
    if odoo is None:
        raise Exception("Failed to connect to Odoo")
    
    # Query POS order lines
    start_dt = target_dt.replace(hour=0, minute=0, second=0)
    end_dt = target_dt.replace(hour=23, minute=59, second=59)
    
    domain = [
        ('x_studio_order_date', '>=', start_dt.strftime('%Y-%m-%d %H:%M:%S')),
        ('x_studio_order_date', '<=', end_dt.strftime('%Y-%m-%d %H:%M:%S')),
    ]
    
    fields = [
        'x_studio_order_date',
        'product_id', 
        'qty',
        'price_subtotal_incl',
    ]
    
    PosOrderLine = odoo.env['pos.order.line']
    lines = PosOrderLine.search_read(domain, fields)
    
    # Get product details
    product_ids = {
        line['product_id'][0]
        for line in lines
        if isinstance(line.get('product_id'), (list, tuple)) and line['product_id']
    }
    
    category_by_product = {}
    brand_by_product = {}
    if product_ids and 'product.product' in odoo.env:
        Product = odoo.env['product.product']
        products = Product.read(list(product_ids), ['categ_id', 'x_studio_brand_id'])
        category_by_product = {
            prod['id']: prod.get('categ_id')
            for prod in products
        }
        brand_by_product = {
            prod['id']: prod.get('x_studio_brand_id')
            for prod in products
        }
    
    # Process lines
    processed_lines = []
    for line in lines:
        product = line.get('product_id')
        product_id = product[0] if isinstance(product, (list, tuple)) and product else None
        
        # Extract category info
        categ_value = category_by_product.get(product_id)
        brand_value = brand_by_product.get(product_id)
        
        # Parse category hierarchy
        categ_name = None
        if isinstance(categ_value, (list, tuple)) and len(categ_value) >= 2:
            categ_name = categ_value[1]
        elif isinstance(categ_value, str):
            categ_name = categ_value
            
        parent_category = None
        leaf_category = None
        if isinstance(categ_name, str):
            segments = [segment.strip() for segment in categ_name.split('/') if segment.strip()]
            if segments:
                parent_category = segments[0]
                leaf_category = segments[-1]
        
        # Extract brand name
        brand_name = None
        if isinstance(brand_value, (list, tuple)) and len(brand_value) >= 2:
            brand_name = brand_value[1]
        elif isinstance(brand_value, str):
            brand_name = brand_value
        
        processed_line = {
            'order_date': line['x_studio_order_date'],
            'product_id': product_id,
            'qty': float(line['qty']),
            'price_subtotal_incl': float(line['price_subtotal_incl']),
            'product_parent_category': parent_category,
            'product_category': leaf_category,
            'product_brand': brand_name or 'Unknown',
        }
        processed_lines.append(processed_line)
    
    return processed_lines

@app.task
def save_raw_data(lines: list, target_date: str):
    """Save raw POS data to Parquet files."""
    if not lines:
        return
    
    # Create date-partitioned path
    year, month, day = target_date.split('-')
    partition_path = f'{RAW_PATH}/year={year}/month={month}/day={day}'
    os.makedirs(partition_path, exist_ok=True)
    
    # Convert to Polars and save
    df = pl.DataFrame(lines)
    output_file = f'{partition_path}/pos_order_lines_{target_date}.parquet'
    df.write_parquet(output_file)
    
    return output_file

@app.task
def clean_pos_data(raw_file_path: str, target_date: str):
    """Clean and validate POS data."""
    # Read raw data
    df = pl.read_parquet(raw_file_path)
    
    # Data cleaning
    df_clean = df.filter(
        (pl.col('product_id').is_not_null()) &
        (pl.col('qty') > 0) &
        (pl.col('price_subtotal_incl') > 0)
    ).with_columns([
        pl.col('order_date').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S'),
        pl.col('qty').cast(pl.Float64),
        pl.col('price_subtotal_incl').cast(pl.Float64),
        pl.col('product_id').cast(pl.Int64),
    ]).with_columns([
        pl.col('product_category').fill_null('Unknown'),
        pl.col('product_parent_category').fill_null('Unknown'),
        pl.col('product_brand').fill_null('Unknown'),
    ])
    
    # Save cleaned data
    year, month, day = target_date.split('-')
    clean_path = f'{CLEAN_PATH}/year={year}/month={month}/day={day}'
    os.makedirs(clean_path, exist_ok=True)
    
    output_file = f'{clean_path}/pos_order_lines_clean_{target_date}.parquet'
    df_clean.write_parquet(output_file)
    
    return output_file

@app.task
def update_star_schema(clean_file_path: str, target_date: str):
    """Update star schema tables with cleaned data."""
    # Read cleaned data
    df = pl.read_parquet(clean_file_path)
    
    # Update fact_sales
    fact_df = df.select([
        pl.col('order_date').alias('date'),
        pl.col('product_id'),
        pl.col('qty').alias('quantity'),
        pl.col('price_subtotal_incl').alias('revenue'),
    ])
    
    fact_path = f'{STAR_SCHEMA_PATH}/fact_sales'
    os.makedirs(fact_path, exist_ok=True)
    
    # Append to fact table
    fact_output = f'{fact_path}/fact_sales_{target_date}.parquet'
    fact_df.write_parquet(fact_output)
    
    # Update dimensions
    _update_dimensions(df)
    
    return fact_output

def _update_dimensions(df: pl.DataFrame):
    """Update dimension tables using single files (not subdirectories)."""
    # dim_products - single file approach
    products_df = df.select([
        'product_id',
        'product_category',
        'product_parent_category', 
        'product_brand'
    ]).unique()
    
    dim_products_path = f'{STAR_SCHEMA_PATH}/dim_products.parquet'
    
    # Check if file exists and merge/append
    if os.path.exists(dim_products_path):
        existing_products = pl.read_parquet(dim_products_path)
        # Combine new and existing, remove duplicates
        products_df = pl.concat([existing_products, products_df]).unique()
    
    products_df.write_parquet(dim_products_path)
    
    # dim_categories - single file approach
    categories_df = df.select([
        'product_category',
        'product_parent_category'
    ]).unique().filter(
        pl.col('product_category').is_not_null()
    )
    
    dim_categories_path = f'{STAR_SCHEMA_PATH}/dim_categories.parquet'
    if os.path.exists(dim_categories_path):
        existing_categories = pl.read_parquet(dim_categories_path)
        categories_df = pl.concat([existing_categories, categories_df]).unique()
    
    categories_df.write_parquet(dim_categories_path)
    
    # dim_brands - single file approach
    brands_df = df.select([
        'product_brand'
    ]).unique().filter(
        pl.col('product_brand').is_not_null()
    )
    
    dim_brands_path = f'{STAR_SCHEMA_PATH}/dim_brands.parquet'
    if os.path.exists(dim_brands_path):
        existing_brands = pl.read_parquet(dim_brands_path)
        brands_df = pl.concat([existing_brands, brands_df]).unique()
    
    brands_df.write_parquet(dim_brands_path)

@app.task
def daily_etl_pipeline(target_date: str = None):
    """Complete daily ETL pipeline."""
    if target_date is None:
        target_date = date.today().isoformat()
    
    print(f"Starting ETL pipeline for {target_date}")
    
    # Extract
    lines = extract_pos_order_lines.delay(target_date)
    
    # Transform and Load
    save_raw_task = save_raw_data.s(lines.get(), target_date)
    clean_task = clean_pos_data.s(target_date)
    update_task = update_star_schema.s(target_date)
    
    # Chain the tasks
    chain = save_raw_task | clean_task | update_task
    result = chain()
    
    print(f"ETL pipeline completed for {target_date}")
    return result

# Schedule daily ETL at 2 AM
from celery.schedules import crontab
app.conf.beat_schedule = {
    'daily-etl': {
        'task': 'etl_tasks.daily_etl_pipeline',
        'schedule': crontab(hour=2, minute=0),
    },
}
