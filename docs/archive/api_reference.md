# NKDash API Reference

This document provides comprehensive API documentation for NKDash, including ETL tasks, services, and data models.

## Table of Contents
- [ETL Tasks API](#etl-tasks-api)
- [Services API](#services-api)
- [Data Models](#data-models)
- [DuckDB Views](#duckdb-views)
- [Configuration API](#configuration-api)
- [Examples](#examples)

---

## ETL Tasks API

### Core ETL Tasks

#### Extraction Tasks

##### `extract_pos_order_lines`
**Description**: Extract POS order data from Odoo for a specific date

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.extract_pos_order_lines')
def extract_pos_order_lines(self, target_date: str) -> dict
```

**Parameters**:
- `target_date` (str): Date in YYYY-MM-DD format

**Returns**:
```python
{
    'status': 'success',
    'records_extracted': int,
    'target_date': str,
    'execution_time': float
}
```

**Example**:
```python
from etl_tasks import extract_pos_order_lines
result = extract_pos_order_lines('2025-02-21')
print(result)
# {'status': 'success', 'records_extracted': 1247, 'target_date': '2025-02-21', 'execution_time': 12.3}
```

##### `extract_sales_invoice_lines`
**Description**: Extract customer invoice data from Odoo

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.extract_sales_invoice_lines')
def extract_sales_invoice_lines(self, target_date: str) -> dict
```

**Parameters**:
- `target_date` (str): Date in YYYY-MM-DD format

**Returns**: Same structure as `extract_pos_order_lines`

##### `extract_purchase_invoice_lines`
**Description**: Extract vendor bill data from Odoo

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.extract_purchase_invoice_lines')
def extract_purchase_invoice_lines(self, target_date: str) -> dict
```

##### `extract_inventory_moves`
**Description**: Extract inventory movement data from Odoo

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.extract_inventory_moves')
def extract_inventory_moves(self, target_date: str) -> dict
```

#### Transformation Tasks

##### `clean_pos_data`
**Description**: Clean and transform raw POS data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.clean_pos_data')
def clean_pos_data(self, target_date: str) -> dict
```

**Transformations Applied**:
- Date normalization
- Currency conversion
- Product reference resolution
- Payment method classification
- Discount calculation

##### `clean_sales_invoice_lines`
**Description**: Clean and transform invoice sales data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.clean_sales_invoice_lines')
def clean_sales_invoice_lines(self, target_date: str) -> dict
```

##### `clean_purchase_invoice_lines`
**Description**: Clean and transform purchase data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.clean_purchase_invoice_lines')
def clean_purchase_invoice_lines(self, target_date: str) -> dict
```

**Special Features**:
- `actual_price` calculation with invoice-level discounts
- Tax classification and multiplier application
- Bonus item exclusion logic

##### `clean_inventory_moves`
**Description**: Clean and transform inventory movement data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.clean_inventory_moves')
def clean_inventory_moves(self, target_date: str) -> dict
```

**Movement Classifications**:
- `incoming`: Stock receipts
- `outgoing`: Sales/shipments
- `internal`: Transfers
- `adjustment`: Manual adjustments
- `scrap`: Damaged goods
- `production`: Manufacturing

#### Loading Tasks

##### `update_star_schema`
**Description**: Load cleaned POS data into star schema

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.update_star_schema')
def update_star_schema(self, target_date: str) -> dict
```

**Target Tables**:
- `fact_sales` (partitioned by date)
- `dim_products` (incremental merge)
- `dim_categories` (incremental merge)
- `dim_brands` (incremental merge)

##### `update_invoice_sales_star_schema`
**Description**: Load invoice sales data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.update_invoice_sales_star_schema')
def update_invoice_sales_star_schema(self, target_date: str) -> dict
```

##### `update_purchase_star_schema`
**Description**: Load purchase data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.update_purchase_star_schema')
def update_purchase_star_schema(self, target_date: str) -> dict
```

##### `update_inventory_moves_star_schema`
**Description**: Load inventory movement data

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.update_inventory_moves_star_schema')
def update_inventory_moves_star_schema(self, target_date: str) -> dict
```

#### Orchestration Tasks

##### `daily_etl_pipeline`
**Description**: Complete daily ETL pipeline for all datasets

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.daily_etl_pipeline')
def daily_etl_pipeline(self, target_date: str = None) -> dict
```

**Parameters**:
- `target_date` (str, optional): Date to process, defaults to yesterday

**Pipeline Steps**:
1. Extract POS data
2. Extract invoice data
3. Extract purchase data
4. Extract inventory data
5. Clean all datasets
6. Load to star schema
7. Refresh dimensions
8. Update DuckDB views

**Example**:
```python
from etl_tasks import daily_etl_pipeline
result = daily_etl_pipeline('2025-02-21')
print(result)
# {'status': 'success', 'datasets_processed': 4, 'total_records': 5234, 'execution_time': 245.7}
```

##### `date_range_etl_pipeline`
**Description**: Process ETL for a date range

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.date_range_etl_pipeline')
def date_range_etl_pipeline(self, start_date: str, end_date: str) -> dict
```

##### `catch_up_etl`
**Description**: Fill missing data gaps

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.catch_up_etl')
def catch_up_etl(self, days_back: int = 30) -> dict
```

##### `health_check`
**Description**: System health and data freshness check

**Signature**:
```python
@app.task(bind=True, name='etl_tasks.health_check')
def health_check(self) -> dict
```

**Returns**:
```python
{
    'status': 'healthy',
    'latest_data_date': '2025-02-21',
    'services_up': ['redis', 'celery-worker', 'dash-app'],
    'data_lake_status': 'accessible',
    'duckdb_views': 'accessible'
}
```

---

## Services API

### Sales Metrics Service

#### `services/sales_metrics.py`

##### `query_sales_summary`
**Description**: Get sales summary for date range

**Signature**:
```python
def query_sales_summary(start_date: str, end_date: str) -> dict
```

**Parameters**:
- `start_date` (str): Start date in YYYY-MM-DD format
- `end_date` (str): End date in YYYY-MM-DD format

**Returns**:
```python
{
    'total_revenue': float,
    'total_orders': int,
    'total_items': int,
    'avg_order_value': float,
    'unique_customers': int,
    'top_products': List[dict],
    'daily_breakdown': List[dict]
}
```

**Example**:
```python
from services.sales_metrics import query_sales_summary
summary = query_sales_summary('2025-02-01', '2025-02-28')
print(f"Revenue: ${summary['total_revenue']:,.2f}")
print(f"Orders: {summary['total_orders']:,}")
```

##### `query_product_performance`
**Description**: Get product performance metrics

**Signature**:
```python
def query_product_performance(start_date: str, end_date: str, limit: int = 100) -> List[dict]
```

**Returns**:
```python
[
    {
        'product_id': str,
        'product_name': str,
        'category': str,
        'brand': str,
        'revenue': float,
        'quantity': int,
        'orders': int,
        'avg_price': float
    },
    ...
]
```

### Inventory Metrics Service

#### `services/inventory_metrics.py`

##### `query_stock_levels`
**Description**: Get current stock levels

**Signature**:
```python
def query_stock_levels(as_of_date: str = None, category_filter: str = None) -> List[dict]
```

**Returns**:
```python
[
    {
        'product_id': str,
        'product_name': str,
        'on_hand_qty': int,
        'reserved_qty': int,
        'available_qty': int,
        'days_of_cover': float,
        'status': str,  # 'normal', 'low_stock', 'out_of_stock'
        'last_updated': str
    },
    ...
]
```

##### `query_sell_through_analysis`
**Description**: Calculate sell-through ratios

**Signature**:
```python
def query_sell_through_analysis(start_date: str, end_date: str) -> dict
```

**Returns**:
```python
{
    'overall_sell_through': float,
    'category_breakdown': List[dict],
    'top_performers': List[dict],
    'underperformers': List[dict]
}
```

##### `query_abc_analysis`
**Description**: Perform ABC analysis by revenue

**Signature**:
```python
def query_abc_analysis(start_date: str, end_date: str, 
                      a_threshold: float = 0.2, b_threshold: float = 0.5) -> dict
```

**Returns**:
```python
{
    'class_a': List[dict],
    'class_b': List[dict],
    'class_c': List[dict],
    'summary': {
        'a_revenue_share': float,
        'b_revenue_share': float,
        'c_revenue_share': float,
        'a_sku_count': int,
        'b_sku_count': int,
        'c_sku_count': int
    }
}
```

### Profit Metrics Service

#### `services/profit_metrics.py`

##### `query_profit_trends`
**Description**: Get profit trends over time

**Signature**:
```python
@cache.memoize(timeout=600)
def query_profit_trends(start_date: str, end_date: str) -> List[dict]
```

**Returns**:
```python
[
    {
        'date': str,
        'revenue': float,
        'cogs': float,
        'gross_profit': float,
        'margin_pct': float,
        'units_sold': int
    },
    ...
]
```

##### `query_profit_by_product`
**Description**: Get profit analysis by product

**Signature**:
```python
@cache.memoize(timeout=600)
def query_profit_by_product(start_date: str, end_date: str, limit: int = 50) -> List[dict]
```

**Returns**:
```python
[
    {
        'product_id': str,
        'product_name': str,
        'revenue': float,
        'cogs': float,
        'gross_profit': float,
        'margin_pct': float,
        'quantity': int,
        'profit_per_unit': float
    },
    ...
]
```

##### `query_profit_summary`
**Description**: Get overall profit summary

**Signature**:
```python
@cache.memoize(timeout=600)
def query_profit_summary(start_date: str, end_date: str) -> dict
```

**Returns**:
```python
{
    'total_revenue': float,
    'total_cogs': float,
    'gross_profit': float,
    'overall_margin': float,
    'total_units': int,
    'avg_margin_per_unit': float,
    'top_margin_products': List[dict],
    'low_margin_products': List[dict]
}
```

---

## Data Models

### Fact Tables

#### `fact_sales`
**Description**: POS sales transactions

**Schema**:
```python
{
    'date': date,
    'order_id': str,
    'customer_id': str,
    'product_id': str,
    'quantity': int,
    'revenue': float,
    'price_unit': float,
    'discount_amount': float,
    'payment_methods': List[str],
    'created_at': timestamp
}
```

#### `fact_invoice_sales`
**Description**: Customer invoice sales

**Schema**:
```python
{
    'date': date,
    'invoice_id': str,
    'customer_id': str,
    'product_id': str,
    'quantity': int,
    'revenue': float,
    'tax_amount': float,
    'tax_id': int,
    'created_at': timestamp
}
```

#### `fact_purchases`
**Description**: Vendor purchases

**Schema**:
```python
{
    'date': date,
    'invoice_id': str,
    'vendor_id': str,
    'product_id': str,
    'quantity': int,
    'price_unit': float,
    'actual_price': float,
    'discount_pct': float,
    'tax_amount': float,
    'tax_id': int,
    'created_at': timestamp
}
```

#### `fact_inventory_moves`
**Description**: Inventory movements

**Schema**:
```python
{
    'date': date,
    'move_id': str,
    'product_id': str,
    'location_id': str,
    'qty_moved': int,
    'movement_type': str,
    'reference': str,
    'created_at': timestamp
}
```

#### `fact_sales_lines_profit`
**Description**: Profit analysis at sales line level

**Schema**:
```python
{
    'date': date,
    'sales_line_id': str,
    'product_id': str,
    'quantity': int,
    'revenue_tax_in': float,
    'cost_unit_tax_in': float,
    'cogs_tax_in': float,
    'gross_profit': float,
    'margin_pct': float,
    'source_cost_move_id': str,
    'created_at': timestamp
}
```

### Dimension Tables

#### `dim_products`
**Schema**:
```python
{
    'product_id': str,
    'product_name': str,
    'default_code': str,
    'category_id': str,
    'brand_id': str,
    'active': bool,
    'sale_ok': bool,
    'purchase_ok': bool,
    'created_at': timestamp,
    'updated_at': timestamp
}
```

#### `dim_categories`
**Schema**:
```python
{
    'category_id': str,
    'category_name': str,
    'parent_id': str,
    'level': int,
    'active': bool,
    'created_at': timestamp,
    'updated_at': timestamp
}
```

#### `dim_brands`
**Schema**:
```python
{
    'brand_id': str,
    'brand_name': str,
    'active': bool,
    'created_at': timestamp,
    'updated_at': timestamp
}
```

---

## DuckDB Views

### Sales Views

#### `fact_sales_all`
**Description**: Unified view of all sales data (POS + invoices)

**SQL**:
```sql
CREATE OR REPLACE VIEW fact_sales_all AS
SELECT 
    date,
    order_id as transaction_id,
    customer_id,
    product_id,
    quantity,
    revenue,
    'POS' as source_type
FROM fact_sales

UNION ALL

SELECT 
    date,
    invoice_id as transaction_id,
    customer_id,
    product_id,
    quantity,
    revenue,
    'INVOICE' as source_type
FROM fact_invoice_sales;
```

### Profit Views

#### `agg_profit_daily`
**Description**: Daily profit aggregates

**SQL**:
```sql
CREATE OR REPLACE VIEW agg_profit_daily AS
SELECT 
    date,
    SUM(revenue_tax_in) as revenue,
    SUM(cogs_tax_in) as cogs,
    SUM(gross_profit) as gross_profit,
    SUM(gross_profit) / NULLIF(SUM(revenue_tax_in), 0) as margin_pct,
    SUM(quantity) as units_sold,
    COUNT(DISTINCT product_id) as products_sold
FROM fact_sales_lines_profit
GROUP BY date
ORDER BY date;
```

#### `agg_profit_daily_by_product`
**Description**: Daily profit by product

**SQL**:
```sql
CREATE OR REPLACE VIEW agg_profit_daily_by_product AS
SELECT 
    date,
    product_id,
    SUM(revenue_tax_in) as revenue,
    SUM(cogs_tax_in) as cogs,
    SUM(gross_profit) as gross_profit,
    SUM(gross_profit) / NULLIF(SUM(revenue_tax_in), 0) as margin_pct,
    SUM(quantity) as units_sold
FROM fact_sales_lines_profit
GROUP BY date, product_id
ORDER BY date, gross_profit DESC;
```

---

## Configuration API

### Environment Variables

#### Odoo Configuration
```python
# odoorpc_connector.py
class OdooConfig:
    host: str = os.getenv('ODOO_HOST')
    port: int = int(os.getenv('ODOO_PORT', 443))
    protocol: str = os.getenv('ODOO_PROTOCOL', 'jsonrpc+ssl')
    database: str = os.getenv('ODOO_DB')
    username: str = os.getenv('ODOO_USERNAME')
    api_key: str = os.getenv('ODOO_API_KEY')
```

#### Data Lake Configuration
```python
# etl/config.py
class DataLakeConfig:
    root_path: Path = Path(os.getenv('DATA_LAKE_ROOT', '/data-lake'))
    raw_path: Path = root_path / 'raw'
    clean_path: Path = root_path / 'clean'
    star_schema_path: Path = root_path / 'star-schema'
    metadata_path: Path = root_path / 'metadata'
```

#### Performance Configuration
```python
# app.py
class PerformanceConfig:
    cache_ttl_seconds: int = int(os.getenv('DASH_CACHE_TTL_SECONDS', 600))
    celery_worker_concurrency: int = int(os.getenv('CELERY_WORKER_CONCURRENCY', 4))
    task_soft_time_limit: int = int(os.getenv('CELERY_TASK_SOFT_TIME_LIMIT', 1800))
    task_time_limit: int = int(os.getenv('CELERY_TASK_TIME_LIMIT', 1900))
```

### Celery Configuration

#### Queue Routing
```python
# etl_tasks.py
CELERY_TASK_ROUTES = {
    'etl_tasks.extract_*': {'queue': 'extract'},
    'etl_tasks.clean_*': {'queue': 'transform'},
    'etl_tasks.update_*': {'queue': 'load'},
    'etl_tasks.daily_*': {'queue': 'orchestration'},
    'etl_tasks.date_range_*': {'queue': 'orchestration'},
    'etl_tasks.catch_up_*': {'queue': 'orchestration'},
    'etl_tasks.health_check': {'queue': 'orchestration'},
}
```

#### Beat Schedule
```python
CELERY_BEAT_SCHEDULE = {
    'daily-etl': {
        'task': 'etl_tasks.daily_etl_pipeline',
        'schedule': crontab(hour=2, minute=0),  # 02:00 daily
    },
    'health-check': {
        'task': 'etl_tasks.health_check',
        'schedule': crontab(minute=0),  # Hourly
    },
}
```

---

## Examples

### Complete ETL Pipeline Example

```python
#!/usr/bin/env python3
"""
Example: Run complete ETL pipeline for a specific date
"""

from etl_tasks import daily_etl_pipeline
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_daily_etl():
    """Run ETL for yesterday's data"""
    try:
        result = daily_etl_pipeline()
        
        logger.info(f"ETL Status: {result['status']}")
        logger.info(f"Datasets processed: {result['datasets_processed']}")
        logger.info(f"Total records: {result['total_records']}")
        logger.info(f"Execution time: {result['execution_time']:.2f}s")
        
        if result['status'] == 'success':
            logger.info("ETL pipeline completed successfully")
        else:
            logger.error(f"ETL pipeline failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"ETL pipeline error: {e}")

if __name__ == "__main__":
    run_daily_etl()
```

### Sales Analysis Example

```python
#!/usr/bin/env python3
"""
Example: Analyze sales performance
"""

from services.sales_metrics import query_sales_summary, query_product_performance
from services.profit_metrics import query_profit_summary
import pandas as pd

def analyze_monthly_sales(year: int, month: int):
    """Analyze sales for a specific month"""
    start_date = f"{year}-{month:02d}-01"
    
    # Get end date of month
    if month == 12:
        end_date = f"{year+1}-01-01"
    else:
        end_date = f"{year}-{month+1:02d}-01"
    
    # Sales summary
    sales_summary = query_sales_summary(start_date, end_date)
    
    # Product performance
    top_products = query_product_performance(start_date, end_date, limit=10)
    
    # Profit analysis
    profit_summary = query_profit_summary(start_date, end_date)
    
    # Create DataFrame for analysis
    df_products = pd.DataFrame(top_products)
    
    print(f"Sales Analysis for {year}-{month:02d}")
    print("=" * 50)
    print(f"Total Revenue: ${sales_summary['total_revenue']:,.2f}")
    print(f"Total Orders: {sales_summary['total_orders']:,}")
    print(f"Average Order Value: ${sales_summary['avg_order_value']:.2f}")
    print(f"Gross Profit: ${profit_summary['gross_profit']:,.2f}")
    print(f"Overall Margin: {profit_summary['overall_margin']:.2%}")
    print()
    print("Top 10 Products:")
    for i, product in enumerate(top_products[:5], 1):
        print(f"{i}. {product['product_name']}: ${product['revenue']:,.2f}")

if __name__ == "__main__":
    analyze_monthly_sales(2025, 2)
```

### Inventory Analysis Example

```python
#!/usr/bin/env python3
"""
Example: Inventory analysis and alerts
"""

from services.inventory_metrics import query_stock_levels, query_sell_through_analysis, query_abc_analysis

def inventory_health_check():
    """Check inventory health and generate alerts"""
    
    # Current stock levels
    stock_levels = query_stock_levels()
    
    # Sell-through analysis (last 30 days)
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    sell_through = query_sell_through_analysis(start_date, end_date)
    
    # ABC analysis (last 90 days)
    start_date_abc = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    abc_analysis = query_abc_analysis(start_date_abc, end_date)
    
    # Generate alerts
    alerts = []
    
    # Low stock alerts
    for item in stock_levels:
        if item['status'] == 'low_stock':
            alerts.append(f"LOW STOCK: {item['product_name']} - {item['on_hand_qty']} units")
        elif item['status'] == 'out_of_stock':
            alerts.append(f"OUT OF STOCK: {item['product_name']}")
    
    # Poor sell-through alerts
    if sell_through['overall_sell_through'] < 0.5:
        alerts.append(f"LOW SELL-THROUGH: {sell_through['overall_sell_through']:.1%}")
    
    # Print results
    print("Inventory Health Check")
    print("=" * 40)
    print(f"Total Products: {len(stock_levels)}")
    print(f"Low Stock Items: {len([s for s in stock_levels if s['status'] == 'low_stock'])}")
    print(f"Out of Stock Items: {len([s for s in stock_levels if s['status'] == 'out_of_stock'])}")
    print(f"Overall Sell-through: {sell_through['overall_sell_through']:.1%}")
    print()
    
    if alerts:
        print("ALERTS:")
        for alert in alerts:
            print(f"⚠️  {alert}")
    else:
        print("✅ No inventory alerts")

if __name__ == "__main__":
    inventory_health_check()
```

### Custom Query Example

```python
#!/usr/bin/env python3
"""
Example: Custom DuckDB queries
"""

from services.duckdb_connector import get_duckdb_connection

def custom_sales_analysis():
    """Perform custom sales analysis"""
    
    conn = get_duckdb_connection()
    
    # Custom query: Top customers by revenue
    query = """
    SELECT 
        customer_id,
        COUNT(DISTINCT transaction_id) as order_count,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_order_value,
        COUNT(DISTINCT product_id) as unique_products
    FROM fact_sales_all
    WHERE date >= '2025-01-01'
    GROUP BY customer_id
    HAVING total_revenue > 1000
    ORDER BY total_revenue DESC
    LIMIT 20
    """
    
    result = conn.execute(query).fetchall()
    
    print("Top 20 Customers by Revenue (YTD)")
    print("=" * 50)
    print(f"{'Customer ID':<15} {'Orders':<8} {'Revenue':<12} {'Avg Order':<10} {'Products':<10}")
    print("-" * 65)
    
    for row in result:
        customer_id, order_count, revenue, avg_order, products = row
        print(f"{customer_id:<15} {order_count:<8} ${revenue:<11,.2f} ${avg_order:<9,.2f} {products:<10}")

if __name__ == "__main__":
    custom_sales_analysis()
```

---

## Error Handling

### Common Exceptions

#### `ETLTaskError`
```python
class ETLTaskError(Exception):
    """Base exception for ETL task errors"""
    pass

class DataExtractionError(ETLTaskError):
    """Raised when data extraction fails"""
    pass

class DataTransformationError(ETLTaskError):
    """Raised when data transformation fails"""
    pass

class DataLoadingError(ETLTaskError):
    """Raised when data loading fails"""
    pass
```

#### Example Error Handling
```python
from etl_tasks import extract_pos_order_lines, DataExtractionError

def safe_extract_data(target_date: str):
    """Extract data with error handling"""
    try:
        result = extract_pos_order_lines(target_date)
        return result
    except DataExtractionError as e:
        logger.error(f"Data extraction failed for {target_date}: {e}")
        # Implement retry logic or fallback
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
```

---

## Performance Considerations

### Caching Strategy
- Use `@cache.memoize()` for frequently accessed data
- Set appropriate TTL values (600s default)
- Clear cache when data is updated

### Query Optimization
- Use date predicates in all queries
- Prefer aggregate tables for summary data
- Limit result sets with SQL LIMIT clauses
- Use partition pruning for date-range queries

### Memory Management
- Process data in batches for large datasets
- Use generators for data streaming
- Monitor memory usage in long-running tasks

---

*This API reference should be updated as the system evolves.*  
*For implementation details, see ETL_GUIDE.md*  
*For troubleshooting, see TROUBLESHOOTING.md*  
*Last updated: 2026-02-21*
