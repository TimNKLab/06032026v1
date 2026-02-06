"""Daily pipeline implementations."""
from datetime import date
from typing import Optional

from etl.metadata import ETLMetadata
import logging

logger = logging.getLogger(__name__)


def daily_etl_pipeline_impl(target_date: Optional[str] = None) -> str:
    """Optimized daily ETL pipeline implementation."""
    from etl_tasks import app
    
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting ETL pipeline for {target_date}")

    # Import here to avoid circular imports
    from etl_tasks import (
        extract_pos_order_lines, save_raw_data, clean_pos_data, update_star_schema
    )
    
    # Create the chain
    pipeline = (
        extract_pos_order_lines.s(target_date) |
        save_raw_data.s() |
        clean_pos_data.s(target_date) |
        update_star_schema.s(target_date)
    )

    result = pipeline.apply_async()
    logger.info(f"ETL pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


def daily_invoice_sales_pipeline_impl(target_date: Optional[str] = None) -> str:
    """Daily pipeline for invoice-based sales (out_invoice) implementation."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting invoice sales pipeline for {target_date}")
    
    from etl_tasks import (
        extract_sales_invoice_lines, save_raw_sales_invoice_lines,
        clean_sales_invoice_lines, update_invoice_sales_star_schema
    )
    
    pipeline = (
        extract_sales_invoice_lines.s(target_date) |
        save_raw_sales_invoice_lines.s() |
        clean_sales_invoice_lines.s(target_date) |
        update_invoice_sales_star_schema.s(target_date)
    )
    
    result = pipeline.apply_async()
    logger.info(f"Invoice sales pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


def daily_invoice_purchases_pipeline_impl(target_date: Optional[str] = None) -> str:
    """Daily pipeline for purchases (vendor bills, in_invoice) implementation."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting purchases pipeline for {target_date}")
    
    from etl_tasks import (
        extract_purchase_invoice_lines, save_raw_purchase_invoice_lines,
        clean_purchase_invoice_lines, update_purchase_star_schema
    )
    
    pipeline = (
        extract_purchase_invoice_lines.s(target_date) |
        save_raw_purchase_invoice_lines.s() |
        clean_purchase_invoice_lines.s(target_date) |
        update_purchase_star_schema.s(target_date)
    )
    
    result = pipeline.apply_async()
    logger.info(f"Purchases pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


def daily_inventory_moves_pipeline_impl(target_date: Optional[str] = None) -> str:
    """Daily pipeline for inventory moves (stock.move.line) implementation."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting inventory moves pipeline for {target_date}")
    
    from etl_tasks import (
        refresh_dimensions_incremental, extract_inventory_moves,
        save_raw_inventory_moves, clean_inventory_moves,
        update_inventory_moves_star_schema
    )
    
    pipeline = (
        refresh_dimensions_incremental.si([
            'products', 'locations', 'uoms', 'partners', 'users', 'companies', 'lots'
        ]) |
        extract_inventory_moves.si(target_date) |
        save_raw_inventory_moves.s() |
        clean_inventory_moves.s(target_date) |
        update_inventory_moves_star_schema.s(target_date)
    )
    
    result = pipeline.apply_async()
    logger.info(f"Inventory moves pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


def daily_stock_quants_pipeline_impl(target_date: Optional[str] = None) -> str:
    """Daily pipeline for stock quant snapshots (stock.quant) implementation."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting stock quants pipeline for {target_date}")

    from etl_tasks import (
        refresh_dimensions_incremental, extract_stock_quants,
        save_raw_stock_quants, clean_stock_quants,
        update_stock_quants_star_schema,
    )

    pipeline = (
        refresh_dimensions_incremental.si([
            'products', 'locations', 'lots'
        ]) |
        extract_stock_quants.si(target_date) |
        save_raw_stock_quants.s() |
        clean_stock_quants.s(target_date) |
        update_stock_quants_star_schema.s(target_date)
    )

    result = pipeline.apply_async()
    logger.info(f"Stock quants pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id


def daily_profit_pipeline_impl(target_date: Optional[str] = None) -> str:
    """Daily pipeline for cost/profit materialization."""
    if target_date is None:
        target_date = date.today().isoformat()

    logger.info(f"Starting profit pipeline for {target_date}")

    from etl_tasks import (
        update_product_cost_events, update_product_cost_latest_daily,
        update_sales_lines_profit, update_profit_aggregates,
    )

    pipeline = (
        update_product_cost_events.si(target_date) |
        update_product_cost_latest_daily.si(target_date) |
        update_sales_lines_profit.si(target_date) |
        update_profit_aggregates.si(target_date)
    )

    result = pipeline.apply_async()
    logger.info(f"Profit pipeline submitted for {target_date}, task_id: {result.id}")
    return result.id
