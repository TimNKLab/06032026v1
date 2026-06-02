"""Task Registry: Maps string identifiers to modular ETL functions.

This module acts as the central 'glue' for the ETL engine. The scheduler 
and Admin UI use these string keys to trigger specific atomic operations 
without needing to know the internal module structure.

No Celery decorators here. Just pure Python function mapping.
"""
from typing import Any, Callable, Dict

from etl.core import cost_engine, profit_calculator, schema
from etl.extract import dimensions, invoices, pos, stock_quants, inventory_moves
from etl.load import raw, star_schema
from etl.transform import inventory as transform_inventory, invoices as transform_invoices, pos as transform_pos

# ---------------------------------------------------------------------------
# Task Registry
# ---------------------------------------------------------------------------

TASK_REGISTRY: Dict[str, Callable] = {
    # --- Extraction & Raw Save ---
    'save_raw_data': raw.save_raw_data,
    'save_raw_sales_invoice_lines': raw.save_raw_sales_invoice_lines,
    'save_raw_purchase_invoice_lines': raw.save_raw_purchase_invoice_lines,
    'save_raw_inventory_moves': raw.save_raw_inventory_moves,
    'save_raw_stock_quants': raw.save_raw_stock_quants,
    'refresh_dimensions': dimensions.refresh_dimensions_incremental,
    'load_beginning_costs': cost_engine.load_beginning_costs_from_csv,

    # --- Transformation (Cleaning) ---
    'clean_pos_data': transform_pos.clean_pos_data,
    'clean_sales_invoice_lines': transform_invoices.clean_sales_invoice_lines,
    'clean_purchase_invoice_lines': transform_invoices.clean_purchase_invoice_lines,
    'clean_stock_quants': transform_inventory.clean_stock_quants,
    'clean_inventory_moves': transform_inventory.clean_inventory_moves,

    # --- Loading (Star Schema) ---
    'update_star_schema': star_schema.update_fact_sales_pos, # Default to POS
    'update_invoice_sales_star_schema': star_schema.update_fact_invoice_sales,
    'update_purchase_star_schema': star_schema.update_fact_purchases,
    'update_inventory_moves_star_schema': star_schema.update_fact_inventory_moves,
    'update_stock_quants_star_schema': star_schema.update_fact_stock_on_hand_snapshot,

    # --- Core Logic (Profit/Costs) ---
    'update_product_cost_events': profit_calculator.build_product_cost_events,
    'update_product_cost_latest_daily': profit_calculator.build_product_cost_latest_daily,
    'update_sales_lines_profit': profit_calculator.build_sales_lines_profit,
    'update_profit_aggregates': profit_calculator.build_profit_aggregates,
    'update_sales_aggregates': profit_calculator.build_sales_aggregates,
}


def get_task(name: str) -> Callable:
    """Retrieve a task function by its registered name."""
    if name not in TASK_REGISTRY:
        raise KeyError(f"Task '{name}' is not registered in TASK_REGISTRY.")
    return TASK_REGISTRY[name]

def list_registered_tasks() -> list[str]:
    """Return a list of all available task keys."""
    return list(TASK_REGISTRY.keys())
