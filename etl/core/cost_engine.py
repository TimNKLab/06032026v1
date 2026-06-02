"""Cost-engine primitives: tax-adjustment rules and validation helpers.  
  
Zero framework dependencies — used by both ETL transform/load and by  
unit tests in isolation.  
"""  
import logging  
import os
from datetime import date, datetime
from typing import Optional, Dict, Any  
  
import polars as pl  
  
from etl.config import FACT_PRODUCT_BEGINNING_COSTS_PATH
from etl.io_parquet import atomic_write_parquet

logger = logging.getLogger(__name__)  
  
  
def tax_multiplier_expr(tax_col: str) -> pl.Expr:  
    """Polars expression that maps Odoo tax IDs to inclusive-cost multipliers.  
  
    Rules (as documented in Decision Log 2026-02):  
      tax_id in {5, 2}  → 1.00x  (no adjustment)  
      tax_id in {7, 6}  → 1.11x  (11% VAT inclusive)  
      anything else     → 1.00x  
    """  
    return (  
        pl.when(pl.col(tax_col).is_in([5, 2])).then(1.0)  
        .when(pl.col(tax_col).is_in([7, 6])).then(1.11)  
        .otherwise(1.0)  
    )  
  
  
def validate_beginning_costs(df: pl.DataFrame) -> pl.DataFrame:  
    """Validate beginning-costs CSV before it enters the pipeline.  
  
    Raises ValueError on fatal data-quality issues so bad seed data never  
    pollutes downstream profit calculations.  
    """  
    if not df.filter(pl.col('product_id').is_null() | (pl.col('product_id') <= 0)).is_empty():  
        raise ValueError("Invalid product_id found: must be positive integer")  
  
    if not df.filter(pl.col('cost_unit').is_null() | (pl.col('cost_unit') < 0)).is_empty():  
        raise ValueError("Invalid cost_unit found: must be non-negative")  
  
    if not df.filter(pl.col('cost_unit') > 10_000_000).is_empty():  
        logger.warning("Some costs exceed 10M — please verify these are correct")  
  
    return df  
  
def load_beginning_costs_from_csv(csv_path: str) -> Optional[str]:
    """Load beginning costs from CSV file and save as Parquet."""
    try:
        logger.info(f"Loading beginning costs from {csv_path}")
        
        # Read CSV
        df = pl.read_csv(csv_path)
        
        # Validate required columns
        required_cols = ['product_id', 'cost_unit', 'purchase_tax_id']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Cast and validate data
        df = df.with_columns([
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('cost_unit').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('purchase_tax_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('notes').cast(pl.Utf8, strict=False).fill_null(''),
        ])
        
        # Validate data quality
        df = validate_beginning_costs(df)
        
        # Apply tax multipliers to get tax-inclusive costs
        df = df.with_columns([
            (pl.col('cost_unit') * tax_multiplier_expr('purchase_tax_id')).alias('cost_unit_tax_in'),
        ])
        
        # Build final schema
        beginning_costs_df = df.with_columns([
            pl.col('product_id'),
            pl.col('cost_unit_tax_in'),
            pl.col('purchase_tax_id').alias('source_tax_id'),
            pl.lit(date(2025, 2, 10)).alias('effective_date'),
            pl.lit(True).alias('is_active'),
            pl.lit(datetime.now()).alias('created_at'),
            pl.col('notes'),
        ]).select([
            'product_id',
            'cost_unit_tax_in', 
            'source_tax_id',
            'effective_date',
            'is_active',
            'created_at',
            'notes',
        ])
        
        # Write to parquet
        output_path = f'{FACT_PRODUCT_BEGINNING_COSTS_PATH}/beginning_costs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        atomic_write_parquet(beginning_costs_df, output_path)
        
        logger.info(f"Successfully loaded {len(beginning_costs_df)} beginning costs to {output_path}")
        return output_path
        
    except Exception as exc:
        logger.error(f"Error loading beginning costs from {csv_path}: {exc}", exc_info=True)
        raise

# ---------------------------------------------------------------------------  
# Pre-baked tax-multiplier constants for use in contexts without Polars  
# ---------------------------------------------------------------------------  
  
TAX_NO_ADJUSTMENT = {5, 2}  
TAX_VAT_INCLUSIVE = {7, 6}   # 11% VAT → multiplier 1.11  
