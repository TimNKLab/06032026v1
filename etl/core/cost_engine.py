"""Cost-engine primitives: tax-adjustment rules and validation helpers.

Zero framework dependencies — used by both ETL transform/load and by
unit tests in isolation.
"""
import logging
from typing import Optional

import polars as pl

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


# ---------------------------------------------------------------------------
# Pre-baked tax-multiplier constants for use in contexts without Polars
# ---------------------------------------------------------------------------

TAX_NO_ADJUSTMENT = {5, 2}
TAX_VAT_INCLUSIVE = {7, 6}   # 11% VAT → multiplier 1.11
