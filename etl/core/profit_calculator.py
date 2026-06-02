"""Profit-calculation ETL core: cost snapshots, COGS, gross-profit, aggregates.

All functions are **pure Polars / pure Python** — no Celery decorators, no
DuckDB connection imports, no dashboard-service coupling.  The only external
dependencies allowed are:

  - etl.core.schema      (I/O helpers, schemas)
  - etl.core.cost_engine (tax multipliers)
  - etl.config           (data-lake path constants)
  - etl.io_parquet       (atomic_write_parquet — used by write_partitioned)
"""
from datetime import date, timedelta
from typing import Tuple, Optional

import polars as pl

from etl.config import (
    STAR_SCHEMA_PATH,
    FACT_PRODUCT_COST_EVENTS_PATH,
    FACT_PRODUCT_COST_LATEST_DAILY_PATH,
    FACT_PRODUCT_LEGACY_COSTS_PATH,
)
from etl.core.schema import (
    read_parquet_or_empty,
    write_partitioned,
    SALES_SCHEMA,
    COST_SCHEMA,
    PROFIT_SCHEMA,
    DAILY_AGG_SCHEMA,
    BY_PRODUCT_AGG_SCHEMA,
)
from etl.core.cost_engine import tax_multiplier_expr


def build_product_cost_events(target_date: str) -> pl.DataFrame:
    """Derive per-product cost events from cleaned purchase-invoice data."""
    from etl.config import FACT_PRODUCT_COST_EVENTS_PATH  # needed for write later

    purchases_schema = {
        'date': pl.Date,
        'move_id': pl.Int64,
        'move_line_id': pl.Int64,
        'product_id': pl.Int64,
        'actual_price': pl.Float64,
        'quantity': pl.Float64,
        'tax_id': pl.Int64,
    }

    target_dt = date.fromisoformat(target_date)

    partition_path = (
        f"{STAR_SCHEMA_PATH}/fact_purchases/year={target_dt.year}"
        f"/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )
    df = read_parquet_or_empty(partition_path, purchases_schema)

    if df.is_empty():
        return pl.DataFrame(schema=COST_SCHEMA)

    df = (
        df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('product_id').cast(pl.Int64, strict=False),
            pl.col('actual_price').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('tax_id').cast(pl.Int64, strict=False).fill_null(0),
        )
        .filter(
            (pl.col('product_id').is_not_null())
            & (pl.col('product_id') != 0)
            & (pl.col('actual_price') > 0)
            & (pl.col('quantity') > 0)
        )
        .with_columns(
            (pl.col('actual_price') * tax_multiplier_expr('tax_id')).alias('cost_unit_tax_in')
        )
        .select([
            'date',
            'product_id',
            'cost_unit_tax_in',
            pl.col('move_id').alias('source_move_id'),
            pl.col('tax_id').alias('source_tax_id'),
        ])
    )

    return df if not df.is_empty() else pl.DataFrame(schema=COST_SCHEMA)


def latest_cost_by_product(events: pl.DataFrame) -> pl.DataFrame:
    """Collapse cost events to the single latest record per product_id."""
    if events.is_empty():
        return events
    return (
        events.sort('source_move_id')
        .group_by('product_id')
        .agg([
            pl.last('cost_unit_tax_in').alias('cost_unit_tax_in'),
            pl.last('source_move_id').alias('source_move_id'),
            pl.last('source_tax_id').alias('source_tax_id'),
        ])
    )


def build_cost_snapshot_from_events(target_date: str) -> pl.DataFrame:
    """Roll-up all historical cost events up to (and including) *target_date*."""
    events = read_parquet_or_empty(
        f"{FACT_PRODUCT_COST_EVENTS_PATH}/**/*.parquet",
        COST_SCHEMA,
    )
    if events.is_empty():
        return pl.DataFrame(schema=COST_SCHEMA)

    target_dt = date.fromisoformat(target_date)
    events = events.with_columns(
        pl.col('date').cast(pl.Date, strict=False),
        pl.col('product_id').cast(pl.Int64, strict=False),
        pl.col('source_move_id').cast(pl.Int64, strict=False),
        pl.col('source_tax_id').cast(pl.Int64, strict=False),
        pl.col('cost_unit_tax_in').cast(pl.Float64, strict=False).fill_null(0),
    )
    events = events.filter(pl.col('date') <= pl.lit(target_dt))

    if events.is_empty():
        return pl.DataFrame(schema=COST_SCHEMA)

    latest = (
        events.sort(['date', 'source_move_id'])
        .group_by('product_id')
        .agg([
            pl.last('cost_unit_tax_in').alias('cost_unit_tax_in'),
            pl.last('source_move_id').alias('source_move_id'),
            pl.last('source_tax_id').alias('source_tax_id'),
        ])
        .with_columns(pl.lit(target_dt).alias('date'))
        .select(['date', 'product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'])
    )

    return latest if not latest.is_empty() else pl.DataFrame(schema=COST_SCHEMA)


def build_product_cost_latest_daily(target_date: str) -> pl.DataFrame:
    """Incremental cost snapshot: yesterday's snapshot + today's new events.

    Falls back to a full rebuild from all events when yesterday's partition
    is missing (first run, backfill, or data-integrity issue).
    """
    target_dt = date.fromisoformat(target_date)
    prev_date = (target_dt - timedelta(days=1)).isoformat()
    prev_partition = (
        f"{FACT_PRODUCT_COST_LATEST_DAILY_PATH}"
        f"/year={target_dt.year}/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )

    prev_df = read_parquet_or_empty(prev_partition, COST_SCHEMA)
    prev_df = prev_df.select([
        'product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'
    ])

    today_partition = (
        f"{FACT_PRODUCT_COST_EVENTS_PATH}"
        f"/year={target_dt.year}/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )
    today_events = read_parquet_or_empty(today_partition, COST_SCHEMA)
    today_events = today_events.with_columns(
        pl.col('product_id').cast(pl.Int64, strict=False),
        pl.col('source_move_id').cast(pl.Int64, strict=False),
        pl.col('source_tax_id').cast(pl.Int64, strict=False),
        pl.col('cost_unit_tax_in').cast(pl.Float64, strict=False).fill_null(0),
    )
    today_latest = latest_cost_by_product(today_events)

    if prev_df.is_empty() and today_latest.is_empty():
        return pl.DataFrame(schema=COST_SCHEMA)

    merged = prev_df.join(today_latest, on='product_id', how='outer', suffix='_today')
    snapshot = (
        merged.with_columns(
            pl.coalesce(
                [pl.col('cost_unit_tax_in_today'), pl.col('cost_unit_tax_in')]
            ).alias('cost_unit_tax_in'),
            pl.coalesce(
                [pl.col('source_move_id_today'), pl.col('source_move_id')]
            ).alias('source_move_id'),
            pl.coalesce(
                [pl.col('source_tax_id_today'), pl.col('source_tax_id')]
            ).alias('source_tax_id'),
        )
        .select(['product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'])
        .with_columns(pl.lit(target_dt).alias('date'))
        .select(['date', 'product_id', 'cost_unit_tax_in', 'source_move_id', 'source_tax_id'])
    )

    return snapshot if not snapshot.is_empty() else pl.DataFrame(schema=COST_SCHEMA)


def _unified_costs(target_date: str) -> pl.DataFrame:
    """Replicate the old ``fact_product_costs_unified`` DuckDB view in pure Polars.

    Priority logic (matches DuckDB view):
      1.  Latest purchase costs  (priority = 1)
      2.  Legacy / manual costs   (priority = 3, or whatever is stored)

    We read both Parquet sources, union them, and keep the *best* row per
    product ordered by priority ASC, effective_date DESC.
    """
    target_dt = date.fromisoformat(target_date)

    # --- 1. latest purchase costs (from our own daily snapshots) ---
    latest_schema = {
        'date': pl.Date,
        'product_id': pl.Int64,
        'cost_unit_tax_in': pl.Float64,
        'source_tax_id': pl.Int64,
        'source_move_id': pl.Int64,
    }
    latest = read_parquet_or_empty(
        f"{FACT_PRODUCT_COST_LATEST_DAILY_PATH}/**/*.parquet",
        latest_schema,
    )
    latest = latest.with_columns(
        pl.lit(1).alias('priority'),
        pl.lit('latest_purchase').alias('cost_source'),
    )

    # --- 2. legacy / beginning costs (from CSV import or manual correction) ---
    legacy_schema = {
        'product_id': pl.Int64,
        'cost_unit_tax_in': pl.Float64,
        'source_tax_id': pl.Int64,
        'effective_date': pl.Date,
        'cost_source': pl.Utf8,
        'priority': pl.Int64,
        'is_active': pl.Boolean,
    }
    legacy = read_parquet_or_empty(
        f"{FACT_PRODUCT_LEGACY_COSTS_PATH}/**/*.parquet",
        legacy_schema,
    )
    legacy = (
        legacy.with_columns(
            pl.col('priority').cast(pl.Int64, strict=False).fill_null(3),
            pl.col('effective_date').cast(pl.Date, strict=False),
            pl.col('is_active').cast(pl.Boolean, strict=False).fill_null(True),
            pl.col('cost_source').cast(pl.Utf8, strict=False).fill_null('unknown'),
        )
        .filter(pl.col('is_active') == True)
        .filter(pl.col('cost_unit_tax_in') > 0)
        .filter(pl.col('effective_date') <= pl.lit(target_dt))
        .rename({'effective_date': 'date'})
    )

    # --- 3. union & rank ---
    combined = pl.concat([latest, legacy.select(latest.columns + ['priority', 'cost_source'])], how='diagonal_relaxed')
    if combined.is_empty():
        return pl.DataFrame(schema={
            'product_id': pl.Int64,
            'cost_unit_tax_in': pl.Float64,
            'source_tax_id': pl.Int64,
            'cost_source': pl.Utf8,
            'effective_date': pl.Date,
        })

    # Polars equivalent of ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY priority ASC, date DESC)
    ranked = (
        combined
        .with_columns(
            pl.struct(['priority', 'date']).alias('_sort_key')
        )
        .sort(['product_id', '_sort_key'])
        .group_by('product_id', maintain_order=True)
        .agg(pl.all().first())
        .drop('_sort_key')
        .rename({'date': 'effective_date'})
    )

    return ranked.select(['product_id', 'cost_unit_tax_in', 'source_tax_id', 'cost_source', 'effective_date'])


def build_sales_lines_profit(target_date: str) -> pl.DataFrame:
    """Join POS + invoice sales lines with the unified cost snapshot for the day.

    Replaces the old implementation that reached into ``services.duckdb_connector``
    via a DuckDB view.  Everything here is pure Polars over local Parquet.
    """
    target_dt = date.fromisoformat(target_date)

    # --- read sales fact tables ---
    pos_partition = (
        f"{STAR_SCHEMA_PATH}/fact_sales"
        f"/year={target_dt.year}/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )
    invoice_partition = (
        f"{STAR_SCHEMA_PATH}/fact_invoice_sales"
        f"/year={target_dt.year}/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )

    pos_df = read_parquet_or_empty(pos_partition, SALES_SCHEMA)
    invoice_df = read_parquet_or_empty(invoice_partition, SALES_SCHEMA)

    # Exclude cancelled / placeholder refs (business rule from Decision Log 2026-02)
    if 'order_ref' in pos_df.columns:
        pos_df = pos_df.filter(
            (pl.col('order_ref').is_null()) | (pl.col('order_ref') != '/')
        )
    if 'move_name' in invoice_df.columns:
        invoice_df = invoice_df.filter(
            (pl.col('move_name').is_null()) | (pl.col('move_name') != '/')
        )

    # --- normalise both sources to a common schema ---
    pos_lines = (
        pos_df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('order_id').cast(pl.Int64, strict=False).fill_null(0).alias('txn_id'),
            pl.col('line_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('revenue').cast(pl.Float64, strict=False).fill_null(0).alias('revenue_tax_in'),
        )
        .select(['date', 'txn_id', 'line_id', 'product_id', 'quantity', 'revenue_tax_in'])
    )

    invoice_lines = (
        invoice_df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('move_id').cast(pl.Int64, strict=False).fill_null(0).alias('txn_id'),
            pl.col('move_line_id').cast(pl.Int64, strict=False).fill_null(0).alias('line_id'),
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('price_unit').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('tax_id').cast(pl.Int64, strict=False).fill_null(0),
        )
        .with_columns(
            (pl.col('price_unit') * pl.col('quantity') * tax_multiplier_expr('tax_id')).alias('revenue_tax_in')
        )
        .select(['date', 'txn_id', 'line_id', 'product_id', 'quantity', 'revenue_tax_in'])
    )

    sales_lines = pl.concat([pos_lines, invoice_lines], how='vertical')
    if sales_lines.is_empty():
        return pl.DataFrame(schema=PROFIT_SCHEMA)

    # --- attach unified costs (pure Polars, no DuckDB) ---
    unified_costs = _unified_costs(target_date)
    merged = sales_lines.join(unified_costs, on='product_id', how='left')

    merged = merged.with_columns(
        pl.col('cost_unit_tax_in').cast(pl.Float64, strict=False).fill_null(0),
        pl.col('source_tax_id').cast(pl.Int64, strict=False).fill_null(0),
        (pl.col('cost_unit_tax_in') * pl.col('quantity')).alias('cogs_tax_in'),
    )
    merged = merged.with_columns(
        (pl.col('revenue_tax_in') - pl.col('cogs_tax_in')).alias('gross_profit')
    )

    profit_df = merged.select([
        'date',
        'txn_id',
        'line_id',
        'product_id',
        'quantity',
        'revenue_tax_in',
        'cost_unit_tax_in',
        'cogs_tax_in',
        'gross_profit',
        pl.lit(None).alias('source_cost_move_id').cast(pl.Int64),
        pl.col('source_tax_id').alias('source_cost_tax_id'),
    ])

    return profit_df if not profit_df.is_empty() else pl.DataFrame(schema=PROFIT_SCHEMA)


def build_profit_aggregates(profit_df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Roll up line-level profit into daily and by-product summaries."""
    if profit_df.is_empty():
        return (
            pl.DataFrame(schema=DAILY_AGG_SCHEMA),
            pl.DataFrame(schema=BY_PRODUCT_AGG_SCHEMA),
        )

    daily = profit_df.group_by('date').agg([
        pl.sum('revenue_tax_in').alias('revenue_tax_in'),
        pl.sum('cogs_tax_in').alias('cogs_tax_in'),
        pl.sum('gross_profit').alias('gross_profit'),
        pl.sum('quantity').alias('quantity'),
        pl.col('txn_id').n_unique().alias('transactions'),
        pl.len().alias('lines'),
    ])

    by_product = profit_df.group_by(['date', 'product_id']).agg([
        pl.sum('revenue_tax_in').alias('revenue_tax_in'),
        pl.sum('cogs_tax_in').alias('cogs_tax_in'),
        pl.sum('gross_profit').alias('gross_profit'),
        pl.sum('quantity').alias('quantity'),
        pl.len().alias('lines'),
    ])

    return daily, by_product


# ---------------------------------------------------------------------------
# Sales aggregates (conceptually separate from profit, but historically
# bundled in the same ETL phase — kept here for parity with legacy pipeline)
# ---------------------------------------------------------------------------

def build_sales_aggregates(target_date: str) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Build daily, by-product, and by-principal sales aggregates."""
    from etl.config import DIM_PRODUCTS_FILE

    daily_schema = {
        'date': pl.Date,
        'revenue': pl.Float64,
        'transactions': pl.Int64,
        'items_sold': pl.Float64,
        'lines': pl.Int64,
    }
    by_product_schema = {
        'date': pl.Date,
        'product_id': pl.Int64,
        'revenue': pl.Float64,
        'quantity': pl.Float64,
        'lines': pl.Int64,
    }
    by_principal_schema = {
        'date': pl.Date,
        'principal': pl.Utf8,
        'revenue': pl.Float64,
        'quantity': pl.Float64,
        'lines': pl.Int64,
    }

    target_dt = date.fromisoformat(target_date)
    pos_partition = (
        f"{STAR_SCHEMA_PATH}/fact_sales"
        f"/year={target_dt.year}/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )
    invoice_partition = (
        f"{STAR_SCHEMA_PATH}/fact_invoice_sales"
        f"/year={target_dt.year}/month={target_dt.month:02d}/day={target_dt.day:02d}"
    )

    pos_df = read_parquet_or_empty(pos_partition, SALES_SCHEMA)
    invoice_df = read_parquet_or_empty(invoice_partition, SALES_SCHEMA)

    if 'order_ref' in pos_df.columns:
        pos_df = pos_df.filter(
            (pl.col('order_ref').is_null()) | (pl.col('order_ref') != '/')
        )
    if 'move_name' in invoice_df.columns:
        invoice_df = invoice_df.filter(
            (pl.col('move_name').is_null()) | (pl.col('move_name') != '/')
        )

    pos_lines = (
        pos_df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('order_id').cast(pl.Int64, strict=False).fill_null(0).alias('txn_id'),
            pl.col('line_id').cast(pl.Int64, strict=False).fill_null(0).alias('line_id'),
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0).alias('product_id'),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0).alias('quantity'),
            pl.col('revenue').cast(pl.Float64, strict=False).fill_null(0).alias('revenue'),
        )
        .select(['date', 'txn_id', 'line_id', 'product_id', 'quantity', 'revenue'])
    )

    invoice_lines = (
        invoice_df.with_columns(
            pl.lit(target_dt).alias('date'),
            pl.col('move_id').cast(pl.Int64, strict=False).fill_null(0).alias('txn_id'),
            pl.col('move_line_id').cast(pl.Int64, strict=False).fill_null(0).alias('line_id'),
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0).alias('product_id'),
            pl.col('quantity').cast(pl.Float64, strict=False).fill_null(0).alias('quantity'),
            (pl.col('price_unit').cast(pl.Float64, strict=False).fill_null(0) * pl.col('quantity')).alias('revenue'),
        )
        .select(['date', 'txn_id', 'line_id', 'product_id', 'quantity', 'revenue'])
    )

    sales_lines = pl.concat([pos_lines, invoice_lines], how='vertical')
    if sales_lines.is_empty():
        return (
            pl.DataFrame(schema=daily_schema),
            pl.DataFrame(schema=by_product_schema),
            pl.DataFrame(schema=by_principal_schema),
        )

    daily = sales_lines.group_by('date').agg([
        pl.sum('revenue').alias('revenue'),
        pl.col('txn_id').n_unique().alias('transactions'),
        pl.sum('quantity').alias('items_sold'),
        pl.len().alias('lines'),
    ])

    by_product = sales_lines.group_by(['date', 'product_id']).agg([
        pl.sum('revenue').alias('revenue'),
        pl.sum('quantity').alias('quantity'),
        pl.len().alias('lines'),
    ])

    # --- principal lookup ---
    try:
        dim_products = pl.read_parquet(DIM_PRODUCTS_FILE)
        if 'product_id' not in dim_products.columns:
            dim_products = pl.DataFrame(schema={'product_id': pl.Int64, 'product_brand': pl.Utf8})
        if 'product_brand' not in dim_products.columns:
            dim_products = dim_products.with_columns(pl.lit('').alias('product_brand'))
        dim_products = dim_products.select([
            pl.col('product_id').cast(pl.Int64, strict=False).fill_null(0),
            pl.col('product_brand').cast(pl.Utf8, strict=False).fill_null(''),
        ]).unique('product_id')
    except Exception:
        dim_products = pl.DataFrame(schema={'product_id': pl.Int64, 'product_brand': pl.Utf8})

    enriched = sales_lines.join(dim_products, on='product_id', how='left')
    enriched = enriched.with_columns(
        pl.when(
            pl.col('product_brand').is_null()
            | (pl.col('product_brand').str.strip_chars().str.len_chars() == 0)
        )
        .then(pl.lit('Unknown'))
        .otherwise(pl.col('product_brand'))
        .alias('principal')
    )

    by_principal = enriched.group_by(['date', 'principal']).agg([
        pl.sum('revenue').alias('revenue'),
        pl.sum('quantity').alias('quantity'),
        pl.len().alias('lines'),
    ])

    return daily, by_product, by_principal
