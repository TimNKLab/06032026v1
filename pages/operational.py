import os
import redis
import logging
from enum import Enum
from collections import Counter
from typing import Any
import dash
from dash import dcc
import dash_mantine_components as dmc
import dash_ag_grid as dag
from datetime import date, timedelta

# Configure logging for ETL operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from services.etl_ops import (
    scan_dataset_partitions,
    scan_dimension_files,
    parse_date,
)
from services.profit_metrics import clear_profit_caches
from scripts.etl_data_manager import get_mv_scanner, MV_TO_PARQUET_MAP
from etl_tasks import (
    app,
    force_refresh_day,
    extract_pos_order_lines,
    save_raw_data,
    clean_pos_data,
    update_star_schema,
    extract_sales_invoice_lines,
    save_raw_sales_invoice_lines,
    clean_sales_invoice_lines,
    update_invoice_sales_star_schema,
    extract_purchase_invoice_lines,
    save_raw_purchase_invoice_lines,
    clean_purchase_invoice_lines,
    update_purchase_star_schema,
    refresh_dimensions_incremental,
    extract_inventory_moves,
    save_raw_inventory_moves,
    clean_inventory_moves,
    update_inventory_moves_star_schema,
    extract_stock_quants,
    save_raw_stock_quants,
    clean_stock_quants,
    update_stock_quants_star_schema,
    update_product_cost_events,
    update_product_cost_latest_daily,
    update_sales_lines_profit,
    update_profit_aggregates,
)
from celery.result import AsyncResult
from etl_tasks import refresh_materialized_views


class Dataset(str, Enum):
    """Dataset enumeration to replace magic strings."""
    POS = "pos"
    INVOICE_SALES = "invoice_sales"
    PURCHASES = "purchases"
    INVENTORY_MOVES = "inventory_moves"
    STOCK_QUANTS = "stock_quants"
    PROFIT = "profit"
    DIMENSIONS = "dimensions"


# Dataset pipeline configuration for DRY principle
DATASET_PIPELINE = {
    Dataset.POS: (
        extract_pos_order_lines,
        save_raw_data,
        clean_pos_data,
        update_star_schema,
    ),
    Dataset.INVOICE_SALES: (
        extract_sales_invoice_lines,
        save_raw_sales_invoice_lines,
        clean_sales_invoice_lines,
        update_invoice_sales_star_schema,
    ),
    Dataset.PURCHASES: (
        extract_purchase_invoice_lines,
        save_raw_purchase_invoice_lines,
        clean_purchase_invoice_lines,
        update_purchase_star_schema,
    ),
    Dataset.INVENTORY_MOVES: (
        extract_inventory_moves,
        save_raw_inventory_moves,
        clean_inventory_moves,
        update_inventory_moves_star_schema,
    ),
    Dataset.STOCK_QUANTS: (
        extract_stock_quants,
        save_raw_stock_quants,
        clean_stock_quants,
        update_stock_quants_star_schema,
    ),
}


dash.register_page(
    __name__,
    path='/operational',
    name='ETL Ops',
    title='ETL Ops'
)


DATASET_OPTIONS = [
    {'value': Dataset.POS, 'label': 'POS Sales'},
    {'value': Dataset.INVOICE_SALES, 'label': 'Invoice Sales'},
    {'value': Dataset.PURCHASES, 'label': 'Purchase Invoices'},
    {'value': Dataset.INVENTORY_MOVES, 'label': 'Inventory Moves'},
    {'value': Dataset.STOCK_QUANTS, 'label': 'Stock Quants'},
    {'value': Dataset.PROFIT, 'label': 'Profit (Cost + Aggregates)'},
    {'value': Dataset.DIMENSIONS, 'label': 'Dimensions Only'},
]




def _aggrid_default_col_def() -> dict:
    return {
        'sortable': True,
        'filter': True,
        'resizable': True,
        'minWidth': 90,
    }


def _aggrid_pagination_options(page_size: int = 50) -> dict:
    return {
        'pagination': True,
        'paginationPageSize': int(page_size),
    }


def _date_range(start: date, end: date) -> list[date]:
    """Generate date range as list to prevent generator consumption issues."""
    if end < start:
        start, end = end, start
    delta = (end - start).days
    return [start + timedelta(days=offset) for offset in range(delta + 1)]


def _collapse_date_ranges(days: list[date]) -> list[tuple[date, date]]:
    if not days:
        return []
    sorted_days = sorted(set(days))
    ranges: list[tuple[date, date]] = []
    start = sorted_days[0]
    prev = sorted_days[0]
    for day in sorted_days[1:]:
        if day == prev + timedelta(days=1):
            prev = day
            continue
        ranges.append((start, prev))
        start = day
        prev = day
    ranges.append((start, prev))
    return ranges


def _days_needing_refresh(rows: list[dict]) -> list[date]:
    out: list[date] = []
    for row in rows:
        d = row.get('date')
        if not d:
            continue
        raw = row.get('raw')
        clean = row.get('clean')
        fact = row.get('fact')
        if raw in {'Missing', 'Empty'} or clean in {'Missing', 'Empty'} or fact in {'Missing', 'Empty'}:
            try:
                out.append(date.fromisoformat(d))
            except ValueError:
                # Skip invalid date strings
                continue
    return out


def _enqueue_async_refresh(dataset_key: str, start: date, end: date, refresh_dims: bool = False) -> list[dict]:
    if end < start:
        start, end = end, start

    jobs: list[dict] = []

    for day in _date_range(start, end):
        day_str = day.isoformat()
        res = force_refresh_day.apply_async(kwargs={
            "dataset_key": dataset_key,
            "target_date": day_str,
            "refresh_dims": refresh_dims or False,
        })
        jobs.append({
            'dataset': dataset_key,
            'start': day_str,
            'end': day_str,
            'task_id': res.id,
            'state': 'PENDING',
            'step': 'queued',
            'step_name': 'Queued',
        })

    return jobs


def _run_sync_refresh(dataset_key: str, target_date: str) -> dict[str, Any]:
    """Run synchronous refresh using DRY dataset pipeline configuration."""
    try:
        dataset_enum = Dataset(dataset_key)
    except ValueError:
        return {
            "status": "error",
            "message": f"Unsupported dataset: {dataset_key}",
            "records": None,
            "result": None,
        }
    
    # Handle special cases that don't follow the standard pipeline
    if dataset_enum == Dataset.DIMENSIONS:
        result = refresh_dimensions_incremental.apply(throw=True).get()
        return {
            "status": "success",
            "message": "Dimensions refreshed",
            "records": None,
            "result": result,
        }
    
    if dataset_enum == Dataset.PROFIT:
        cost_events_path = update_product_cost_events.apply(args=(target_date,), throw=True).get()
        cost_snapshot_path = update_product_cost_latest_daily.apply(args=(target_date,), throw=True).get()
        profit_lines_path = update_sales_lines_profit.apply(args=(target_date,), throw=True).get()
        agg_paths = update_profit_aggregates.apply(args=(target_date,), throw=True).get()
        return {
            "status": "success",
            "message": f"Profit refreshed for {target_date}",
            "records": None,
            "result": {
                "cost_events_path": cost_events_path,
                "cost_snapshot_path": cost_snapshot_path,
                "profit_lines_path": profit_lines_path,
                "aggregate_paths": agg_paths,
            },
        }
    
    # Handle standard pipeline datasets
    # At this point, dataset_enum is guaranteed to be in DATASET_PIPELINE
    extract, save, clean, update = DATASET_PIPELINE[dataset_enum]
    
    try:
        extraction = extract.apply(args=(target_date,), throw=True).get()
        raw_path = save.apply(args=(extraction,), throw=True).get()
        clean_path = clean.apply(args=(raw_path, target_date), throw=True).get()
        fact_path = update.apply(args=(clean_path, target_date), throw=True).get()
        
        return {
            "status": "success",
            "message": f"{dataset_enum.value} refreshed for {target_date}",
            "records": extraction.get("count", 0) if isinstance(extraction, dict) else 0,
            "result": {
                "raw_path": raw_path,
                "clean_path": clean_path,
                "fact_path": fact_path,
            },
        }
    except Exception as exc:
        return {
            "status": "error",
        "message": f"Failed to refresh {dataset_enum.value}: {str(exc)}",
        "records": None,
        "result": None,
    }
    
    # This should never be reached due to the enum check above
    return {
        "status": "error",
        "message": f"Unsupported dataset: {dataset_key}",
        "records": None,
        "result": None,
    }


layout = dmc.Container(
    [
        dmc.Title('ETL Ops', order=2, mb='xs'),
        dmc.Text('Scan missing partitions and trigger manual refresh jobs.', c='dimmed', mb='lg'),
        dcc.Store(id='etl-ops-bulk-state', storage_type='memory'),
        dcc.Interval(id='etl-ops-bulk-poll', interval=2000, disabled=True),
        
        # Bento Grid Layout
        dmc.Grid(
            [
                # Controls Card - Top Full Width
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Controls', fw=600, size='lg', c='blue.6'),
                                        dmc.Badge('ETL Operations', color='blue', variant='light'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Divider(),
                                dmc.Grid(
                                    [
                                        dmc.GridCol(
                                            dmc.Stack(
                                                [
                                                    dmc.Text('Dataset', fw=500, size='sm', c='dimmed'),
                                                    dmc.Select(
                                                        id='etl-ops-dataset',
                                                        data=DATASET_OPTIONS,
                                                        value='pos',
                                                        size='sm',
                                                        w='100%',
                                                    ),
                                                ],
                                                gap=4,
                                            ),
                                            span={'base': 12, 'sm': 4},
                                        ),
                                        dmc.GridCol(
                                            dmc.Stack(
                                                [
                                                    dmc.Text('From', fw=500, size='sm', c='dimmed'),
                                                    dmc.DatePickerInput(
                                                        id='etl-ops-date-start',
                                                        value=date.today(),
                                                        placeholder='YYYY-MM-DD',
                                                        size='sm',
                                                        w='100%',
                                                    ),
                                                ],
                                                gap=4,
                                            ),
                                            span={'base': 12, 'sm': 4},
                                        ),
                                        dmc.GridCol(
                                            dmc.Stack(
                                                [
                                                    dmc.Text('Until', fw=500, size='sm', c='dimmed'),
                                                    dmc.DatePickerInput(
                                                        id='etl-ops-date-end',
                                                        value=date.today(),
                                                        placeholder='YYYY-MM-DD',
                                                        size='sm',
                                                        w='100%',
                                                    ),
                                                ],
                                                gap=4,
                                            ),
                                            span={'base': 12, 'sm': 4},
                                        ),
                                    ],
                                    gutter={'base': 'xs', 'sm': 'md'},
                                ),
                                dmc.Group(
                                    [
                                        dmc.Button('Scan Partitions', id='etl-ops-scan', variant='filled'),
                                        dmc.Button('Trigger Refresh', id='etl-ops-trigger', variant='light'),
                                        dmc.Button('Refresh MVs', id='etl-ops-mv-refresh', variant='light', color='green'),
                                        dmc.Button('Bulk Repair', id='etl-ops-bulk-run', variant='outline'),
                                    ],
                                    gap='sm',
                                    mt='md'
                                ),
                                dmc.Grid(
                                    [
                                        dmc.GridCol(
                                            dmc.Switch(
                                                id='etl-ops-sync-mode',
                                                label='Sync mode',
                                                description='Wait for completion',
                                                size='sm',
                                            ),
                                            span={'base': 12, 'sm': 4},
                                        ),
                                        dmc.GridCol(
                                            dmc.Switch(
                                                id='etl-ops-refresh-dims',
                                                label='Refresh dimensions',
                                                description='Slow operation',
                                                size='sm',
                                            ),
                                            span={'base': 12, 'sm': 4},
                                        ),
                                        dmc.GridCol(
                                            dmc.Switch(
                                                id='etl-ops-auto-mv',
                                                label='Auto-refresh MVs',
                                                description='After ETL completion',
                                                size='sm',
                                            ),
                                            span={'base': 12, 'sm': 4},
                                        ),
                                    ],
                                    gutter={'base': 'xs', 'sm': 'md'},
                                    mt='sm',
                                ),
                            ],
                            gap='md',
                        ),
                        p='lg',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                    ),
                    span=12,
                ),
                
                # Status Cards - Middle Row
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Scan Summary', fw=600, size='md'),
                                        dmc.Badge('Scan', color='gray', variant='light', size='xs'),
                                    ],
                                    gap='sm',
                                    align='center'
                                ),
                                dmc.Text('Scan summary: —', id='etl-ops-summary', size='sm', c='dimmed'),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        h=120,
                        shadow='sm',
                    ),
                    span=6,
                ),
                
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Trigger Status', fw=600, size='md'),
                                        dmc.Badge('Status', color='gray', variant='light', size='xs'),
                                    ],
                                    gap='sm',
                                    align='center'
                                ),
                                dmc.Text('Trigger status: —', id='etl-ops-trigger-status', size='sm', c='dimmed'),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        h=120,
                        shadow='sm',
                    ),
                    span=6,
                ),
                
                # Main Content Cards - Bottom Row
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Partition Status', fw=600, size='lg'),
                                        dmc.Badge('Live Data', color='gray', variant='light', size='xs'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Group(
                                    [
                                        dmc.Button('Export CSV', id='etl-ops-scan-export', variant='light', size='xs'),
                                    ],
                                    justify='flex-end',
                                ),
                                dag.AgGrid(
                                    id='etl-ops-scan-table',
                                    columnDefs=[
                                        {'field': 'date', 'headerName': 'Date', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                                        {'field': 'raw', 'headerName': 'Raw', 'filter': 'agTextColumnFilter', 'minWidth': 110},
                                        {'field': 'clean', 'headerName': 'Clean', 'filter': 'agTextColumnFilter', 'minWidth': 110},
                                        {'field': 'fact', 'headerName': 'Fact', 'filter': 'agTextColumnFilter', 'minWidth': 110},
                                        {'field': 'raw_rows', 'headerName': 'Raw Rows', 'type': 'numericColumn', 'filter': 'agNumberColumnFilter', 'minWidth': 110,
                                         'valueFormatter': {'function': 'params.value != null ? params.value.toLocaleString() : "0"'}},
                                        {'field': 'clean_rows', 'headerName': 'Clean Rows', 'type': 'numericColumn', 'filter': 'agNumberColumnFilter', 'minWidth': 120,
                                         'valueFormatter': {'function': 'params.value != null ? params.value.toLocaleString() : "0"'}},
                                        {'field': 'fact_rows', 'headerName': 'Fact Rows', 'type': 'numericColumn', 'filter': 'agNumberColumnFilter', 'minWidth': 110,
                                         'valueFormatter': {'function': 'params.value != null ? params.value.toLocaleString() : "0"'}},
                                    ],
                                    defaultColDef=_aggrid_default_col_def(),
                                    rowData=[],
                                    dashGridOptions=_aggrid_pagination_options(),
                                    csvExportParams={'fileName': 'etl_partition_status.csv'},
                                ),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        h=500,
                        style={'overflowY': 'auto'},
                        shadow='sm',
                    ),
                    span=8,
                ),
                
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Dimension Files', fw=600, size='lg'),
                                        dmc.Badge('System', color='gray', variant='light', size='xs'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Group(
                                    [
                                        dmc.Button('Export CSV', id='etl-ops-dim-export', variant='light', size='xs'),
                                    ],
                                    justify='flex-end',
                                ),
                                dag.AgGrid(
                                    id='etl-ops-dim-table',
                                    columnDefs=[
                                        {'field': 'dimension', 'headerName': 'Dimension', 'filter': 'agTextColumnFilter', 'minWidth': 160},
                                        {'field': 'exists', 'headerName': 'Exists', 'filter': 'agTextColumnFilter', 'minWidth': 110},
                                        {'field': 'path', 'headerName': 'Path', 'filter': 'agTextColumnFilter', 'minWidth': 300},
                                    ],
                                    defaultColDef=_aggrid_default_col_def(),
                                    rowData=[],
                                    dashGridOptions=_aggrid_pagination_options(),
                                    csvExportParams={'fileName': 'etl_dimension_files.csv'},
                                ),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        h=500,
                        style={'overflowY': 'auto'},
                        shadow='sm',
                    ),
                    span=4,
                ),
            ],
            gutter='lg',
        ),
        
        # Info Alert
        dmc.Alert(
            dmc.Stack(
                [
                    dmc.Text('💡 Tip: Sync mode runs inside the web worker and can time out on large ranges.', size='sm'),
                    dmc.Text('Use async trigger or force-refresh scripts for heavy workloads.', size='sm', c='dimmed'),
                ],
                gap=0,
            ),
            color='blue',
            variant='light',
            mt='lg',
            radius='lg',
        ),
        dmc.Modal(
            id='etl-ops-bulk-modal',
            opened=False,
            title='Bulk Scan + Repair',
            size='lg',
            children=[
                dmc.Box(
                    [
                        dmc.LoadingOverlay(
                            id='etl-ops-bulk-loading',
                            visible=False,
                            overlayProps={'radius': 'sm', 'blur': 2},
                        ),
                        dmc.Stack(
                            [
                                dmc.Text('Status: —', id='etl-ops-bulk-status', size='sm', c='dimmed'),
                                dmc.Progress(id='etl-ops-bulk-progress', value=0, striped=True, animated=True),
                                
                                # Advanced Options
                                dmc.Accordion(
                                    [
                                        dmc.AccordionItem(
                                            [
                                                dmc.AccordionControl(
                                                    "Advanced Options",
                                                    icon=dmc.themeIcon('settings')
                                                ),
                                                dmc.AccordionPanel(
                                                    dmc.Stack([
                                                        dmc.Text('Auto-fetch Missing Data', fw=500, size='sm'),
                                                        dmc.Switch(
                                                            id='etl-ops-auto-fetch',
                                                            label='Fetch missing raw data before refresh',
                                                            description='Automatically fetch missing POS/invoice data',
                                                            size='sm',
                                                            checked=False
                                                        ),
                                                        dmc.Text('Refresh Dimensions', fw=500, size='sm'),
                                                        dmc.Switch(
                                                            id='etl-ops-refresh-dims',
                                                            label='Refresh dimension tables',
                                                            description='Update product, partner, etc. tables',
                                                            size='sm',
                                                            checked=False
                                                        ),
                                                        dmc.Text('Build Aggregates', fw=500, size='sm'),
                                                        dmc.Switch(
                                                            id='etl-ops-build-agg',
                                                            label='Build sales/profit aggregates',
                                                            description='Rebuild aggregate tables after data refresh',
                                                            size='sm',
                                                            checked=False
                                                        ),
                                                        dmc.Text('Validate Profit', fw=500, size='sm'),
                                                        dmc.Switch(
                                                            id='etl-ops-validate',
                                                            label='Run profit validation',
                                                            description='Check data integrity after refresh',
                                                            size='sm',
                                                            checked=False
                                                        ),
                                                        dmc.Text('Refresh MVs', fw=500, size='sm'),
                                                        dmc.Switch(
                                                            id='etl-ops-refresh-mv',
                                                            label='Refresh materialized views',
                                                            description='Update MVs after data changes',
                                                            size='sm',
                                                            checked=False
                                                        ),
                                                    ], gap='sm')
                                                )
                                            ]
                                        ),
                                    ],
                                    chevronPosition="right",
                                    variant="contained",
                                    style={'marginBottom': '20px'}
                                ),
                                
                                dmc.Group(
                                    [
                                        dmc.Button('Export CSV', id='etl-ops-bulk-export', variant='light', size='xs'),
                                    ],
                                    justify='flex-end',
                                ),
                                dag.AgGrid(
                                    id='etl-ops-bulk-table',
                                    columnDefs=[
                                        {'field': 'dataset', 'headerName': 'Dataset', 'filter': 'agTextColumnFilter', 'minWidth': 150},
                                        {'field': 'range', 'headerName': 'Range', 'filter': 'agTextColumnFilter', 'minWidth': 180},
                                        {'field': 'step', 'headerName': 'Step', 'filter': 'agTextColumnFilter', 'minWidth': 150},
                                        {'field': 'task', 'headerName': 'Task', 'filter': 'agTextColumnFilter', 'minWidth': 230},
                                        {'field': 'state', 'headerName': 'State', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                                    ],
                                    defaultColDef=_aggrid_default_col_def(),
                                    rowData=[],
                                    dashGridOptions=_aggrid_pagination_options(),
                                    csvExportParams={'fileName': 'etl_bulk_jobs.csv'},
                                ),
                                dmc.Group(
                                    [
                                        dmc.Button('Close', id='etl-ops-bulk-close', variant='light'),
                                    ],
                                    justify='flex-end',
                                ),
                            ],
                            gap='sm',
                        ),
                    ],
                    pos='relative',
                )
            ],
        ),
    ],
    size='100%',  # Changed from 'lg' to '100%' for full viewport width
    px='md',      # Added horizontal padding for breathing room
    py='lg',
)

# MV Management Section
dmc.GridCol(
    dmc.Paper(
        dmc.Stack(
            [
                dmc.Group(
                    [
                        dmc.Text('Materialized View Management', fw=600, size='lg'),
                        dmc.Badge('Advanced', color='blue', variant='light', size='xs'),
                    ],
                    gap='sm',
                    align='center'
                ),
                dmc.Group(
                    [
                        dmc.Button('Scan MV Differences', id='etl-ops-mv-scan', n_clicks=0, 
                                    variant='filled'),
                        dmc.Button('Refresh All MVs', id='etl-ops-mv-refresh-all', n_clicks=0,
                                    variant='light'),
                        dmc.Button('Cascading MV Refresh', id='etl-ops-mv-cascading', n_clicks=0,
                                    variant='light', color='green'),
                    ],
                    gap='sm',
                ),
                dmc.Text('MV scan status: —', id='etl-ops-mv-status', size='sm', c='dimmed'),
                dag.AgGrid(
                    id='etl-ops-mv-results',
                    columnDefs=[
                        {'field': 'mv_name', 'headerName': 'Materialized View', 'filter': 'agTextColumnFilter', 'minWidth': 200},
                        {'field': 'status', 'headerName': 'Status', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                        {'field': 'missing_count', 'headerName': 'Missing Days', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                        {'field': 'date_counts', 'headerName': 'Parquet/MV Dates', 'filter': 'agTextColumnFilter', 'minWidth': 150},
                    ],
                    defaultColDef=_aggrid_default_col_def(),
                    rowData=[],
                    dashGridOptions=_aggrid_pagination_options(),
                ),
                dmc.Group(
                    [
                        dmc.Button('Export CSV', id='etl-ops-mv-export', variant='light', size='xs'),
                    ],
                    justify='flex-end',
                    mt='sm'
                ),
            ],
            gap='sm',
        ),
        p='md',
        radius='lg',
        withBorder=True,
        shadow='sm',
    ),
    span=6,
)

# Aggregate Management Section
dmc.GridCol(
    dmc.Paper(
        dmc.Stack(
            [
                dmc.Group(
                    [
                        dmc.Text('Aggregate Management', fw=600, size='lg'),
                        dmc.Badge('Advanced', color='blue', variant='light', size='xs'),
                    ],
                    gap='sm',
                    align='center'
                ),
                dmc.Group(
                    [
                        dmc.Button('Build Sales Aggregates', id='etl-ops-build-sales-agg', n_clicks=0,
                                    variant='light'),
                        dmc.Button('Build Profit Aggregates', id='etl-ops-build-profit-agg', n_clicks=0,
                                    variant='light'),
                        dmc.Button('Build All Aggregates', id='etl-ops-build-all-agg', n_clicks=0,
                                    variant='light', color='blue'),
                    ],
                    gap='sm',
                ),
                dmc.Text('Aggregate build status: —', id='etl-ops-agg-status', size='sm', c='dimmed'),
                dmc.Group(
                    [
                        dmc.Button('Export CSV', id='etl-ops-agg-export', variant='light', size='xs'),
                    ],
                    justify='flex-end',
                    mt='sm'
                ),
            ],
            gap='sm',
        ),
        p='md',
        radius='lg',
        withBorder=True,
        shadow='sm',
    ),
    span=6,
)


@dash.callback(
    [
        dash.Output('etl-ops-scan-table', 'rowData'),
        dash.Output('etl-ops-dim-table', 'rowData'),
        dash.Output('etl-ops-summary', 'children'),
    ],
    dash.Input('etl-ops-scan', 'n_clicks'),
    dash.State('etl-ops-dataset', 'value'),
    dash.State('etl-ops-date-start', 'value'),
    dash.State('etl-ops-date-end', 'value'),
    prevent_initial_call=True,
)
def scan_partitions(n_clicks, dataset_key, date_start, date_end):
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    
    scan_row_data: list[dict] = []
    dim_row_data: list[dict] = []

# Data Validation Section
dmc.GridCol(
    dmc.Paper(
        dmc.Stack(
            [
                dmc.Group(
                    [
                        dmc.Text('Data Validation', fw=600, size='lg'),
                        dmc.Badge('Advanced', color='blue', variant='light', size='xs'),
                    ],
                    gap='sm',
                    align='center'
                ),
                dmc.Group(
                    [
                        dmc.Button('Validate Profit Data', id='etl-ops-validate-profit', n_clicks=0,
                                    variant='light'),
                    ],
                    gap='sm',
                ),
                dmc.Text('Validation status: —', id='etl-ops-validation-status', size='sm', c='dimmed'),
                dag.AgGrid(
                    id='etl-ops-validation-results',
                    columnDefs=[
                        {'field': 'date', 'headerName': 'Date', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                        {'field': 'status', 'headerName': 'Status', 'filter': 'agTextColumnFilter', 'minWidth': 120, 'cellRenderer': 'agGroupCellRenderer'},
                        {'field': 'records', 'headerName': 'Records', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                        {'field': 'issues', 'headerName': 'Issues', 'filter': 'agTextColumnFilter', 'minWidth': 250},
                    ],
                    defaultColDef=_aggrid_default_col_def(),
                    rowData=[],
                    dashGridOptions=_aggrid_pagination_options(),
                ),
                dmc.Group(
                    [
                        dmc.Button('Export CSV', id='etl-ops-validation-export', variant='light', size='xs'),
                    ],
                    justify='flex-end',
                    mt='sm'
                ),
            ],
            gap='sm',
        ),
        p='md',
        radius='lg',
        withBorder=True,
        shadow='sm',
    ),
    span=6,
)

    if dataset_key == Dataset.DIMENSIONS:
        dim_rows = scan_dimension_files()
        for row in dim_rows:
            dim_row_data.append({
                'dimension': row.get('dimension'),
                'exists': 'OK' if row.get('exists') else 'Missing',
                'path': row.get('path'),
            })
        summary = f"Dimensions checked: {len(dim_rows)}"
        return scan_row_data, dim_row_data, summary

    rows = scan_dataset_partitions(dataset_key, start_date, end_date)
    # Use Counter for concise status counting
    raw_counts = Counter(row.get('raw') for row in rows)
    clean_counts = Counter(row.get('clean') for row in rows)
    fact_counts = Counter(row.get('fact') for row in rows)
    
    missing_raw = raw_counts.get('Missing', 0)
    empty_raw = raw_counts.get('Empty', 0)
    missing_clean = clean_counts.get('Missing', 0)
    empty_clean = clean_counts.get('Empty', 0)
    missing_fact = fact_counts.get('Missing', 0)
    empty_fact = fact_counts.get('Empty', 0)
    
    for row in rows:
        scan_row_data.append({
            'date': row.get('date'),
            'raw': row.get('raw'),
            'clean': row.get('clean'),
            'fact': row.get('fact'),
            'raw_rows': row.get('raw_rows'),
            'clean_rows': row.get('clean_rows'),
            'fact_rows': row.get('fact_rows'),
        })

    dim_rows = scan_dimension_files()
    for row in dim_rows:
        dim_row_data.append({
            'dimension': row.get('dimension'),
            'exists': 'OK' if row.get('exists') else 'Missing',
            'path': row.get('path'),
        })

    summary = (
        f"Range {start_date.isoformat()} → {end_date.isoformat()} | "
        f"Missing raw: {missing_raw}, empty raw: {empty_raw} | "
        f"Missing clean: {missing_clean}, empty clean: {empty_clean} | "
        f"Missing fact: {missing_fact}, empty fact: {empty_fact}"
    )
    return scan_row_data, dim_row_data, summary


@dash.callback(
    dash.Output('etl-ops-trigger-status', 'children'),
    dash.Input('etl-ops-trigger', 'n_clicks'),
    dash.State('etl-ops-dataset', 'value'),
    dash.State('etl-ops-date-start', 'value'),
    dash.State('etl-ops-date-end', 'value'),
    dash.State('etl-ops-sync-mode', 'checked'),
    dash.State('etl-ops-refresh-dims', 'checked'),
    prevent_initial_call=True,
)
def trigger_refresh(n_clicks, dataset_key, date_start, date_end, sync_mode, refresh_dims):
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date

    if sync_mode:
        if dataset_key == Dataset.POS:
            return "ERROR: Sync mode is disabled for POS (risk of web worker timeout). Use async trigger."
        if refresh_dims and dataset_key in {Dataset.INVENTORY_MOVES, Dataset.STOCK_QUANTS}:
            try:
                refresh_dimensions_incremental.apply(
                    args=(["products", "locations", "uoms", "partners", "users", "companies", "lots"],),
                    throw=True,
                ).get()
            except Exception as exc:
                return f"ERROR: Dimension refresh failed ({exc})"
        results = []
        errors = []
        for day in _date_range(start_date, end_date):
            try:
                results.append(_run_sync_refresh(dataset_key, day.isoformat()))
            except Exception as exc:
                errors.append(f"{day.isoformat()}: {exc}")

        if errors:
            return f"ERROR: Sync refresh failed for {len(errors)} day(s): " + "; ".join(errors)

        total_records = sum((res.get('records') or 0) for res in results)
        empty_days = sum(1 for res in results if (res.get('records') or 0) == 0)
        return (
            f"SUCCESS: Sync refresh complete for {len(results)} day(s) | "
            f"total records: {total_records} | empty days: {empty_days}"
        )

    # Async mode: enqueue the same chain pattern used by force-refresh scripts.
    day_count = abs((end_date - start_date).days) + 1
    if day_count > 31:
        return f"ERROR: Range too large ({day_count} days). Limit to 31 days."

    jobs = _enqueue_async_refresh(dataset_key, start_date, end_date, refresh_dims=refresh_dims or False)
    if not jobs:
        return "ERROR: Unsupported dataset for async refresh."
    first_task_id = next((j.get('task_id') for j in jobs if j.get('task_id')), None)
    return f"QUEUED: {len(jobs)} task(s)" + (f" (first: {first_task_id})" if first_task_id else '')


@dash.callback(
    [
        dash.Output('etl-ops-bulk-state', 'data'),
        dash.Output('etl-ops-bulk-modal', 'opened'),
        dash.Output('etl-ops-bulk-loading', 'visible'),
        dash.Output('etl-ops-bulk-poll', 'disabled'),
        dash.Output('etl-ops-bulk-status', 'children'),
        dash.Output('etl-ops-bulk-progress', 'value'),
        dash.Output('etl-ops-bulk-table', 'rowData'),
    ],
    dash.Input('etl-ops-bulk-run', 'n_clicks'),
    [
        dash.State('etl-ops-date-start', 'value'),
        dash.State('etl-ops-date-end', 'value'),
        dash.State('etl-ops-auto-fetch', 'checked'),
        dash.State('etl-ops-refresh-dims', 'checked'),
        dash.State('etl-ops-build-agg', 'checked'),
        dash.State('etl-ops-validate', 'checked'),
        dash.State('etl-ops-refresh-mv', 'checked'),
    ],
    prevent_initial_call=True,
)
def bulk_scan_and_enqueue(n_clicks, date_start, date_end, auto_fetch, refresh_dims, build_agg, validate_profit, refresh_mv):
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    day_count = (end_date - start_date).days + 1
    if day_count > 31:
        msg = f"ERROR: Range too large ({day_count} days). Limit to 31 days for bulk repair."
        return ({'status': 'error', 'message': msg}, True, False, True, msg, 0, [])

    priority_datasets = [Dataset.POS]
    other_datasets = [
        Dataset.INVOICE_SALES,
        Dataset.PURCHASES,
        Dataset.INVENTORY_MOVES,
        Dataset.STOCK_QUANTS,
        Dataset.PROFIT,
    ]

    jobs = []
    for ds in priority_datasets + other_datasets:
        rows = scan_dataset_partitions(ds, start_date, end_date)
        missing_days = _days_needing_refresh(rows)
        for seg_start, seg_end in _collapse_date_ranges(missing_days):
            # Apply advanced options based on dataset type
            if auto_fetch and ds in {Dataset.POS, Dataset.INVOICE_SALES, Dataset.PURCHASES}:
                # Auto-fetch missing raw data before refresh
                jobs.extend(_enqueue_async_refresh(
                    ds,
                    seg_start,
                    seg_end,
                    refresh_dims=refresh_dims and ds in {"inventory_moves", "stock_quants"},
                ))
            else:
                # Standard refresh without auto-fetch
                jobs.extend(_enqueue_async_refresh(
                    ds,
                    seg_start,
                    seg_end,
                    refresh_dims=refresh_dims and ds in {"inventory_moves", "stock_quants"},
                ))

    state = {
        'status': 'running',
        'start': start_date.isoformat(),
        'end': end_date.isoformat(),
        'jobs': jobs,
        'auto_fetch': auto_fetch,
        'refresh_dims': refresh_dims,
        'build_agg': build_agg,
        'validate_profit': validate_profit,
        'refresh_mv': refresh_mv,
    }

    row_data = []
    for job in jobs:
        row_data.append({
            'dataset': job.get('dataset'),
            'range': f"{job.get('start')} → {job.get('end')}",
            'step': job.get('step_name', '-') or '-',
            'task': job.get('task_id') or '-',
            'state': job.get('state') or '-',
        })

    if not jobs:
        msg = f"OK: No missing/empty partitions found in {start_date.isoformat()} → {end_date.isoformat()}."
        return ({'status': 'done', 'message': msg, 'jobs': []}, True, False, True, msg, 100, row_data)

    msg = f"Running: queued {len(jobs)} task(s) for {start_date.isoformat()} → {end_date.isoformat()}"
    return (state, True, False, False, msg, 0, row_data)


@dash.callback(
    [
        dash.Output('etl-ops-bulk-state', 'data', allow_duplicate=True),
        dash.Output('etl-ops-bulk-status', 'children', allow_duplicate=True),
        dash.Output('etl-ops-bulk-progress', 'value', allow_duplicate=True),
        dash.Output('etl-ops-bulk-table', 'rowData', allow_duplicate=True),
        dash.Output('etl-ops-bulk-poll', 'disabled', allow_duplicate=True),
    ],
    dash.Input('etl-ops-bulk-poll', 'n_intervals'),
    dash.State('etl-ops-bulk-state', 'data'),
    prevent_initial_call=True,
)
def bulk_poll(n_intervals, bulk_state):
    if not bulk_state or bulk_state.get('status') not in {'running', 'mv_refreshing'}:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, True

    jobs = bulk_state.get('jobs') or []
    total = len(jobs)
    done = 0

    updated_jobs = []
    for job in jobs:
        task_id = job.get('task_id')
        state = job.get('state')
        
        # Initialize variables to prevent undefined reference errors
        step_name = step = pct = None
        
        if not task_id:
            state = 'FAILED'
        else:
            res = AsyncResult(task_id, app=app)
            state = res.state
            info = res.info if hasattr(res, 'info') else None
            if isinstance(info, dict):
                step_name = info.get('step_name')
                step = info.get('step')
                pct = info.get('pct')
        if state in {'SUCCESS', 'FAILURE', 'REVOKED'}:
            done += 1
        job2 = dict(job)
        job2['state'] = state
        if state == 'PROGRESS':
            if step_name:
                job2['step_name'] = step_name
            if step:
                job2['step'] = step
            if isinstance(pct, (int, float)):
                job2['pct'] = float(pct)
        updated_jobs.append(job2)

    bulk_state = dict(bulk_state)
    bulk_state['jobs'] = updated_jobs

    if total == 0:
        pct = 100
    else:
        progress_sum = 0.0
        for job in updated_jobs:
            st = job.get('state')
            if st in {'SUCCESS', 'FAILURE', 'REVOKED'}:
                progress_sum += 100.0
            elif st == 'PROGRESS' and isinstance(job.get('pct'), (int, float)):
                progress_sum += float(job.get('pct'))
        pct = int(progress_sum / total)
    row_data = []
    for job in updated_jobs:
        step_display = job.get('step_name', '-')
        if job.get('state') == 'PROGRESS' and isinstance(job.get('pct'), (int, float)):
            step_display = f"{step_display} ({int(job.get('pct'))}%)"
        row_data.append({
            'dataset': job.get('dataset') or '-',
            'range': f"{job.get('start')} → {job.get('end')}",
            'step': step_display,
            'task': job.get('task_id') or '-',
            'state': job.get('state') or '-',
        })

    if done >= total:
        # Check if we're in MV refresh phase
        if bulk_state.get('status') == 'mv_refreshing':
            bulk_state['status'] = 'done'
            msg = f"Done: All jobs completed including MV refresh"
            return bulk_state, msg, 100, row_data, True
        
        # Initial completion - handle advanced options
        profit_affecting = {
            Dataset.POS,
            Dataset.INVOICE_SALES,
            Dataset.PURCHASES,
            Dataset.PROFIT,
        }
        processed_datasets = {job.get('dataset') for job in updated_jobs}
        
        # Get advanced options from bulk state (stored in initial call)
        auto_fetch = bulk_state.get('auto_fetch', False)
        refresh_dims = bulk_state.get('refresh_dims', False)
        build_agg = bulk_state.get('build_agg', False)
        validate_profit = bulk_state.get('validate_profit', False)
        refresh_mv = bulk_state.get('refresh_mv', False)
        
        # Clear caches for profit-affecting datasets
        if processed_datasets & profit_affecting:
            try:
                clear_profit_caches()
                msg = f"Done: {done}/{total} job(s) finished | Cleared dashboard caches"
            except Exception as e:
                msg = f"Done: {done}/{total} job(s) finished | Failed to clear dashboard caches: {str(e)}"
        else:
            msg = f"Done: {done}/{total} job(s) finished"
        
        # Handle advanced options after data refresh
        advanced_tasks = []
        
        if build_agg and processed_datasets & {Dataset.POS, Dataset.INVOICE_SALES, Dataset.PROFIT}:
            # Build aggregates after data refresh
            try:
                from scripts.etl_data_manager import get_backfill_runner
                runner = get_backfill_runner()
                start_date = date.fromisoformat(bulk_state.get('start'))
                end_date = date.fromisoformat(bulk_state.get('end'))
                
                # Build sales aggregates
                if Dataset.POS in processed_datasets or Dataset.INVOICE_SALES in processed_datasets:
                    agg_result = runner.backfill_aggregates('sales_aggregates', start_date, end_date)
                    advanced_tasks.append(f"Sales aggregates: {agg_result.get('success', 0)} days")
                
                # Build profit aggregates  
                if Dataset.PROFIT in processed_datasets:
                    agg_result = runner.backfill_aggregates('profit_aggregates', start_date, end_date)
                    advanced_tasks.append(f"Profit aggregates: {agg_result.get('success', 0)} days")
                
                msg += f" | Built aggregates: {'; '.join(advanced_tasks)}"
            except Exception as e:
                msg += f" | Failed to build aggregates: {str(e)}"
        
        if validate_profit and Dataset.PROFIT in processed_datasets:
            # Run profit validation after data refresh
            try:
                from scripts.etl_data_manager import get_backfill_runner
                runner = get_backfill_runner()
                start_date = date.fromisoformat(bulk_state.get('start'))
                end_date = date.fromisoformat(bulk_state.get('end'))
                
                val_result = runner.validate_profit(start_date, end_date)
                success_count = val_result.get("success", 0)
                failed_count = val_result.get("failed", 0)
                
                msg += f" | Validated profit: {success_count} valid, {failed_count} invalid"
            except Exception as e:
                msg += f" | Failed to validate profit: {str(e)}"
        
        # Check if auto MV refresh is enabled and trigger if needed
        try:
            redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
            redis_client = redis.from_url(redis_url)
            auto_mv_enabled = redis_client.exists("mv:auto_refresh_enabled")
            
            if refresh_mv and processed_datasets & profit_affecting:
                # Get date range from bulk state
                start_date = bulk_state.get('start')
                end_date = bulk_state.get('end')
                
                # Trigger MV refresh for same date range
                mv_task = refresh_materialized_views.delay(start_date, end_date)
                
                # Add MV refresh job to the job list for tracking
                mv_job = {
                    'dataset': 'materialized_views',
                    'start': start_date,
                    'end': end_date,
                    'task_id': mv_task.id,
                    'state': 'PENDING',
                    'step': 'queued',
                    'step_name': 'MV Refresh Queued',
                }
                updated_jobs.append(mv_job)
                bulk_state['jobs'] = updated_jobs
                bulk_state['status'] = 'mv_refreshing'
                
                msg += f" | MV refresh queued (ID: {mv_task.id[:8]}...)"
                
                return bulk_state, msg, 100, row_data, False
            elif auto_mv_enabled and processed_datasets & profit_affecting:
                # Get date range from bulk state
                start_date = bulk_state.get('start')
                end_date = bulk_state.get('end')
                
                # Trigger MV refresh for same date range
                mv_task = refresh_materialized_views.delay(start_date, end_date)
                
                # Add MV refresh job to the job list for tracking
                mv_job = {
                    'dataset': 'materialized_views',
                    'start': start_date,
                    'end': end_date,
                    'task_id': mv_task.id,
                    'state': 'PENDING',
                    'step': 'queued',
                    'step_name': 'MV Refresh Queued',
                }
                updated_jobs.append(mv_job)
                bulk_state['jobs'] = updated_jobs
                bulk_state['status'] = 'mv_refreshing'
                
                msg += f" | MV refresh queued (ID: {mv_task.id[:8]}...)"
                
                return bulk_state, msg, 100, row_data, False
                
        except Exception as e:
            msg += f" | Failed to trigger MV refresh: {str(e)}"
        finally:
            try:
                if 'redis_client' in locals():
                    redis_client.close()
            except:
                pass
        
        # No MV refresh needed - complete
        bulk_state['status'] = 'done'
        return bulk_state, msg, 100, row_data, True

    msg = f"Running: {done}/{total} job(s) finished"
    return bulk_state, msg, pct, row_data, False


@dash.callback(
    dash.Output('etl-ops-scan-table', 'exportDataAsCsv'),
    dash.Input('etl-ops-scan-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_scan(n_clicks):
    return True


@dash.callback(
    dash.Output('etl-ops-dim-table', 'exportDataAsCsv'),
    dash.Input('etl-ops-dim-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_dim(n_clicks):
    return True


@dash.callback(
    dash.Output('etl-ops-bulk-table', 'exportDataAsCsv'),
    dash.Input('etl-ops-bulk-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_bulk(n_clicks):
    return True

@dash.callback(
    dash.Output('etl-ops-mv-results', 'exportDataAsCsv'),
    dash.Input('etl-ops-mv-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_mv(n_clicks):
    return True

@dash.callback(
    dash.Output('etl-ops-validation-results', 'exportDataAsCsv'),
    dash.Input('etl-ops-validation-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_validation(n_clicks):
    return True

@dash.callback(
    dash.Output('etl-ops-agg-results', 'exportDataAsCsv'),
    dash.Input('etl-ops-agg-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_aggregates(n_clicks):
    return True


@dash.callback(
    dash.Output('etl-ops-trigger-status', 'children', allow_duplicate=True),
    dash.Input('etl-ops-mv-refresh', 'n_clicks'),
    [dash.State('etl-ops-date-start', 'value'),
     dash.State('etl-ops-date-end', 'value')],
    prevent_initial_call=True,
)
def trigger_mv_refresh(n_clicks, date_start, date_end):
    """Trigger MV refresh for selected date range."""
    if not n_clicks:
        return dash.no_update
    
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    
    try:
        # Trigger the MV refresh task
        task = refresh_materialized_views.delay(start_date.isoformat(), end_date.isoformat())
        
        return f"QUEUED: MV refresh task started (ID: {task.id[:8]}...)"
        
    except Exception as exc:
        return f"ERROR: Failed to queue MV refresh: {str(exc)}"


@dash.callback(
    dash.Output('etl-ops-auto-mv', 'checked', allow_duplicate=True),
    dash.Input('etl-ops-auto-mv', 'checked'),
    prevent_initial_call=True,
)
def toggle_auto_mv_refresh(auto_enabled):
    """Toggle auto MV refresh setting."""
    redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
    redis_client = redis.from_url(redis_url)
    
    try:
        if auto_enabled:
            redis_client.set("mv:auto_refresh_enabled", "true")
        else:
            redis_client.delete("mv:auto_refresh_enabled")
        return dash.no_update
    except Exception as e:
        # Log error but return no_update to maintain UI state
        print(f"Error in toggle_auto_mv_refresh: {e}")
        return dash.no_update
    finally:
        try:
            redis_client.close()
        except Exception as e:
            # Silently ignore Redis connection cleanup errors
            pass

@dash.callback(
    dash.Output('etl-ops-mv-results', 'children'),
    dash.Output('etl-ops-mv-status', 'children'),
    dash.Output('etl-ops-mv-status', 'className'),
    dash.Input('etl-ops-mv-scan', 'n_clicks'),
    dash.State('etl-ops-date-start', 'value'),
    dash.State('etl-ops-date-end', 'value'),
    prevent_initial_call=True,
)
def bulk_close(n_clicks):
    return False, True, None

@dash.callback(
    dash.Output('etl-ops-mv-status', 'children'),
    dash.Output('etl-ops-mv-status', 'className'),
    dash.Output('etl-ops-mv-refresh-all', 'disabled'),
    dash.Input('etl-ops-mv-cascading', 'n_clicks'),
    dash.State('etl-ops-date-start', 'value'),
    dash.State('etl-ops-date-end', 'value'),
    prevent_initial_call=True,
)
def refresh_mvs_cascading(n_clicks, date_start, date_end):
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    
    try:
        from scripts.etl_data_manager import get_backfill_runner
        runner = get_backfill_runner(log_callback=lambda msg: print(f"Progress: {msg}"))
        
        print("Starting cascading MV refresh...")
        
        # Refresh all MVs with auto-fetch
        views = set(MV_TO_PARQUET_MAP.keys())
        results = runner.refresh_materialized_views_cascading(
            views=views,
            start_date=start_date,
            end_date=end_date,
            auto_fetch=True,
            refresh_dims=False
        )
        
        success_count = results.get('success', 0)
        failed_count = results.get('failed', 0)
        errors = results.get('errors', [])
        
        if failed_count == 0:
            status_msg = f"✅ Cascading refresh complete: {success_count} views refreshed successfully"
            return status_msg, "alert alert-success", False
        else:
            error_msg = f"⚠️ Refresh completed with {failed_count} errors. Success: {success_count}"
            if errors:
                error_msg += f" Errors: {'; '.join(errors[:3])}"
            return error_msg, "alert alert-warning", False
            
    except Exception as e:
        return f"❌ Cascading refresh failed: {str(e)}", "alert alert-danger", False

@dash.callback(
    dash.Output('etl-ops-mv-status', 'children'),
    dash.Output('etl-ops-mv-status', 'className'),
    dash.Input('etl-ops-mv-refresh-all', 'n_clicks'),
    prevent_initial_call=True,
)
def refresh_all_mvs_simple(n_clicks):
    if not n_clicks:
        return dash.no_update, dash.no_update
    
    try:
        from scripts.etl_data_manager import get_backfill_runner
        runner = get_backfill_runner()
        views = set(MV_TO_PARQUET_MAP.keys())
        results = runner.refresh_materialized_views(views)
        
        success_count = results.get('success', 0)
        failed_count = results.get('failed', 0)
        
        if failed_count == 0:
            return f"✅ Simple refresh complete: {success_count} views refreshed", "alert alert-success"
        else:
            return f"⚠️ Refresh completed with {failed_count} errors", "alert alert-warning"
            
    except Exception as e:
        return f"❌ Simple refresh failed: {str(e)}", "alert alert-danger"

@dash.callback(
    dash.Output('etl-ops-agg-status', 'children'),
    dash.Output('etl-ops-agg-status', 'className'),
    dash.Output('etl-ops-build-sales-agg', 'disabled'),
    dash.Output('etl-ops-build-profit-agg', 'disabled'),
    dash.Output('etl-ops-build-all-agg', 'disabled'),
    dash.Input('etl-ops-build-sales-agg', 'n_clicks'),
    dash.Input('etl-ops-build-profit-agg', 'n_clicks'),
    dash.Input('etl-ops-build-all-agg', 'n_clicks'),
    dash.State('etl-ops-date-start', 'value'),
    dash.State('etl-ops-date-end', 'value'),
    prevent_initial_call=True,
)
def build_aggregates(n_clicks, sales_clicks, profit_clicks, all_clicks, date_start, date_end):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, False, False, False
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    
    try:
        from scripts.etl_data_manager import get_backfill_runner
        runner = get_backfill_runner(log_callback=lambda msg: print(f"Progress: {msg}"))
        
        # Determine which aggregates to build
        agg_types = []
        if button_id == 'etl-ops-build-sales-agg':
            agg_types = ['sales_aggregates']
        elif button_id == 'etl-ops-build-profit-agg':
            agg_types = ['profit_aggregates']
        elif button_id == 'etl-ops-build-all-agg':
            agg_types = ['sales_aggregates', 'profit_aggregates']
        
        results = {"success": 0, "failed": 0, "errors": []}
        
        for agg_type in agg_types:
            print(f"Building {agg_type}...")
            result = runner.backfill_aggregates(agg_type, start_date, end_date)
            results["success"] += result.get("success", 0)
            results["failed"] += result.get("failed", 0)
            results["errors"].extend(result.get("errors", []))
        
        success_count = results["success"]
        failed_count = results["failed"]
        errors = results["errors"]
        
        if failed_count == 0:
            status_msg = f"✅ Aggregate building complete: {success_count} days processed"
            return status_msg, "alert alert-success", False, False, False
        else:
            error_msg = f"⚠️ Building completed with {failed_count} errors. Success: {success_count}"
            if errors:
                error_msg += f" Errors: {'; '.join(errors[:3])}"
            return error_msg, "alert alert-warning", False, False, False
            
    except Exception as e:
        return f"❌ Aggregate building failed: {str(e)}", "alert alert-danger", False, False, False

@dash.callback(
    dash.Output('etl-ops-validation-results', 'children'),
    dash.Output('etl-ops-validation-status', 'children'),
    dash.Output('etl-ops-validation-status', 'className'),
    dash.Input('etl-ops-validate-profit', 'n_clicks'),
    dash.State('etl-ops-date-start', 'value'),
    dash.State('etl-ops-date-end', 'value'),
    prevent_initial_call=True,
)
def validate_profit_data(n_clicks, date_start, date_end):
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    
    logger.info(f"Starting profit validation for {start_date} to {end_date}")
    
    try:
        from scripts.etl_data_manager import get_backfill_runner
        runner = get_backfill_runner(log_callback=lambda msg: logger.info(f"Validation: {msg}"))
        
        results = runner.validate_profit(start_date, end_date)
        validation_results = results.get("validation_results", [])
        success_count = results.get("success", 0)
        failed_count = results.get("failed", 0)
        
        logger.info(f"Validation completed: {success_count} valid, {failed_count} invalid")
        
        # Create detailed validation results table
        table_data = []
        for result in validation_results:
            date_str = result.get("date", "")
            status = result.get("status", "UNKNOWN")
            records = result.get("records", 0)
            issues = result.get("issues", [])
            
            # Enhanced status badge with color coding
            status_color = "green" if status == "VALID" else "orange" if status == "WARNING" else "red"
            status_badge = dmc.Badge(
                status,
                color=status_color
            )
            
            issues_text = "; ".join(issues) if issues else "No issues"
            
            table_data.append({
                'date': date_str,
                'status': status_badge,
                'records': f"{records:,}",
                'issues': issues_text,
            })
        
        # Enhanced summary statistics with progress indicators
        total_days = success_count + failed_count
        valid_percentage = (success_count / total_days * 100) if total_days > 0 else 0
        
        summary = dmc.Paper(
            dmc.Stack([
                dmc.Group([
                    dmc.Text('Validation Summary', fw=600, size='lg'),
                    dmc.Badge(f"{total_days} days", color="blue", variant="light", size="sm")
                ], justify="space-between"),
                dmc.Progress(
                    value=valid_percentage,
                    size="md",
                    striped=True,
                    color="green" if failed_count == 0 else "orange"
                ),
                dbc.Row([
                    dbc.Col([
                        dmc.Group([
                            dmc.Text(f"{success_count}", className="text-success", size="lg"),
                            dmc.P("Valid", className="text-muted")
                        ])
                    ]),
                    dbc.Col([
                        dmc.Group([
                            dmc.Text(f"{failed_count}", className="text-danger", size="lg"),
                            dmc.P("Invalid", className="text-muted")
                        ])
                    ]),
                    dbc.Col([
                        dmc.Group([
                            dmc.Text(f"{valid_percentage:.1f}%", className="text-primary", size="lg"),
                            dmc.P("Success Rate", className="text-muted")
                        ])
                    ]),
                ])
            ])
        )
        
        results_div = dmc.Stack([summary, dag.AgGrid(
            id='etl-ops-validation-results',
            columnDefs=[
                {'field': 'date', 'headerName': 'Date', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                {'field': 'status', 'headerName': 'Status', 'filter': 'agTextColumnFilter', 'minWidth': 120, 'cellRenderer': 'agGroupCellRenderer'},
                {'field': 'records', 'headerName': 'Records', 'filter': 'agTextColumnFilter', 'minWidth': 120},
                {'field': 'issues', 'headerName': 'Issues', 'filter': 'agTextColumnFilter', 'minWidth': 250},
            ],
            defaultColDef=_aggrid_default_col_def(),
            rowData=table_data,
            dashGridOptions=_aggrid_pagination_options(),
        )])
        
        # Enhanced status messages with visual indicators
        if failed_count == 0:
            status_msg = f"✅ Validation complete: All {success_count} days passed"
            status_class = "alert alert-success"
        elif failed_count / total_days <= 0.1:  # Less than 10% failure rate
            status_msg = f"⚠️ Validation complete: {failed_count} of {total_days} days have minor issues"
            status_class = "alert alert-warning"
        else:  # High failure rate
            status_msg = f"❌ Validation complete: {failed_count} of {total_days} days have significant issues"
            status_class = "alert alert-danger"
            
        logger.info(f"Validation status: {status_msg}")
        return results_div, status_msg, status_class
            
    except Exception as e:
        logger.error(f"Profit validation failed: {str(e)}", exc_info=True)
        return dmc.Text(f"Error during validation: {str(e)}", size='sm', c='red'), f"Validation failed: {str(e)}", "alert alert-danger"
