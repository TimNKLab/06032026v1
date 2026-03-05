import dash
from dash import dcc, html
import dash_mantine_components as dmc
import dash_ag_grid as dag
from datetime import date, timedelta

from services.etl_ops import (
    scan_dataset_partitions,
    scan_dimension_files,
    parse_date,
)
from services.profit_metrics import clear_profit_caches
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
from celery import chain


dash.register_page(
    __name__,
    path='/operational',
    name='ETL Ops',
    title='ETL Ops'
)


DATASET_OPTIONS = [
    {'value': 'pos', 'label': 'POS Sales'},
    {'value': 'invoice_sales', 'label': 'Invoice Sales'},
    {'value': 'purchases', 'label': 'Purchase Invoices'},
    {'value': 'inventory_moves', 'label': 'Inventory Moves'},
    {'value': 'stock_quants', 'label': 'Stock Quants'},
    {'value': 'profit', 'label': 'Profit (Cost + Aggregates)'},
    {'value': 'dimensions', 'label': 'Dimensions Only'},
]


def _table_data(head):
    return {'head': head, 'body': []}


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


def _date_range(start: date, end: date):
    if end < start:
        start, end = end, start
    delta = (end - start).days
    return (start + timedelta(days=offset) for offset in range(delta + 1))


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
            except Exception:
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
            "refresh_dims": bool(refresh_dims),
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


def _run_sync_refresh(dataset_key: str, target_date: str):
    if dataset_key == "dimensions":
        result = refresh_dimensions_incremental.apply(throw=True).get()
        return {
            "status": "success",
            "message": "Dimensions refreshed",
            "records": None,
            "result": result,
        }

    if dataset_key == "profit":
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

    if dataset_key == "pos":
        extraction = extract_pos_order_lines.apply(args=(target_date,), throw=True).get()
        raw_path = save_raw_data.apply(args=(extraction,), throw=True).get()
        clean_path = clean_pos_data.apply(args=(raw_path, target_date), throw=True).get()
        fact_path = update_star_schema.apply(args=(clean_path, target_date), throw=True).get()
        return {
            "status": "success",
            "message": f"POS refreshed for {target_date}",
            "records": extraction.get("count", 0),
            "result": {
                "raw_path": raw_path,
                "clean_path": clean_path,
                "fact_path": fact_path,
            },
        }

    if dataset_key == "invoice_sales":
        extraction = extract_sales_invoice_lines.apply(args=(target_date,), throw=True).get()
        raw_path = save_raw_sales_invoice_lines.apply(args=(extraction,), throw=True).get()
        clean_path = clean_sales_invoice_lines.apply(args=(raw_path, target_date), throw=True).get()
        fact_path = update_invoice_sales_star_schema.apply(args=(clean_path, target_date), throw=True).get()
        return {
            "status": "success",
            "message": f"Invoice sales refreshed for {target_date}",
            "records": extraction.get("count", 0),
            "result": {
                "raw_path": raw_path,
                "clean_path": clean_path,
                "fact_path": fact_path,
            },
        }

    if dataset_key == "purchases":
        extraction = extract_purchase_invoice_lines.apply(args=(target_date,), throw=True).get()
        raw_path = save_raw_purchase_invoice_lines.apply(args=(extraction,), throw=True).get()
        clean_path = clean_purchase_invoice_lines.apply(args=(raw_path, target_date), throw=True).get()
        fact_path = update_purchase_star_schema.apply(args=(clean_path, target_date), throw=True).get()
        return {
            "status": "success",
            "message": f"Purchases refreshed for {target_date}",
            "records": extraction.get("count", 0),
            "result": {
                "raw_path": raw_path,
                "clean_path": clean_path,
                "fact_path": fact_path,
            },
        }

    if dataset_key == "inventory_moves":
        extraction = extract_inventory_moves.apply(args=(target_date,), throw=True).get()
        raw_path = save_raw_inventory_moves.apply(args=(extraction,), throw=True).get()
        clean_path = clean_inventory_moves.apply(args=(raw_path, target_date), throw=True).get()
        fact_path = update_inventory_moves_star_schema.apply(args=(clean_path, target_date), throw=True).get()
        return {
            "status": "success",
            "message": f"Inventory moves refreshed for {target_date}",
            "records": extraction.get("count", 0),
            "result": {
                "raw_path": raw_path,
                "clean_path": clean_path,
                "fact_path": fact_path,
            },
        }

    if dataset_key == "stock_quants":
        extraction = extract_stock_quants.apply(args=(target_date,), throw=True).get()
        raw_path = save_raw_stock_quants.apply(args=(extraction,), throw=True).get()
        clean_path = clean_stock_quants.apply(args=(raw_path, target_date), throw=True).get()
        fact_path = update_stock_quants_star_schema.apply(args=(clean_path, target_date), throw=True).get()
        return {
            "status": "success",
            "message": f"Stock quants refreshed for {target_date}",
            "records": extraction.get("count", 0),
            "result": {
                "raw_path": raw_path,
                "clean_path": clean_path,
                "fact_path": fact_path,
            },
        }

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
                                            span={'base': 12, 'sm': 6},
                                        ),
                                        dmc.GridCol(
                                            dmc.Switch(
                                                id='etl-ops-refresh-dims',
                                                label='Refresh dimensions',
                                                description='Slow operation',
                                                size='sm',
                                            ),
                                            span={'base': 12, 'sm': 6},
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

    if dataset_key == 'dimensions':
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
    missing_raw = 0
    missing_clean = 0
    missing_fact = 0
    empty_raw = 0
    empty_clean = 0
    empty_fact = 0
    for row in rows:
        if row['raw'] == 'Missing':
            missing_raw += 1
        elif row['raw'] == 'Empty':
            empty_raw += 1
        if row['clean'] == 'Missing':
            missing_clean += 1
        elif row['clean'] == 'Empty':
            empty_clean += 1
        if row['fact'] == 'Missing':
            missing_fact += 1
        elif row['fact'] == 'Empty':
            empty_fact += 1
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
        if dataset_key == "pos":
            return "ERROR: Sync mode is disabled for POS (risk of web worker timeout). Use async trigger."
        if refresh_dims and dataset_key in {"inventory_moves", "stock_quants"}:
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

    jobs = _enqueue_async_refresh(dataset_key, start_date, end_date, refresh_dims=bool(refresh_dims))
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
    ],
    prevent_initial_call=True,
)
def bulk_scan_and_enqueue(n_clicks, date_start, date_end):
    start_date = parse_date(date_start) or date.today()
    end_date = parse_date(date_end) or start_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    day_count = (end_date - start_date).days + 1
    if day_count > 31:
        msg = f"ERROR: Range too large ({day_count} days). Limit to 31 days for bulk repair."
        return ({'status': 'error', 'message': msg}, True, False, True, msg, 0, [])

    priority_datasets = ['pos']
    other_datasets = [
        'invoice_sales',
        'purchases',
        'inventory_moves',
        'stock_quants',
        'product_cost_events',
        'product_cost_latest',
        'profit',
    ]

    jobs = []
    for ds in priority_datasets + other_datasets:
        rows = scan_dataset_partitions(ds, start_date, end_date)
        missing_days = _days_needing_refresh(rows)
        for seg_start, seg_end in _collapse_date_ranges(missing_days):
            jobs.extend(_enqueue_async_refresh(
                ds,
                seg_start,
                seg_end,
                refresh_dims=bool(ds in {"inventory_moves", "stock_quants"}),
            ))

    state = {
        'status': 'running',
        'start': start_date.isoformat(),
        'end': end_date.isoformat(),
        'jobs': jobs,
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
    if not bulk_state or bulk_state.get('status') not in {'running'}:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, True

    jobs = bulk_state.get('jobs') or []
    total = len(jobs)
    done = 0

    updated_jobs = []
    for job in jobs:
        task_id = job.get('task_id')
        state = job.get('state')
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
            else:
                step_name = None
                step = None
                pct = None
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
        bulk_state['status'] = 'done'
        msg = f"Done: {done}/{total} job(s) finished"
        # Auto-clear dashboard caches if any profit-affecting datasets were processed
        profit_affecting = {
            'pos',
            'invoice_sales',
            'purchases',
            'product_cost_events',
            'product_cost_latest',
            'profit',
        }
        processed_datasets = {job.get('dataset') for job in updated_jobs}
        if processed_datasets & profit_affecting:
            try:
                clear_profit_caches()
                msg += " | Cleared dashboard caches"
            except Exception:
                msg += " | Failed to clear dashboard caches"
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
    [
        dash.Output('etl-ops-bulk-modal', 'opened', allow_duplicate=True),
        dash.Output('etl-ops-bulk-poll', 'disabled', allow_duplicate=True),
        dash.Output('etl-ops-bulk-state', 'data', allow_duplicate=True),
    ],
    dash.Input('etl-ops-bulk-close', 'n_clicks'),
    prevent_initial_call=True,
)
def bulk_close(n_clicks):
    return False, True, None
