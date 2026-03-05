import dash
from dash import dcc, Output, Input, State
import dash_mantine_components as dmc
import dash_ag_grid as dag
from datetime import date, datetime, timedelta
import pandas as pd

from services.inventory_metrics import (
    get_abc_analysis,
    get_stock_levels,
    get_stock_levels_ledger,
    get_sell_through_analysis,
    DEFAULT_STOCK_LOOKBACK_DAYS,
    DEFAULT_LOW_STOCK_DAYS,
    STOCK_LEDGER_BASELINE_DATE,
)
from services.inventory_charts import (
    build_abc_pareto_chart,
    build_abc_category_distribution_chart,
    build_stock_cover_distribution_chart,
    build_low_stock_chart,
    build_sell_through_by_category_chart,
    build_sell_through_top_bottom_chart,
)


dash.register_page(
    __name__,
    path='/inventory',
    name='Inventory Management',
    title='Inventory Health',
)

DEFAULT_LOOKBACK = 30
CHART_HEIGHT = '380px'
TEXT_FIELDS = frozenset({
    'product_name', 'product_category', 'product_brand', 'flags', 'abc_class',
})


# ── Reusable layout builders ─────────────────────────────────────────

def _abc_kpi_card(title, count_id, share_id, share_color):
    return dmc.GridCol(
        dmc.Paper(
            dmc.Stack([
                dmc.Text(title, size='sm', c='dimmed'),
                dmc.Text('—', size='xl', fw=600, id=count_id),
                dmc.Text('Revenue share: —', size='xs', c=share_color, id=share_id),
            ]),
            p='md', radius='md', withBorder=True,
        ),
        span={'base': 12, 'sm': 4},
    )


def _simple_kpi_card(title, value_id, span_sm):
    return dmc.GridCol(
        dmc.Paper(
            dmc.Stack([
                dmc.Text(title, size='sm', c='dimmed'),
                dmc.Text('—', size='xl', fw=600, id=value_id),
            ]),
            p='md', radius='md', withBorder=True,
        ),
        span={'base': 12, 'sm': span_sm},
    )


def _chart_grid(left_title, left_id, right_title, right_id):
    """7 / 5 responsive grid with two chart cards wrapped in loading spinners."""
    return dmc.Grid(
        [
            dmc.GridCol(
                dmc.Paper(
                    dmc.Stack([
                        dmc.Text(left_title, fw=600, mb='md'),
                        dcc.Loading(
                            dcc.Graph(
                                id=left_id,
                                figure={},
                                config={'displayModeBar': False},
                                style={'height': CHART_HEIGHT, 'width': '100%'},
                            ),
                            type='dot',
                        ),
                    ]),
                    p='md', radius='md', withBorder=True, style={'height': '100%'},
                ),
                span={'base': 12, 'sm': 7},
            ),
            dmc.GridCol(
                dmc.Paper(
                    dmc.Stack([
                        dmc.Text(right_title, fw=600, mb='md'),
                        dcc.Loading(
                            dcc.Graph(
                                id=right_id,
                                figure={},
                                config={'displayModeBar': False},
                                style={'height': CHART_HEIGHT},
                                responsive=True,
                            ),
                            type='dot',
                        ),
                    ]),
                    p='md', radius='md', withBorder=True, style={'height': '100%'},
                ),
                span={'base': 12, 'sm': 5},
            ),
        ],
        gutter={'base': 'md', 'lg': 'lg'},
        mt='lg',
    )


def _table_section(title, table_id, export_id, column_defs, csv_filename):
    """Paper card containing an Export button and a paginated AG Grid."""
    return dmc.Paper(
        dmc.Stack([
            dmc.Text(title, fw=600, mb='md'),
            dmc.Group(
                [dmc.Button('Export CSV', id=export_id, variant='light', size='xs')],
                justify='flex-end',
            ),
            dcc.Loading(
                dmc.Box(
                    dag.AgGrid(
                        id=table_id,
                        columnDefs=column_defs,
                        defaultColDef={
                            'sortable': True,
                            'filter': True,
                            'resizable': True,
                            'minWidth': 80,
                        },
                        rowData=[],
                        dashGridOptions={
                            'pagination': True,
                            'paginationPageSize': 50,
                            'enableCellTextSelection': True,
                            'animateRows': True,
                        },
                        csvExportParams={'fileName': csv_filename},
                    ),
                    h=420,
                    style={'height': '100%'},
                ),
                type='dot',
            ),
        ]),
        p='md', radius='md', withBorder=True, mt='lg',
    )


# ── Column definitions ───────────────────────────────────────────────

STOCK_COLUMNS = [
    {
        'field': 'product_name', 'headerName': 'SKU',
        'filter': 'agTextColumnFilter', 'minWidth': 200,
    },
    {
        'field': 'product_category', 'headerName': 'Category',
        'filter': 'agTextColumnFilter', 'minWidth': 150,
    },
    {
        'field': 'product_brand', 'headerName': 'Brand',
        'filter': 'agTextColumnFilter', 'minWidth': 120,
    },
    {
        'field': 'on_hand_qty', 'headerName': 'On-hand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 100,
    },
    {
        'field': 'reserved_qty', 'headerName': 'Reserved',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 100,
    },
    {
        'field': 'avg_daily_sold', 'headerName': 'Avg Daily Sold',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(2) : "0.00"',
        },
        'minWidth': 120,
    },
    {
        'field': 'days_of_cover', 'headerName': 'Days of Cover',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(1) : "—"',
        },
        'minWidth': 120,
    },
    {
        'field': 'flags', 'headerName': 'Flags',
        'filter': 'agTextColumnFilter', 'minWidth': 100,
    },
]

SELL_THROUGH_COLUMNS = [
    {
        'field': 'product_name', 'headerName': 'SKU',
        'filter': 'agTextColumnFilter', 'minWidth': 200,
    },
    {
        'field': 'product_category', 'headerName': 'Category',
        'filter': 'agTextColumnFilter', 'minWidth': 150,
    },
    {
        'field': 'product_brand', 'headerName': 'Brand',
        'filter': 'agTextColumnFilter', 'minWidth': 120,
    },
    {
        'field': 'begin_on_hand', 'headerName': 'Begin On-hand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 130,
    },
    {
        'field': 'units_received', 'headerName': 'Units Received',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 130,
    },
    {
        'field': 'units_sold', 'headerName': 'Units Sold',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 110,
    },
    {
        'field': 'sell_through', 'headerName': 'Sell-through',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function':
                'params.value != null ? (params.value * 100).toFixed(1) + "%" : "0.0%"',
        },
        'minWidth': 120,
    },
]

ABC_COLUMNS = [
    {
        'field': 'product_name', 'headerName': 'SKU',
        'filter': 'agTextColumnFilter', 'minWidth': 200,
    },
    {
        'field': 'product_category', 'headerName': 'Category',
        'filter': 'agTextColumnFilter', 'minWidth': 150,
    },
    {
        'field': 'product_brand', 'headerName': 'Brand',
        'filter': 'agTextColumnFilter', 'minWidth': 120,
    },
    {
        'field': 'revenue', 'headerName': 'Revenue',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function':
                'params.value != null ? "Rp " + params.value.toLocaleString() : "Rp 0"',
        },
        'minWidth': 120,
    },
    {
        'field': 'quantity', 'headerName': 'Units',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 90,
    },
    {
        'field': 'cumulative_share', 'headerName': 'Cumulative %',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function':
                'params.value != null ? (params.value * 100).toFixed(1) + "%" : "0.0%"',
        },
        'minWidth': 120,
    },
    {
        'field': 'abc_class', 'headerName': 'Class',
        'filter': 'agTextColumnFilter', 'minWidth': 90,
    },
]


# ── Layout ────────────────────────────────────────────────────────────

def layout():
    today = date.today()
    default_start = max(today - timedelta(days=DEFAULT_LOOKBACK), STOCK_LEDGER_BASELINE_DATE)

    return dmc.Container(
        [
            dmc.Title('Inventory Health', order=2),
            dmc.Text(
                'Stock levels, sell-through, and ABC analysis for inventory performance.',
                c='dimmed',
            ),
            dmc.Tabs(
                [
                    dmc.TabsList([
                        dmc.TabsTab('Stock Levels', value='stock-levels'),
                        dmc.TabsTab('Sell-through', value='sell-through'),
                        dmc.TabsTab('ABC Analysis', value='abc-analysis'),
                    ]),

                    # ── Stock Levels ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            dmc.Paper(
                                dmc.Stack([
                                    dmc.Group([
                                        dmc.Stack([
                                            dmc.Text('As of:', fw=600),
                                            dmc.DatePickerInput(
                                                value=today,
                                                placeholder='Select date',
                                                minDate=STOCK_LEDGER_BASELINE_DATE,
                                                maxDate=today,
                                                id='inventory-stock-date',
                                            ),
                                        ], gap=4),
                                        dmc.Button(
                                            'Apply',
                                            id='inventory-stock-apply',
                                            variant='filled',
                                            size='sm',
                                        ),
                                    ], gap='xl', align='flex-end', wrap='wrap'),
                                    dmc.Text(
                                        'Snapshot: —',
                                        size='xs', c='dimmed',
                                        id='inventory-stock-snapshot-label',
                                    ),
                                    dmc.Text(
                                        f'Lookback: {DEFAULT_STOCK_LOOKBACK_DAYS} days · '
                                        f'Low stock: {DEFAULT_LOW_STOCK_DAYS} days',
                                        size='xs', c='dimmed',
                                    ),
                                ], gap='xs'),
                                p='md', radius='md', withBorder=True, mt='md',
                            ),
                            dmc.Grid([
                                _simple_kpi_card('Total On-hand Units', 'inventory-stock-kpi-onhand', 4),
                                _simple_kpi_card('Low Stock SKUs', 'inventory-stock-kpi-low', 4),
                                _simple_kpi_card('Dead Stock SKUs', 'inventory-stock-kpi-dead', 4),
                            ], gutter={'base': 'md', 'lg': 'lg'}, mt='md'),
                            _chart_grid(
                                'Days of Cover Distribution', 'inventory-stock-cover',
                                'Lowest Days of Cover', 'inventory-stock-low',
                            ),
                            _table_section(
                                'Stock Levels Table', 'inventory-stock-table',
                                'inventory-stock-export', STOCK_COLUMNS, 'stock_levels.csv',
                            ),
                        ], gap='md'),
                        value='stock-levels',
                    ),

                    # ── Sell-through ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            dmc.Paper(
                                dmc.Stack([
                                    dmc.Group([
                                        dmc.Stack([
                                            dmc.Text('From:', fw=600),
                                            dmc.DatePickerInput(
                                                value=default_start,
                                                placeholder='Select date',
                                                minDate=STOCK_LEDGER_BASELINE_DATE,
                                                maxDate=today,
                                                id='inventory-sell-date-from',
                                            ),
                                        ], gap=4),
                                        dmc.Stack([
                                            dmc.Text('Until:', fw=600),
                                            dmc.DatePickerInput(
                                                value=today,
                                                placeholder='Select date',
                                                minDate=STOCK_LEDGER_BASELINE_DATE,
                                                maxDate=today,
                                                id='inventory-sell-date-until',
                                            ),
                                        ], gap=4),
                                        dmc.Button(
                                            'Apply',
                                            id='inventory-sell-apply',
                                            variant='filled',
                                            size='sm',
                                        ),
                                    ], gap='xl', align='flex-end', wrap='wrap'),
                                    dmc.Text(
                                        'Snapshot: —',
                                        size='xs', c='dimmed',
                                        id='inventory-sell-snapshot-label',
                                    ),
                                ], gap='xs'),
                                p='md', radius='md', withBorder=True, mt='md',
                            ),
                            dmc.Grid([
                                _simple_kpi_card('Sell-through %', 'inventory-sell-kpi-sellthrough', 3),
                                _simple_kpi_card('Units Sold', 'inventory-sell-kpi-sold', 3),
                                _simple_kpi_card('Units Received', 'inventory-sell-kpi-received', 3),
                                _simple_kpi_card('Begin On-hand', 'inventory-sell-kpi-begin', 3),
                            ], gutter={'base': 'md', 'lg': 'lg'}, mt='md'),
                            _chart_grid(
                                'Sell-through by Category', 'inventory-sell-category',
                                'Top / Bottom Sell-through', 'inventory-sell-top-bottom',
                            ),
                            _table_section(
                                'Sell-through Table', 'inventory-sell-table',
                                'inventory-sell-export', SELL_THROUGH_COLUMNS, 'sell_through.csv',
                            ),
                        ], gap='md'),
                        value='sell-through',
                    ),

                    # ── ABC Analysis ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            dmc.Paper(
                                dmc.Group([
                                    dmc.Stack([
                                        dmc.Text('From:', fw=600),
                                        dmc.DatePickerInput(
                                            value=default_start,
                                            placeholder='Select date',
                                            minDate=STOCK_LEDGER_BASELINE_DATE,
                                            maxDate=today,
                                            id='inventory-abc-date-from',
                                        ),
                                    ], gap=4),
                                    dmc.Stack([
                                        dmc.Text('Until:', fw=600),
                                        dmc.DatePickerInput(
                                            value=today,
                                            placeholder='Select date',
                                            minDate=STOCK_LEDGER_BASELINE_DATE,
                                            maxDate=today,
                                            id='inventory-abc-date-until',
                                        ),
                                    ], gap=4),
                                    dmc.Button(
                                        'Apply',
                                        id='inventory-abc-apply',
                                        variant='filled',
                                        size='sm',
                                    ),
                                ], gap='xl', align='flex-end', wrap='wrap'),
                                p='md', radius='md', withBorder=True, mt='md',
                            ),
                            dmc.Grid([
                                _abc_kpi_card(
                                    'Class A SKUs',
                                    'inventory-abc-kpi-a-count', 'inventory-abc-kpi-a-share',
                                    'green',
                                ),
                                _abc_kpi_card(
                                    'Class B SKUs',
                                    'inventory-abc-kpi-b-count', 'inventory-abc-kpi-b-share',
                                    'orange',
                                ),
                                _abc_kpi_card(
                                    'Class C SKUs',
                                    'inventory-abc-kpi-c-count', 'inventory-abc-kpi-c-share',
                                    'red',
                                ),
                            ], gutter={'base': 'md', 'lg': 'lg'}, mt='md'),
                            _chart_grid(
                                'ABC Pareto Curve', 'inventory-abc-pareto',
                                'ABC Distribution by Category', 'inventory-abc-category',
                            ),
                            _table_section(
                                'ABC Product Table', 'inventory-abc-table',
                                'inventory-abc-export', ABC_COLUMNS, 'abc_products.csv',
                            ),
                        ], gap='md'),
                        value='abc-analysis',
                    ),
                ],
                value='stock-levels',       # Fix #5: default to first tab
                id='inventory-tabs',        # Fix #4: needed for tab-aware callbacks
                mt='md',
            ),
        ],
        size='100%',
        px='md',
        py='lg',
    )


# ── Helpers ───────────────────────────────────────────────────────────

def _parse_date(date_value):
    """Coerce a callback date value to ``date`` or ``None``."""
    if not date_value:
        return None
    if isinstance(date_value, datetime):
        return date_value.date()
    if isinstance(date_value, date):
        return date_value
    try:
        return date.fromisoformat(date_value)
    except (ValueError, TypeError):
        return None


def _resolve_date_range(raw_from, raw_until):
    """Parse, validate, and auto-swap a from/until pair."""
    start = _parse_date(raw_from) or (date.today() - timedelta(days=DEFAULT_LOOKBACK))
    end = _parse_date(raw_until) or date.today()
    if start > end:
        start, end = end, start
    return start, end


def _safe_label(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _normalize_number(value, abs_tol=1e-9):
    """Cast to float, snapping near-zero to 0.0."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if abs(num) <= abs_tol else num


def _format_snapshot_label(snapshot_date, prefix='Snapshot'):
    if not snapshot_date:
        return f'{prefix}: —'
    return f"{prefix}: {snapshot_date.strftime('%d %b %Y')}"


def _format_stock_snapshot_label(stock_result):
    as_of_ts = stock_result.get('as_of_ts')
    if not as_of_ts:
        return 'Snapshot: —'
    baseline_ts = stock_result.get('baseline_ts')
    location_id = stock_result.get('location_id')
    bl = baseline_ts.strftime('%d %b %Y %H:%M') if baseline_ts else '—'
    loc = str(location_id) if location_id is not None else '—'
    return (
        f"Snapshot: {as_of_ts.strftime('%d %b %Y %H:%M')} (UTC+07) · "
        f"Loc {loc} · Baseline {bl}"
    )


def _empty_row(columns):
    """Single placeholder row for an AG Grid empty state."""
    row = {}
    for col in columns:
        field = col['field']
        row[field] = '' if field in TEXT_FIELDS else 0
    row[columns[0]['field']] = 'No data available'
    return [row]


def _empty_figure():
    return {}


# ── Row builders (consistent normalisation across all tabs) ───────────

def _build_stock_row(row):
    flags = []
    if bool(row.get('low_stock_flag')):
        flags.append('Low')
    if bool(row.get('dead_stock_flag')):
        flags.append('Dead')
    return {
        'product_name': _safe_label(row.get('product_name'), f"Product {row.get('product_id', '')}"),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'product_brand': _safe_label(row.get('product_brand'), 'Unknown Brand'),
        'on_hand_qty': _normalize_number(row.get('on_hand_qty', 0)),
        'reserved_qty': _normalize_number(row.get('reserved_qty', 0)),
        'avg_daily_sold': _normalize_number(row.get('avg_daily_sold', 0)),
        'days_of_cover': row.get('days_of_cover'),          # None = infinite cover
        'flags': ', '.join(flags) if flags else '—',
    }


def _build_sell_row(row):
    return {
        'product_name': _safe_label(row.get('product_name'), f"Product {row.get('product_id', '')}"),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'product_brand': _safe_label(row.get('product_brand'), 'Unknown Brand'),
        'begin_on_hand': _normalize_number(row.get('begin_on_hand', 0)),
        'units_received': _normalize_number(row.get('units_received', 0)),
        'units_sold': _normalize_number(row.get('units_sold', 0)),
        'sell_through': _normalize_number(row.get('sell_through', 0)),
    }


def _build_abc_row(row):
    return {
        'product_name': _safe_label(row.get('product_name'), f"Product {row.get('product_id', '')}"),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'product_brand': _safe_label(row.get('product_brand'), 'Unknown Brand'),
        'revenue': _normalize_number(row.get('revenue', 0)),
        'quantity': _normalize_number(row.get('quantity', 0)),
        'cumulative_share': _normalize_number(row.get('cumulative_share', 0)),
        'abc_class': _safe_label(row.get('abc_class'), 'C'),
    }


def _is_active_tab(required_tab):
    """Return True when the callback was triggered by Apply or by switching TO the required tab."""
    ctx = dash.callback_context
    trigger = ctx.triggered_id
    # Apply button or initial call → always proceed
    if trigger != 'inventory-tabs':
        return True
    # Tab switch → only proceed if it's ours
    for t in ctx.triggered:
        if t.get('value') == required_tab:
            return True
    # Fallback: check the Input value (second positional arg in all data callbacks)
    # The active_tab arg is passed directly by the callback signature, so we
    # just compare against the required tab.  This branch is reached when
    # ctx.triggered has the new tab value embedded differently across Dash versions.
    return False


# ── Data callbacks ────────────────────────────────────────────────────

@dash.callback(
    Output('inventory-stock-cover', 'figure'),
    Output('inventory-stock-low', 'figure'),
    Output('inventory-stock-table', 'rowData'),
    Output('inventory-stock-kpi-onhand', 'children'),
    Output('inventory-stock-kpi-low', 'children'),
    Output('inventory-stock-kpi-dead', 'children'),
    Output('inventory-stock-snapshot-label', 'children'),
    Input('inventory-stock-apply', 'n_clicks'),
    Input('inventory-tabs', 'value'),
    State('inventory-stock-date', 'value'),
    prevent_initial_call=False,          # fires once on page load (default tab)
)
def update_stock_levels(n_clicks, active_tab, date_value):
    if dash.callback_context.triggered_id == 'inventory-tabs' and active_tab != 'stock-levels':
        raise dash.exceptions.PreventUpdate

    try:
        as_of_date = _parse_date(date_value) or date.today()
        as_of_date = max(as_of_date, STOCK_LEDGER_BASELINE_DATE)

        stock_result = get_stock_levels_ledger(as_of_date)
        items_df = stock_result['items']
        summary = stock_result['summary']
        display_date = stock_result.get('snapshot_date') or as_of_date

        cover_fig = build_stock_cover_distribution_chart(
            items_df, display_date,
            summary.get('lookback_days', DEFAULT_STOCK_LOOKBACK_DAYS),
            summary.get('low_stock_days', DEFAULT_LOW_STOCK_DAYS),
        )
        low_fig = build_low_stock_chart(
            items_df, display_date,
            summary.get('low_stock_days', DEFAULT_LOW_STOCK_DAYS),
        )

        if items_df.empty:
            row_data = _empty_row(STOCK_COLUMNS)
        else:
            sorted_df = items_df.sort_values('on_hand_qty', ascending=False)
            row_data = [_build_stock_row(r) for _, r in sorted_df.iterrows()]

        return (
            cover_fig,
            low_fig,
            row_data,
            f"{_normalize_number(summary.get('total_on_hand', 0)):,.0f}",
            f"{summary.get('low_stock_count', 0):,}",
            f"{summary.get('dead_stock_count', 0):,}",
            _format_stock_snapshot_label(stock_result),
        )

    except Exception as exc:
        return (
            _empty_figure(), _empty_figure(),
            _empty_row(STOCK_COLUMNS),
            '—', '—', '—',
            f'Error: {exc}',
        )


@dash.callback(
    Output('inventory-sell-category', 'figure'),
    Output('inventory-sell-top-bottom', 'figure'),
    Output('inventory-sell-table', 'rowData'),
    Output('inventory-sell-kpi-sellthrough', 'children'),
    Output('inventory-sell-kpi-sold', 'children'),
    Output('inventory-sell-kpi-received', 'children'),
    Output('inventory-sell-kpi-begin', 'children'),
    Output('inventory-sell-snapshot-label', 'children'),
    Input('inventory-sell-apply', 'n_clicks'),
    Input('inventory-tabs', 'value'),
    State('inventory-sell-date-from', 'value'),
    State('inventory-sell-date-until', 'value'),
    prevent_initial_call=True,           # only fires on tab switch or Apply
)
def update_sell_through(n_clicks, active_tab, date_from, date_until):
    if dash.callback_context.triggered_id == 'inventory-tabs' and active_tab != 'sell-through':
        raise dash.exceptions.PreventUpdate

    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        sell_result = get_sell_through_analysis(start_date, end_date)
        items_df = sell_result['items']
        categories_df = sell_result['categories']
        summary = sell_result['summary']

        category_fig = build_sell_through_by_category_chart(categories_df, start_date, end_date)
        top_bottom_fig = build_sell_through_top_bottom_chart(items_df, start_date, end_date)

        if items_df.empty:
            row_data = _empty_row(SELL_THROUGH_COLUMNS)
        else:
            sorted_df = items_df.sort_values('units_sold', ascending=False)
            row_data = [_build_sell_row(r) for _, r in sorted_df.iterrows()]

        return (
            category_fig,
            top_bottom_fig,
            row_data,
            f"{summary.get('sell_through', 0):.1%}",
            f"{summary.get('units_sold', 0):,.0f}",
            f"{summary.get('units_received', 0):,.0f}",
            f"{summary.get('begin_on_hand', 0):,.0f}",
            _format_snapshot_label(sell_result.get('snapshot_date')),
        )

    except Exception as exc:
        return (
            _empty_figure(), _empty_figure(),
            _empty_row(SELL_THROUGH_COLUMNS),
            '—', '—', '—', '—',
            f'Error: {exc}',
        )


@dash.callback(
    Output('inventory-abc-pareto', 'figure'),
    Output('inventory-abc-category', 'figure'),
    Output('inventory-abc-table', 'rowData'),
    Output('inventory-abc-kpi-a-count', 'children'),
    Output('inventory-abc-kpi-b-count', 'children'),
    Output('inventory-abc-kpi-c-count', 'children'),
    Output('inventory-abc-kpi-a-share', 'children'),
    Output('inventory-abc-kpi-b-share', 'children'),
    Output('inventory-abc-kpi-c-share', 'children'),
    Input('inventory-abc-apply', 'n_clicks'),
    Input('inventory-tabs', 'value'),
    State('inventory-abc-date-from', 'value'),
    State('inventory-abc-date-until', 'value'),
    prevent_initial_call=True,           # only fires on tab switch or Apply
)
def update_abc_analysis(n_clicks, active_tab, date_from, date_until):
    if dash.callback_context.triggered_id == 'inventory-tabs' and active_tab != 'abc-analysis':
        raise dash.exceptions.PreventUpdate

    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        abc_result = get_abc_analysis(start_date, end_date)
        items_df = abc_result['items']
        summary_df = abc_result['summary']
        categories_df = abc_result['categories']

        pareto_fig = build_abc_pareto_chart(items_df, start_date, end_date)
        category_fig = build_abc_category_distribution_chart(categories_df, start_date, end_date)

        lookup = {
            r.get('abc_class'): r for r in summary_df.to_dict('records')
        }

        def _kpi(label):
            row = lookup.get(label, {})
            return (
                int(row.get('sku_count', 0) or 0),
                float(row.get('revenue_share', 0) or 0),
            )

        a_count, a_share = _kpi('A')
        b_count, b_share = _kpi('B')
        c_count, c_share = _kpi('C')

        if items_df.empty:
            row_data = _empty_row(ABC_COLUMNS)
        else:
            sorted_df = items_df.sort_values('revenue', ascending=False)
            row_data = [_build_abc_row(r) for _, r in sorted_df.iterrows()]

        return (
            pareto_fig,
            category_fig,
            row_data,
            f'{a_count:,}', f'{b_count:,}', f'{c_count:,}',
            f'Revenue share: {a_share:.1%}',
            f'Revenue share: {b_share:.1%}',
            f'Revenue share: {c_share:.1%}',
        )

    except Exception:
        empty = _empty_row(ABC_COLUMNS)
        return (
            _empty_figure(), _empty_figure(), empty,
            '—', '—', '—',
            'Revenue share: —', 'Revenue share: —', 'Revenue share: —',
        )


# ── CSV export callbacks ─────────────────────────────────────────────

@dash.callback(
    Output('inventory-stock-table', 'exportDataAsCsv'),
    Input('inventory-stock-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_stock_csv(_):
    return True


@dash.callback(
    Output('inventory-sell-table', 'exportDataAsCsv'),
    Input('inventory-sell-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_sell_csv(_):
    return True


@dash.callback(
    Output('inventory-abc-table', 'exportDataAsCsv'),
    Input('inventory-abc-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_abc_csv(_):
    return True