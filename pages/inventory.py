import dash
from dash import dcc, Output, Input, State
import dash_mantine_components as dmc
import dash_ag_grid as dag
from datetime import date, datetime, timedelta
import io
import logging
import numpy as np
import pandas as pd

from services.duckdb_connector import get_duckdb_connection
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

logger = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path='/inventory',
    name='Inventory Management',
    title='Inventory Health',
)

DEFAULT_LOOKBACK = 30
CHART_HEIGHT = '300px'
TEXT_FIELDS = frozenset({
    'product_name', 'product_category', 'product_brand', 'flags', 'abc_class',
})

# ── Executive-summary thresholds ──────────────────────────────────────
REORDER_TARGET_DAYS = 30
REORDER_ALERT_DAYS = 14
OVERSTOCK_DAYS = 90


# ── Reusable layout builders ─────────────────────────────────────────

def _abc_kpi_card(title, count_id, share_id, share_color):
    return dmc.GridCol(
        dmc.Paper(
            dmc.Stack([
                dmc.Text(title, size='xs', c='dimmed'),
                dmc.Text('—', size='lg', fw=700, id=count_id),
                dmc.Text('Revenue share: —', size='xs', c=share_color, id=share_id),
            ], gap=4),
            p='sm', radius='sm', withBorder=True,
        ),
        span={'base': 12, 'sm': 4},
    )


def _simple_kpi_card(title, value_id, span_sm):
    return dmc.GridCol(
        dmc.Paper(
            dmc.Stack([
                dmc.Text(title, size='xs', c='dimmed'),
                dmc.Text('—', size='lg', fw=700, id=value_id),
            ], gap=4),
            p='sm', radius='sm', withBorder=True,
        ),
        span={'base': 6, 'sm': span_sm},
    )


def _chart_grid(left_title, left_id, right_title, right_id):
    return dmc.Grid(
        [
            dmc.GridCol(
                dmc.Paper(
                    dmc.Stack([
                        dmc.Text(left_title, fw=600, size='sm'),
                        dcc.Loading(
                            dcc.Graph(
                                id=left_id,
                                figure={},
                                config={'displayModeBar': False},
                                style={'height': CHART_HEIGHT, 'width': '100%'},
                            ),
                            type='dot',
                        ),
                    ], gap=4),
                    p='sm', radius='sm', withBorder=True, style={'height': '100%'},
                ),
                span={'base': 12, 'sm': 7},
            ),
            dmc.GridCol(
                dmc.Paper(
                    dmc.Stack([
                        dmc.Text(right_title, fw=600, size='sm'),
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
                    ], gap=4),
                    p='sm', radius='sm', withBorder=True, style={'height': '100%'},
                ),
                span={'base': 12, 'sm': 5},
            ),
        ],
        gutter='sm',
    )


def _table_section(title, table_id, export_id, column_defs, csv_filename):
    return dmc.Paper(
        dmc.Stack([
            dmc.Group([
                dmc.Text(title, fw=600, size='sm'),
                dmc.Button('Export CSV', id=export_id, variant='subtle', size='compact-xs'),
            ], justify='space-between'),
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
                        style={'width': '100%', 'height': '100%'},
                        dashGridOptions={
                            'pagination': True,
                            'paginationPageSize': 50,
                            'enableCellTextSelection': True,
                            'animateRows': True,
                        },
                        csvExportParams={'fileName': csv_filename},
                    ),
                    h=380,
                    style={'height': '100%', 'width': '100%', 'minWidth': 0, 'overflow': 'hidden'},
                ),
                type='dot',
            ),
        ], gap='xs'),
        p='sm', radius='sm', withBorder=True,
        style={'minWidth': 0, 'overflow': 'hidden'},
    )


def _action_table(title, subtitle, table_id, count_id, export_id, export_xlsx_id,
                   column_defs, border_color, badge_color, csv_filename):
    return dmc.Paper(
        dmc.Stack([
            dmc.Group([
                dmc.Group([
                    dmc.Text(title, fw=700, size='md'),
                    dmc.Badge(
                        '—', id=count_id,
                        color=badge_color, variant='light', size='sm',
                    ),
                ], gap='xs', align='center'),
                dmc.Group([
                    dmc.Button('Export CSV', id=export_id, variant='subtle', size='compact-xs'),
                    dmc.Button('Export XLSX', id=export_xlsx_id, variant='subtle', size='compact-xs'),
                ], gap=4),
            ], justify='space-between', align='center'),
            dmc.Text(subtitle, size='xs', c='dimmed'),
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
                        style={'width': '100%', 'height': '100%'},
                        dashGridOptions={
                            'pagination': True,
                            'paginationPageSize': 10,
                            'enableCellTextSelection': True,
                            'animateRows': True,
                        },
                        csvExportParams={'fileName': csv_filename},
                    ),
                    h=300, style={'height': '100%', 'width': '100%', 'minWidth': 0, 'overflow': 'hidden'},
                ),
                type='dot',
            ),
        ], gap='xs'),
        p='sm', radius='sm', withBorder=True,
        style={'borderLeft': f'4px solid {border_color}', 'minWidth': 0, 'overflow': 'hidden'},
    )


# ── Inline date-range control ────────────────────────────────────────

def _date_range_bar(*controls, snapshot_id=None, extra_text=None):
    """Compact horizontal bar with date pickers, action button, and snapshot."""
    items = list(controls)
    if snapshot_id:
        items.append(
            dmc.Text('Snapshot: —', size='xs', c='dimmed', id=snapshot_id),
        )
    if extra_text:
        items.append(dmc.Text(extra_text, size='xs', c='dimmed'))
    return dmc.Paper(
        dmc.Group(items, gap='sm', align='flex-end', wrap='wrap'),
        p='xs', radius='sm', withBorder=True,
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
        'field': 'flags', 'headerName': 'Status',
        'filter': 'agTextColumnFilter',
        'minWidth': 120,
        'cellStyle': {
            'function': (
                "if (params.value && params.value.includes('Dead')) "
                "{ return {color: '#e03131', fontWeight: '600'}; } "
                "if (params.value && params.value.includes('Low')) "
                "{ return {color: '#e8590c', fontWeight: '600'}; } "
                "return {color: '#868e96'};"
            ),
        },
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

# ── Executive-summary column definitions ──────────────────────────────

REORDER_COLUMNS = [
    {
        'field': 'product_name', 'headerName': 'SKU',
        'filter': 'agTextColumnFilter', 'minWidth': 200,
    },
    {
        'field': 'product_category', 'headerName': 'Category',
        'filter': 'agTextColumnFilter', 'minWidth': 130,
    },
    {
        'field': 'abc_class', 'headerName': 'Class',
        'filter': 'agTextColumnFilter', 'minWidth': 70,
        'cellStyle': {
            'function': (
                "params.value === 'A' ? {color:'#2f9e44',fontWeight:600} "
                ": params.value === 'B' ? {color:'#e8590c',fontWeight:600} : {}"
            ),
        },
    },
    {
        'field': 'on_hand_qty', 'headerName': 'On Hand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 90,
    },
    {
        'field': 'avg_daily_sold', 'headerName': 'Daily Demand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(1) : "0.0"',
        },
        'minWidth': 110,
    },
    {
        'field': 'days_of_cover', 'headerName': 'Days Left',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(0) : "0"',
        },
        'minWidth': 90,
        'cellStyle': {
            'function': (
                "params.value != null && params.value < 7 "
                "? {color:'#e03131',fontWeight:600} "
                ": params.value != null && params.value < 14 "
                "? {color:'#e8590c',fontWeight:600} : {}"
            ),
        },
    },
    {
        'field': 'reorder_qty', 'headerName': 'Suggested Order',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 130,
        'cellStyle': {'fontWeight': '600', 'color': '#1971c2'},
    },
    {
        'field': 'revenue', 'headerName': 'Period Revenue',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function':
                'params.value != null ? "Rp " + params.value.toLocaleString() : "Rp 0"',
        },
        'minWidth': 130,
    },
]

MARKDOWN_COLUMNS = [
    {
        'field': 'product_name', 'headerName': 'SKU',
        'filter': 'agTextColumnFilter', 'minWidth': 200,
    },
    {
        'field': 'product_category', 'headerName': 'Category',
        'filter': 'agTextColumnFilter', 'minWidth': 130,
    },
    {
        'field': 'on_hand_qty', 'headerName': 'On Hand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 90,
    },
    {
        'field': 'days_of_cover', 'headerName': 'Days of Cover',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(0) : "∞"',
        },
        'minWidth': 120,
    },
    {
        'field': 'avg_daily_sold', 'headerName': 'Daily Demand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(1) : "0.0"',
        },
        'minWidth': 110,
    },
    {
        'field': 'est_stock_value', 'headerName': 'Est. Stock Value',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function':
                'params.value != null '
                '? "Rp " + Math.round(params.value).toLocaleString() : "—"',
        },
        'minWidth': 140,
    },
    {
        'field': 'revenue', 'headerName': 'Period Revenue',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function':
                'params.value != null ? "Rp " + params.value.toLocaleString() : "Rp 0"',
        },
        'minWidth': 130,
    },
]

TOP_PERFORMERS_COLUMNS = [
    {
        'field': 'product_name', 'headerName': 'SKU',
        'filter': 'agTextColumnFilter', 'minWidth': 200,
    },
    {
        'field': 'product_category', 'headerName': 'Category',
        'filter': 'agTextColumnFilter', 'minWidth': 130,
    },
    {
        'field': 'abc_class', 'headerName': 'Class',
        'filter': 'agTextColumnFilter', 'minWidth': 70,
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
        'field': 'on_hand_qty', 'headerName': 'On Hand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toLocaleString() : "0"',
        },
        'minWidth': 90,
    },
    {
        'field': 'days_of_cover', 'headerName': 'Days of Cover',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(0) : "∞"',
        },
        'minWidth': 110,
    },
    {
        'field': 'avg_daily_sold', 'headerName': 'Daily Demand',
        'type': 'numericColumn', 'filter': 'agNumberColumnFilter',
        'valueFormatter': {
            'function': 'params.value != null ? params.value.toFixed(1) : "0.0"',
        },
        'minWidth': 110,
    },
]


# ── Layout ────────────────────────────────────────────────────────────

def layout():
    today = date.today()
    default_start = max(
        today - timedelta(days=DEFAULT_LOOKBACK), STOCK_LEDGER_BASELINE_DATE,
    )

    return dmc.Container(
        [
            dcc.Download(id='exec-reorder-xlsx-download'),
            dcc.Download(id='exec-markdown-xlsx-download'),
            dcc.Download(id='exec-top-xlsx-download'),
            # ── Header + global filters on one row ────────────────
            dmc.Group([
                dmc.Stack([
                    dmc.Title('Inventory Health', order=3),
                    dmc.Text(
                        'Stock levels · Sell-through · ABC analysis',
                        c='dimmed', size='xs',
                    ),
                ], gap=0),
            ], justify='space-between', align='flex-start'),

            dmc.Paper(
                dmc.Group(
                    [
                        dmc.MultiSelect(
                            id='global-distributor-filter',
                            placeholder='Distributor',
                            data=[], searchable=True, clearable=True,
                            w=200, size='xs',
                        ),
                        dmc.MultiSelect(
                            id='global-category-filter',
                            placeholder='Category',
                            data=[], searchable=True, clearable=True,
                            w=200, size='xs',
                        ),
                        dmc.MultiSelect(
                            id='global-brand-filter',
                            placeholder='Brand',
                            data=[], searchable=True, clearable=True,
                            w=200, size='xs',
                        ),
                        dmc.TextInput(
                            id='global-sku-search',
                            placeholder='Search Barcode',
                            w=160, size='xs',
                        ),
                    ],
                    gap='xs',
                    align='flex-end',
                    wrap='wrap',
                ),
                p='xs',
                radius='sm',
                withBorder=True,
                mt='xs',
            ),

            dmc.Tabs(
                [
                    dmc.TabsList([
                        dmc.TabsTab('Action Items', value='actions'),
                        dmc.TabsTab('Stock Levels', value='stock-levels'),
                        dmc.TabsTab('Sell-through', value='sell-through'),
                        dmc.TabsTab('ABC Analysis', value='abc-analysis'),
                    ]),

                    # ── Action Items ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            _date_range_bar(
                                dmc.DatePickerInput(
                                    value=default_start, placeholder='From',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='exec-date-from',
                                    size='xs', w=140,
                                ),
                                dmc.DatePickerInput(
                                    value=today, placeholder='Until',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='exec-date-until',
                                    size='xs', w=140,
                                ),
                                dmc.Button(
                                    'Refresh', id='exec-apply',
                                    variant='filled', size='compact-sm',
                                ),
                                snapshot_id='exec-snapshot-label',
                            ),
                            dmc.Grid([
                                _simple_kpi_card('Need Attention', 'exec-kpi-attention', 3),
                                _simple_kpi_card('Revenue at Risk', 'exec-kpi-revenue-risk', 3),
                                _simple_kpi_card('Capital Locked', 'exec-kpi-capital-locked', 3),
                                _simple_kpi_card('Healthy SKUs', 'exec-kpi-healthy', 3),
                            ], gutter='xs'),
                            _action_table(
                                'Reorder Now',
                                f'A/B items < {REORDER_ALERT_DAYS}d cover · order to {REORDER_TARGET_DAYS}d',
                                'exec-reorder-table', 'exec-reorder-count',
                                'exec-reorder-export', 'exec-reorder-export-xlsx', REORDER_COLUMNS,
                                '#e03131', 'red', 'reorder_now.csv',
                            ),
                            _action_table(
                                'Consider Promo',
                                f'C items > {OVERSTOCK_DAYS}d cover or zero sales · review for promo, push, clearance',
                                'exec-markdown-table', 'exec-markdown-count',
                                'exec-markdown-export', 'exec-markdown-export-xlsx', MARKDOWN_COLUMNS,
                                '#e8590c', 'orange', 'consider_markdown.csv',
                            ),
                            _action_table(
                                'Top Performers',
                                'Class A with healthy stock · no action needed',
                                'exec-top-table', 'exec-top-count',
                                'exec-top-export', 'exec-top-export-xlsx', TOP_PERFORMERS_COLUMNS,
                                '#2f9e44', 'teal', 'top_performers.csv',
                            ),
                        ], gap='sm'),
                        value='actions',
                    ),

                    # ── Stock Levels ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            _date_range_bar(
                                dmc.DatePickerInput(
                                    value=today, placeholder='As of',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='inventory-stock-date',
                                    size='xs', w=140,
                                ),
                                dmc.Button(
                                    'Apply', id='inventory-stock-apply',
                                    variant='filled', size='compact-sm',
                                ),
                                snapshot_id='inventory-stock-snapshot-label',
                                extra_text=(
                                    f'Lookback {DEFAULT_STOCK_LOOKBACK_DAYS}d'
                                    f' · Low stock {DEFAULT_LOW_STOCK_DAYS}d'
                                ),
                            ),
                            dmc.Grid([
                                _simple_kpi_card('Total On-hand', 'inventory-stock-kpi-onhand', 4),
                                _simple_kpi_card('Low Stock SKUs', 'inventory-stock-kpi-low', 4),
                                _simple_kpi_card('Dead Stock SKUs', 'inventory-stock-kpi-dead', 4),
                            ], gutter='xs'),
                            dmc.Group(
                                [
                                    dmc.Button('All', id='stock-filter-all', variant='light', size='compact-xs'),
                                    dmc.Button('Low Stock', id='stock-filter-low', variant='light', size='compact-xs', color='orange'),
                                    dmc.Button('Dead Stock', id='stock-filter-dead', variant='light', size='compact-xs', color='red'),
                                    dmc.Button('Healthy', id='stock-filter-healthy', variant='light', size='compact-xs', color='green'),
                                ],
                                gap=4,
                            ),
                            _chart_grid(
                                'Days of Cover Distribution', 'inventory-stock-cover',
                                'Lowest Days of Cover', 'inventory-stock-low',
                            ),
                            _table_section(
                                'Stock Levels', 'inventory-stock-table',
                                'inventory-stock-export', STOCK_COLUMNS, 'stock_levels.csv',
                            ),
                        ], gap='sm'),
                        value='stock-levels',
                    ),

                    # ── Sell-through ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            _date_range_bar(
                                dmc.DatePickerInput(
                                    value=default_start, placeholder='From',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='inventory-sell-date-from',
                                    size='xs', w=140,
                                ),
                                dmc.DatePickerInput(
                                    value=today, placeholder='Until',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='inventory-sell-date-until',
                                    size='xs', w=140,
                                ),
                                dmc.Button(
                                    'Apply', id='inventory-sell-apply',
                                    variant='filled', size='compact-sm',
                                ),
                                snapshot_id='inventory-sell-snapshot-label',
                            ),
                            dmc.Grid([
                                _simple_kpi_card('Sell-through %', 'inventory-sell-kpi-sellthrough', 3),
                                _simple_kpi_card('Units Sold', 'inventory-sell-kpi-sold', 3),
                                _simple_kpi_card('Units Received', 'inventory-sell-kpi-received', 3),
                                _simple_kpi_card('Begin On-hand', 'inventory-sell-kpi-begin', 3),
                            ], gutter='xs'),
                            _chart_grid(
                                'Sell-through by Category', 'inventory-sell-category',
                                'Top / Bottom Sell-through', 'inventory-sell-top-bottom',
                            ),
                            _table_section(
                                'Sell-through', 'inventory-sell-table',
                                'inventory-sell-export', SELL_THROUGH_COLUMNS, 'sell_through.csv',
                            ),
                        ], gap='sm'),
                        value='sell-through',
                    ),

                    # ── ABC Analysis ──────────────────────────────────
                    dmc.TabsPanel(
                        dmc.Stack([
                            _date_range_bar(
                                dmc.DatePickerInput(
                                    value=default_start, placeholder='From',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='inventory-abc-date-from',
                                    size='xs', w=140,
                                ),
                                dmc.DatePickerInput(
                                    value=today, placeholder='Until',
                                    minDate=STOCK_LEDGER_BASELINE_DATE,
                                    maxDate=today, id='inventory-abc-date-until',
                                    size='xs', w=140,
                                ),
                                dmc.Button(
                                    'Apply', id='inventory-abc-apply',
                                    variant='filled', size='compact-sm',
                                ),
                            ),
                            dmc.Grid([
                                _abc_kpi_card('Class A', 'inventory-abc-kpi-a-count', 'inventory-abc-kpi-a-share', 'green'),
                                _abc_kpi_card('Class B', 'inventory-abc-kpi-b-count', 'inventory-abc-kpi-b-share', 'orange'),
                                _abc_kpi_card('Class C', 'inventory-abc-kpi-c-count', 'inventory-abc-kpi-c-share', 'red'),
                            ], gutter='xs'),
                            _chart_grid(
                                'ABC Pareto Curve', 'inventory-abc-pareto',
                                'ABC by Category', 'inventory-abc-category',
                            ),
                            _table_section(
                                'ABC Products', 'inventory-abc-table',
                                'inventory-abc-export', ABC_COLUMNS, 'abc_products.csv',
                            ),
                        ], gap='sm'),
                        value='abc-analysis',
                    ),
                ],
                value='actions',
                id='inventory-tabs',
                mt='sm',
            ),
        ],
        size='100%',
        px='sm',
        py='md',
    )


# ── Helpers ───────────────────────────────────────────────────────────

def _parse_date(date_value):
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


def _format_currency_short(value):
    v = _normalize_number(value)
    if v <= 0:
        return 'Rp 0'
    if v >= 1_000_000_000:
        return f'Rp {v / 1_000_000_000:.1f}B'
    if v >= 1_000_000:
        return f'Rp {v / 1_000_000:.1f}M'
    if v >= 1_000:
        return f'Rp {v / 1_000:.1f}K'
    return f'Rp {v:,.0f}'


def _empty_row(columns):
    row = {}
    for col in columns:
        field = col['field']
        row[field] = '' if field in TEXT_FIELDS else 0
    row[columns[0]['field']] = 'No data available'
    return [row]


def _empty_figure():
    return {}


# ── Row builders ──────────────────────────────────────────────────────

def _build_stock_row(row):
    flags = []
    if bool(row.get('low_stock_flag')):
        flags.append('Low')
    if bool(row.get('dead_stock_flag')):
        flags.append('Dead')
    return {
        'product_name': _safe_label(
            row.get('product_name'), f"Product {row.get('product_id', '')}",
        ),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'product_brand': _safe_label(row.get('product_brand'), 'Unknown Brand'),
        'on_hand_qty': _normalize_number(row.get('on_hand_qty', 0)),
        'reserved_qty': _normalize_number(row.get('reserved_qty', 0)),
        'avg_daily_sold': _normalize_number(row.get('avg_daily_sold', 0)),
        'days_of_cover': row.get('days_of_cover'),
        'flags': ', '.join(flags) if flags else '—',
    }


def _build_sell_row(row):
    return {
        'product_name': _safe_label(
            row.get('product_name'), f"Product {row.get('product_id', '')}",
        ),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'product_brand': _safe_label(row.get('product_brand'), 'Unknown Brand'),
        'begin_on_hand': _normalize_number(row.get('begin_on_hand', 0)),
        'units_received': _normalize_number(row.get('units_received', 0)),
        'units_sold': _normalize_number(row.get('units_sold', 0)),
        'sell_through': _normalize_number(row.get('sell_through', 0)),
    }


def _build_abc_row(row):
    return {
        'product_name': _safe_label(
            row.get('product_name'), f"Product {row.get('product_id', '')}",
        ),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'product_brand': _safe_label(row.get('product_brand'), 'Unknown Brand'),
        'revenue': _normalize_number(row.get('revenue', 0)),
        'quantity': _normalize_number(row.get('quantity', 0)),
        'cumulative_share': _normalize_number(row.get('cumulative_share', 0)),
        'abc_class': _safe_label(row.get('abc_class'), 'C'),
    }


def _build_reorder_row(row):
    return {
        'product_name': _safe_label(
            row.get('product_name'), f"Product {row.get('product_id', '')}",
        ),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'abc_class': _safe_label(row.get('abc_class'), 'C'),
        'on_hand_qty': _normalize_number(row.get('on_hand_qty', 0)),
        'avg_daily_sold': _normalize_number(row.get('avg_daily_sold', 0)),
        'days_of_cover': row.get('days_of_cover'),
        'reorder_qty': int(row.get('reorder_qty', 0) or 0),
        'revenue': _normalize_number(row.get('revenue', 0)),
    }


def _build_markdown_row(row):
    return {
        'product_name': _safe_label(
            row.get('product_name'), f"Product {row.get('product_id', '')}",
        ),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'on_hand_qty': _normalize_number(row.get('on_hand_qty', 0)),
        'days_of_cover': row.get('days_of_cover'),
        'avg_daily_sold': _normalize_number(row.get('avg_daily_sold', 0)),
        'est_stock_value': _normalize_number(row.get('est_stock_value', 0)),
        'revenue': _normalize_number(row.get('revenue', 0)),
    }


def _build_top_row(row):
    return {
        'product_name': _safe_label(
            row.get('product_name'), f"Product {row.get('product_id', '')}",
        ),
        'product_category': _safe_label(row.get('product_category'), 'Unknown Category'),
        'abc_class': _safe_label(row.get('abc_class'), 'A'),
        'revenue': _normalize_number(row.get('revenue', 0)),
        'on_hand_qty': _normalize_number(row.get('on_hand_qty', 0)),
        'days_of_cover': row.get('days_of_cover'),
        'avg_daily_sold': _normalize_number(row.get('avg_daily_sold', 0)),
    }


# ── Global-filter helpers ─────────────────────────────────────────────

def _coerce_str_list(values):
    if not values:
        return []
    if isinstance(values, (list, tuple, set)):
        return [str(v) for v in values if str(v).strip()]
    return [str(values)]


def apply_global_filters(
    df: pd.DataFrame, categories, brands, sku_search, distributors,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    filtered = df.copy()

    category_values = set(_coerce_str_list(categories))
    if category_values:
        filtered = filtered[
            filtered['product_category'].astype(str).isin(category_values)
        ]

    brand_values = set(_coerce_str_list(brands))
    if brand_values:
        filtered = filtered[
            filtered['product_brand'].astype(str).isin(brand_values)
        ]

    distributor_values = set(_coerce_str_list(distributors))
    if distributor_values and 'product_id' in filtered.columns:
        conn = get_duckdb_connection()
        vendor_ids = []
        for raw in distributor_values:
            try:
                vendor_ids.append(int(raw))
            except (TypeError, ValueError):
                continue
        if vendor_ids:
            placeholders = ','.join(['?'] * len(vendor_ids))
            vendor_products_df = conn.execute(
                f"""
                SELECT DISTINCT product_id
                FROM fact_purchases
                WHERE vendor_id IN ({placeholders})
                  AND COALESCE(product_id, 0) != 0
                """,
                vendor_ids,
            ).df()
            product_ids = set(pd.to_numeric(vendor_products_df.get('product_id'), errors='coerce').dropna().astype('int64').tolist())
            if product_ids:
                filtered = filtered[filtered['product_id'].isin(product_ids)]
            else:
                filtered = filtered.iloc[0:0]

    query = (sku_search or '').strip()
    if query:
        barcode_match = (
            filtered.get('product_barcode', pd.Series(index=filtered.index, dtype='object'))
            .astype(str)
            .str.contains(query, case=False, na=False, regex=False)
        )
        filtered = filtered[barcode_match]

    return filtered


# ── Data callbacks ────────────────────────────────────────────────────

@dash.callback(
    Output('exec-kpi-attention', 'children'),
    Output('exec-kpi-revenue-risk', 'children'),
    Output('exec-kpi-capital-locked', 'children'),
    Output('exec-kpi-healthy', 'children'),
    Output('exec-reorder-table', 'rowData'),
    Output('exec-reorder-count', 'children'),
    Output('exec-markdown-table', 'rowData'),
    Output('exec-markdown-count', 'children'),
    Output('exec-top-table', 'rowData'),
    Output('exec-top-count', 'children'),
    Output('exec-snapshot-label', 'children'),
    Input('exec-apply', 'n_clicks'),
    Input('inventory-tabs', 'value'),
    State('exec-date-from', 'value'),
    State('exec-date-until', 'value'),
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=False,
)
def update_exec_summary(
    n_clicks, active_tab, date_from, date_until,
    categories, brands, distributors, sku_search,
):
    if (dash.callback_context.triggered_id == 'inventory-tabs'
            and active_tab != 'actions'):
        raise dash.exceptions.PreventUpdate

    empty = (
        '—', '—', '—', '—',
        _empty_row(REORDER_COLUMNS), '0 items',
        _empty_row(MARKDOWN_COLUMNS), '0 items',
        _empty_row(TOP_PERFORMERS_COLUMNS), '0 items',
        'Snapshot: —',
    )

    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)

        stock_result = get_stock_levels_ledger(end_date)
        stock_df = stock_result['items'].copy()

        abc_result = get_abc_analysis(start_date, end_date)
        abc_df = abc_result['items'].copy()

        if stock_df.empty:
            return empty

        abc_cols = ['product_id', 'abc_class', 'revenue', 'quantity']
        abc_cols = [c for c in abc_cols if c in abc_df.columns]
        abc_subset = abc_df[abc_cols].copy() if not abc_df.empty else pd.DataFrame(
            columns=['product_id', 'abc_class', 'revenue', 'quantity'],
        )

        merged = stock_df.merge(abc_subset, on='product_id', how='left')
        merged['abc_class'] = merged.get('abc_class', pd.Series('C')).fillna('C')
        merged['revenue'] = pd.to_numeric(
            merged.get('revenue', 0), errors='coerce',
        ).fillna(0)
        merged['quantity'] = pd.to_numeric(
            merged.get('quantity', 0), errors='coerce',
        ).fillna(0)
        merged['on_hand_qty'] = pd.to_numeric(
            merged.get('on_hand_qty', 0), errors='coerce',
        ).fillna(0)
        merged['avg_daily_sold'] = pd.to_numeric(
            merged.get('avg_daily_sold', 0), errors='coerce',
        ).fillna(0)
        merged['low_stock_flag'] = (
            merged.get('low_stock_flag', pd.Series(dtype=bool))
            .fillna(False).astype(bool)
        )
        merged['dead_stock_flag'] = (
            merged.get('dead_stock_flag', pd.Series(dtype=bool))
            .fillna(False).astype(bool)
        )

        merged = apply_global_filters(merged, categories, brands, sku_search, distributors)
        if merged.empty:
            return empty

        safe_qty = merged['quantity'].where(merged['quantity'] > 0)
        unit_price = merged['revenue'] / safe_qty
        merged['est_stock_value'] = (unit_price * merged['on_hand_qty']).fillna(0)

        merged['reorder_qty'] = np.ceil(
            (REORDER_TARGET_DAYS * merged['avg_daily_sold'] - merged['on_hand_qty'])
            .clip(lower=0),
        ).fillna(0).astype(int)

        has_cover = merged['days_of_cover'].notna()
        low_cover = has_cover & (merged['days_of_cover'] < REORDER_ALERT_DAYS)
        high_value = merged['abc_class'].isin(['A', 'B'])
        has_demand = merged['avg_daily_sold'] > 0
        out_of_stock = merged['on_hand_qty'] <= 0
        has_stock = merged['on_hand_qty'] > 0
        is_c = merged['abc_class'] == 'C'

        reorder_mask = high_value & (low_cover | (out_of_stock & has_demand))

        excess_cover = (
            (has_cover & (merged['days_of_cover'] > OVERSTOCK_DAYS))
            | (~has_cover & has_stock)
        )
        markdown_mask = is_c & has_stock & (excess_cover | merged['dead_stock_flag'])

        top_mask = (
            (merged['abc_class'] == 'A')
            & ~merged['low_stock_flag']
            & ~merged['dead_stock_flag']
            & has_stock
        )

        reorder_df = merged[reorder_mask].sort_values(
            ['days_of_cover', 'revenue'], ascending=[True, False],
        )
        markdown_df = merged[markdown_mask].sort_values(
            'est_stock_value', ascending=False,
        )
        top_df = merged[top_mask].sort_values('revenue', ascending=False)

        attention_count = int(reorder_mask.sum()) + int(markdown_mask.sum())
        revenue_at_risk = float(reorder_df['revenue'].sum())
        capital_locked = float(markdown_df['est_stock_value'].sum())
        healthy_count = int((~(reorder_mask | markdown_mask) & has_stock).sum())

        def _rows(df, cols, builder):
            if df.empty:
                return _empty_row(cols)
            return [builder(r) for _, r in df.head(200).iterrows()]

        reorder_rows = _rows(reorder_df, REORDER_COLUMNS, _build_reorder_row)
        markdown_rows = _rows(markdown_df, MARKDOWN_COLUMNS, _build_markdown_row)
        top_rows = _rows(top_df, TOP_PERFORMERS_COLUMNS, _build_top_row)

        snapshot_label = (
            f"Stock as of {end_date.strftime('%d %b %Y')} · "
            f"Sales {start_date.strftime('%d %b')} – "
            f"{end_date.strftime('%d %b %Y')}"
        )

        return (
            f'{attention_count:,}',
            _format_currency_short(revenue_at_risk),
            _format_currency_short(capital_locked),
            f'{healthy_count:,}',
            reorder_rows,
            f'{len(reorder_df):,} items',
            markdown_rows,
            f'{len(markdown_df):,} items',
            top_rows,
            f'{len(top_df):,} items',
            snapshot_label,
        )

    except Exception as exc:
        logger.exception('Executive summary callback failed')
        return (
            '—', '—', '—', '—',
            _empty_row(REORDER_COLUMNS), '—',
            _empty_row(MARKDOWN_COLUMNS), '—',
            _empty_row(TOP_PERFORMERS_COLUMNS), '—',
            f'Error: {exc}',
        )


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
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=True,
)
def update_stock_levels(
    n_clicks, active_tab, date_value, categories, brands, distributors, sku_search,
):
    if (dash.callback_context.triggered_id == 'inventory-tabs'
            and active_tab != 'stock-levels'):
        raise dash.exceptions.PreventUpdate

    try:
        as_of_date = _parse_date(date_value) or date.today()
        as_of_date = max(as_of_date, STOCK_LEDGER_BASELINE_DATE)

        stock_result = get_stock_levels_ledger(as_of_date)
        items_df = stock_result['items']
        summary = stock_result['summary']
        display_date = stock_result.get('snapshot_date') or as_of_date

        items_df = apply_global_filters(items_df, categories, brands, sku_search, distributors)

        total_on_hand = (
            float(
                pd.to_numeric(items_df.get('on_hand_qty', 0), errors='coerce')
                .fillna(0).sum(),
            )
            if not items_df.empty
            else 0.0
        )
        low_stock_count = (
            int(
                items_df.get('low_stock_flag', pd.Series(dtype=bool))
                .fillna(False).astype(bool).sum(),
            )
            if not items_df.empty
            else 0
        )
        dead_stock_count = (
            int(
                items_df.get('dead_stock_flag', pd.Series(dtype=bool))
                .fillna(False).astype(bool).sum(),
            )
            if not items_df.empty
            else 0
        )

        summary = dict(summary or {})
        summary['total_on_hand'] = total_on_hand
        summary['low_stock_count'] = low_stock_count
        summary['dead_stock_count'] = dead_stock_count

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
        logger.exception('Stock levels callback failed')
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
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=True,
)
def update_sell_through(
    n_clicks, active_tab, date_from, date_until, categories, brands, distributors, sku_search,
):
    if (dash.callback_context.triggered_id == 'inventory-tabs'
            and active_tab != 'sell-through'):
        raise dash.exceptions.PreventUpdate

    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        sell_result = get_sell_through_analysis(start_date, end_date)
        items_df = sell_result['items']
        categories_df = sell_result['categories']
        summary = sell_result['summary']

        items_df = apply_global_filters(items_df, categories, brands, sku_search, distributors)

        if items_df.empty:
            categories_df = categories_df.head(0).copy()
            summary = {
                'sell_through': 0.0,
                'units_sold': 0.0,
                'units_received': 0.0,
                'begin_on_hand': 0.0,
            }
        else:
            categories_df = (
                items_df
                .groupby('product_category', as_index=False)
                .agg(
                    begin_on_hand=('begin_on_hand', 'sum'),
                    units_received=('units_received', 'sum'),
                    units_sold=('units_sold', 'sum'),
                )
            )
            denom = categories_df['begin_on_hand'] + categories_df['units_received']
            categories_df['sell_through'] = (
                categories_df['units_sold'] / denom.where(denom > 0, pd.NA)
            )
            categories_df['sell_through'] = (
                pd.to_numeric(categories_df['sell_through'], errors='coerce')
                .fillna(0.0)
            )

            total_begin = float(
                pd.to_numeric(items_df.get('begin_on_hand', 0), errors='coerce')
                .fillna(0).sum(),
            )
            total_received = float(
                pd.to_numeric(items_df.get('units_received', 0), errors='coerce')
                .fillna(0).sum(),
            )
            total_sold = float(
                pd.to_numeric(items_df.get('units_sold', 0), errors='coerce')
                .fillna(0).sum(),
            )
            overall = (
                total_sold / (total_begin + total_received)
                if (total_begin + total_received) > 0
                else 0.0
            )
            summary = {
                'sell_through': overall,
                'units_sold': total_sold,
                'units_received': total_received,
                'begin_on_hand': total_begin,
            }

        category_fig = build_sell_through_by_category_chart(
            categories_df, start_date, end_date,
        )
        top_bottom_fig = build_sell_through_top_bottom_chart(
            items_df, start_date, end_date,
        )

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
        logger.exception('Sell-through callback failed')
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
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=True,
)
def update_abc_analysis(
    n_clicks, active_tab, date_from, date_until, categories, brands, distributors, sku_search,
):
    if (dash.callback_context.triggered_id == 'inventory-tabs'
            and active_tab != 'abc-analysis'):
        raise dash.exceptions.PreventUpdate

    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        abc_result = get_abc_analysis(start_date, end_date)
        items_df = abc_result['items']
        summary_df = abc_result['summary']
        categories_df = abc_result['categories']

        items_df = apply_global_filters(items_df, categories, brands, sku_search, distributors)

        if items_df.empty:
            summary_df = summary_df.head(0).copy()
            categories_df = categories_df.head(0).copy()
        else:
            total_revenue = float(
                pd.to_numeric(items_df.get('revenue', 0), errors='coerce')
                .fillna(0).sum(),
            )
            summary_df = (
                items_df
                .groupby('abc_class', as_index=False)
                .agg(
                    sku_count=('product_id', 'count'),
                    revenue=('revenue', 'sum'),
                )
            )
            summary_df['revenue_share'] = (
                summary_df['revenue'] / total_revenue if total_revenue > 0 else 0.0
            )
            summary_df['abc_class'] = pd.Categorical(
                summary_df['abc_class'], ['A', 'B', 'C'], ordered=True,
            )
            summary_df = summary_df.sort_values('abc_class')

            categories_df = (
                items_df
                .groupby(['product_category', 'abc_class'], as_index=False)
                .agg(revenue=('revenue', 'sum'))
            )

        pareto_fig = build_abc_pareto_chart(items_df, start_date, end_date)
        category_fig = build_abc_category_distribution_chart(
            categories_df, start_date, end_date,
        )

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

    except Exception as exc:
        logger.exception('ABC analysis callback failed')
        empty = _empty_row(ABC_COLUMNS)
        return (
            _empty_figure(), _empty_figure(), empty,
            '—', '—', '—',
            'Revenue share: —', 'Revenue share: —', 'Revenue share: —',
        )


# ── CSV export callbacks ─────────────────────────────────────────────

@dash.callback(
    Output('exec-reorder-table', 'exportDataAsCsv'),
    Input('exec-reorder-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_reorder_csv(_):
    return True


@dash.callback(
    Output('exec-markdown-table', 'exportDataAsCsv'),
    Input('exec-markdown-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_markdown_csv(_):
    return True


@dash.callback(
    Output('exec-top-table', 'exportDataAsCsv'),
    Input('exec-top-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_top_csv(_):
    return True


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


# ── XLSX export helper functions ──────────────────────────────────────

def _df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = io.BytesIO()
    safe_sheet_name = (sheet_name or 'Sheet1')[:31]
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
    output.seek(0)
    return output.getvalue()


def _get_exec_merged_df(start_date: date, end_date: date) -> pd.DataFrame:
    stock_result = get_stock_levels_ledger(end_date)
    stock_df = stock_result['items'].copy()
    if stock_df.empty:
        return stock_df

    abc_result = get_abc_analysis(start_date, end_date)
    abc_df = abc_result['items'].copy()

    abc_cols = ['product_id', 'abc_class', 'revenue', 'quantity']
    abc_cols = [c for c in abc_cols if c in abc_df.columns]
    abc_subset = abc_df[abc_cols].copy() if not abc_df.empty else pd.DataFrame(
        columns=['product_id', 'abc_class', 'revenue', 'quantity'],
    )

    merged = stock_df.merge(abc_subset, on='product_id', how='left')
    merged['abc_class'] = merged.get('abc_class', pd.Series('C')).fillna('C')
    merged['revenue'] = pd.to_numeric(
        merged.get('revenue', 0), errors='coerce',
    ).fillna(0)
    merged['quantity'] = pd.to_numeric(
        merged.get('quantity', 0), errors='coerce',
    ).fillna(0)
    merged['on_hand_qty'] = pd.to_numeric(
        merged.get('on_hand_qty', 0), errors='coerce',
    ).fillna(0)
    merged['avg_daily_sold'] = pd.to_numeric(
        merged.get('avg_daily_sold', 0), errors='coerce',
    ).fillna(0)
    merged['low_stock_flag'] = (
        merged.get('low_stock_flag', pd.Series(dtype=bool))
        .fillna(False).astype(bool)
    )
    merged['dead_stock_flag'] = (
        merged.get('dead_stock_flag', pd.Series(dtype=bool))
        .fillna(False).astype(bool)
    )

    safe_qty = merged['quantity'].where(merged['quantity'] > 0)
    unit_price = merged['revenue'] / safe_qty
    merged['est_stock_value'] = (unit_price * merged['on_hand_qty']).fillna(0)

    merged['reorder_qty'] = np.ceil(
        (REORDER_TARGET_DAYS * merged['avg_daily_sold'] - merged['on_hand_qty'])
        .clip(lower=0),
    ).fillna(0).astype(int)

    return merged


def _build_exec_export_df(
    start_date: date,
    end_date: date,
    categories,
    brands,
    distributors,
    sku_search,
    export_kind: str,
) -> pd.DataFrame:
    merged = _get_exec_merged_df(start_date, end_date)
    merged = apply_global_filters(merged, categories, brands, sku_search, distributors)
    if merged.empty:
        return pd.DataFrame()

    has_cover = merged['days_of_cover'].notna()
    low_cover = has_cover & (merged['days_of_cover'] < REORDER_ALERT_DAYS)
    high_value = merged['abc_class'].isin(['A', 'B'])
    has_demand = merged['avg_daily_sold'] > 0
    out_of_stock = merged['on_hand_qty'] <= 0
    has_stock = merged['on_hand_qty'] > 0
    is_c = merged['abc_class'] == 'C'

    reorder_mask = high_value & (low_cover | (out_of_stock & has_demand))
    excess_cover = (
        (has_cover & (merged['days_of_cover'] > OVERSTOCK_DAYS))
        | (~has_cover & has_stock)
    )
    markdown_mask = is_c & has_stock & (excess_cover | merged['dead_stock_flag'])
    top_mask = (
        (merged['abc_class'] == 'A')
        & ~merged['low_stock_flag']
        & ~merged['dead_stock_flag']
        & has_stock
    )

    if export_kind == 'reorder':
        picked = merged[reorder_mask].sort_values(
            ['days_of_cover', 'revenue'], ascending=[True, False],
        )
        return pd.DataFrame([_build_reorder_row(r) for _, r in picked.iterrows()])
    if export_kind == 'markdown':
        picked = merged[markdown_mask].sort_values(
            'est_stock_value', ascending=False,
        )
        return pd.DataFrame([_build_markdown_row(r) for _, r in picked.iterrows()])

    picked = merged[top_mask].sort_values('revenue', ascending=False)
    return pd.DataFrame([_build_top_row(r) for _, r in picked.iterrows()])


# ── XLSX export callbacks ────────────────────────────────────────────

@dash.callback(
    Output('exec-reorder-xlsx-download', 'data'),
    Input('exec-reorder-export-xlsx', 'n_clicks'),
    State('exec-date-from', 'value'),
    State('exec-date-until', 'value'),
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=True,
)
def export_reorder_xlsx(
    _n, date_from, date_until, categories, brands, distributors, sku_search,
):
    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        export_df = _build_exec_export_df(
            start_date, end_date,
            categories, brands, distributors, sku_search,
            export_kind='reorder',
        )
        data = _df_to_xlsx_bytes(export_df, 'Reorder Now')
        return dcc.send_bytes(data, filename='reorder_now_all.xlsx')
    except Exception:
        logger.exception('XLSX export failed for reorder')
        raise dash.exceptions.PreventUpdate


@dash.callback(
    Output('exec-markdown-xlsx-download', 'data'),
    Input('exec-markdown-export-xlsx', 'n_clicks'),
    State('exec-date-from', 'value'),
    State('exec-date-until', 'value'),
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=True,
)
def export_markdown_xlsx(
    _n, date_from, date_until, categories, brands, distributors, sku_search,
):
    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        export_df = _build_exec_export_df(
            start_date, end_date,
            categories, brands, distributors, sku_search,
            export_kind='markdown',
        )
        data = _df_to_xlsx_bytes(export_df, 'Consider Promo')
        return dcc.send_bytes(data, filename='consider_promo_all.xlsx')
    except Exception:
        logger.exception('XLSX export failed for markdown')
        raise dash.exceptions.PreventUpdate


@dash.callback(
    Output('exec-top-xlsx-download', 'data'),
    Input('exec-top-export-xlsx', 'n_clicks'),
    State('exec-date-from', 'value'),
    State('exec-date-until', 'value'),
    State('global-category-filter', 'value'),
    State('global-brand-filter', 'value'),
    State('global-distributor-filter', 'value'),
    State('global-sku-search', 'value'),
    prevent_initial_call=True,
)
def export_top_xlsx(
    _n, date_from, date_until, categories, brands, distributors, sku_search,
):
    try:
        start_date, end_date = _resolve_date_range(date_from, date_until)
        export_df = _build_exec_export_df(
            start_date, end_date,
            categories, brands, distributors, sku_search,
            export_kind='top',
        )
        data = _df_to_xlsx_bytes(export_df, 'Top Performers')
        return dcc.send_bytes(data, filename='top_performers_all.xlsx')
    except Exception:
        logger.exception('XLSX export failed for top performers')
        raise dash.exceptions.PreventUpdate


# ── Populate global filter dropdowns ──────────────────────────────────

@dash.callback(
    Output('global-category-filter', 'data'),
    Output('global-brand-filter', 'data'),
    Output('global-distributor-filter', 'data'),
    Input('inventory-tabs', 'value'),
    prevent_initial_call=False,
)
def populate_global_filter_options(_active_tab):
    conn = get_duckdb_connection()

    categories_df = conn.execute(
        """
        SELECT DISTINCT COALESCE(product_category, 'Unknown Category') AS value
        FROM dim_products
        WHERE COALESCE(product_category, '') <> ''
        ORDER BY 1
        """,
    ).df()
    brands_df = conn.execute(
        """
        SELECT DISTINCT COALESCE(product_brand, 'Unknown Brand') AS value
        FROM dim_products
        WHERE COALESCE(product_brand, '') <> ''
        ORDER BY 1
        """,
    ).df()

    distributors_df = conn.execute(
        """
        SELECT DISTINCT
            CAST(vendor_id AS VARCHAR) AS value,
            COALESCE(NULLIF(TRIM(vendor_name), ''), CAST(vendor_id AS VARCHAR)) AS label
        FROM fact_purchases
        WHERE COALESCE(vendor_id, 0) != 0
        ORDER BY 2
        """,
    ).df()

    category_data = [
        {'value': v, 'label': v}
        for v in categories_df['value'].astype(str).tolist()
    ]
    brand_data = [
        {'value': v, 'label': v}
        for v in brands_df['value'].astype(str).tolist()
    ]
    distributor_data = [
        {'value': row['value'], 'label': row['label']}
        for _, row in distributors_df.iterrows()
    ]
    return category_data, brand_data, distributor_data


# ── Stock quick-filter buttons ────────────────────────────────────────

@dash.callback(
    Output('inventory-stock-table', 'filterModel'),
    Input('stock-filter-all', 'n_clicks'),
    Input('stock-filter-low', 'n_clicks'),
    Input('stock-filter-dead', 'n_clicks'),
    Input('stock-filter-healthy', 'n_clicks'),
    prevent_initial_call=True,
)
def set_stock_quick_filter(_all, _low, _dead, _healthy):
    trigger = dash.callback_context.triggered_id
    if trigger == 'stock-filter-low':
        return {
            'flags': {
                'filterType': 'text',
                'type': 'contains',
                'filter': 'Low',
            },
        }
    if trigger == 'stock-filter-dead':
        return {
            'flags': {
                'filterType': 'text',
                'type': 'contains',
                'filter': 'Dead',
            },
        }
    if trigger == 'stock-filter-healthy':
        return {
            'flags': {
                'filterType': 'text',
                'type': 'equals',
                'filter': '—',
            },
            'on_hand_qty': {
                'filterType': 'number',
                'type': 'greaterThan',
                'filter': 0,
            },
        }
    return {}