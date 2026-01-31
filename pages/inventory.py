import dash
from dash import dcc, Output, Input, State
import dash_mantine_components as dmc
from datetime import date, datetime
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
    title='Inventory Health'
)


def _abc_kpi_card(title: str, count_id: str, share_id: str, share_color: str) -> dmc.GridCol:
    return dmc.GridCol(
        dmc.Paper(
            dmc.Stack([
                dmc.Text(title, size='sm', c='dimmed'),
                dmc.Text('0', size='xl', fw=600, id=count_id),
                dmc.Text('Revenue share: 0%', size='xs', c=share_color, id=share_id),
            ]),
            p='md',
            radius='md',
            withBorder=True,
        ),
        span={"base": 12, "sm": 4},
    )


def _simple_kpi_card(title: str, value_id: str, span_sm: int) -> dmc.GridCol:
    return dmc.GridCol(
        dmc.Paper(
            dmc.Stack([
                dmc.Text(title, size='sm', c='dimmed'),
                dmc.Text('0', size='xl', fw=600, id=value_id),
            ]),
            p='md',
            radius='md',
            withBorder=True,
        ),
        span={"base": 12, "sm": span_sm},
    )


def layout():
    return dmc.Container(
        [
            dmc.Title('Inventory Health', order=2),
            dmc.Text('Stock levels, sell-through, and ABC analysis for inventory performance.', c='dimmed'),
            dmc.Tabs(
                [
                    dmc.TabsList(
                        [
                            dmc.TabsTab('Stock Levels', value='stock-levels'),
                            dmc.TabsTab('Sell-through', value='sell-through'),
                            dmc.TabsTab('ABC Analysis', value='abc-analysis'),
                        ]
                    ),
                    dmc.TabsPanel(
                        dmc.Stack(
                            [
                                dmc.Paper(
                                    dmc.Stack(
                                        [
                                            dmc.Group(
                                                [
                                                    dmc.Stack(
                                                        [
                                                            dmc.Text('As of:', fw=600),
                                                            dmc.DatePickerInput(
                                                                value=date.today(),
                                                                placeholder='Select date',
                                                                minDate=STOCK_LEDGER_BASELINE_DATE,
                                                                id='inventory-stock-date',
                                                            ),
                                                        ],
                                                        gap=4,
                                                    ),
                                                    dmc.Button('Apply', id='inventory-stock-apply', variant='filled', size='sm'),
                                                ],
                                                gap='xl',
                                                align='flex-end',
                                                wrap='wrap',
                                            ),
                                            dmc.Text('Snapshot date: —', size='xs', c='dimmed', id='inventory-stock-snapshot-label'),
                                            dmc.Text(
                                                f"Lookback: {DEFAULT_STOCK_LOOKBACK_DAYS} days · Low stock: {DEFAULT_LOW_STOCK_DAYS} days",
                                                size='xs',
                                                c='dimmed',
                                            ),
                                        ],
                                        gap='xs',
                                    ),
                                    p='md',
                                    radius='md',
                                    withBorder=True,
                                    mt='md',
                                ),
                                dmc.Grid(
                                    [
                                        _simple_kpi_card('Total On-hand Units', 'inventory-stock-kpi-onhand', 4),
                                        _simple_kpi_card('Low Stock SKUs', 'inventory-stock-kpi-low', 4),
                                        _simple_kpi_card('Dead Stock SKUs', 'inventory-stock-kpi-dead', 4),
                                    ],
                                    gutter={"base": "md", "lg": "lg"},
                                    mt='md',
                                ),
                                dmc.Grid(
                                    [
                                        dmc.GridCol(
                                            dmc.Paper(
                                                dmc.Stack(
                                                    [
                                                        dmc.Text('Days of Cover Distribution', fw=600, mb='md'),
                                                        dmc.Container(
                                                            dcc.Graph(
                                                                id='inventory-stock-cover',
                                                                figure={},
                                                                config={'displayModeBar': False},
                                                                style={'height': '100%', 'width': '100%'},
                                                            ),
                                                            p=0,
                                                            fluid=True,
                                                            style={'height': '100%'},
                                                        ),
                                                    ]
                                                ),
                                                p='md',
                                                radius='md',
                                                withBorder=True,
                                                style={'height': '100%'},
                                            ),
                                            span={"base": 12, "sm": 7},
                                        ),
                                        dmc.GridCol(
                                            dmc.Paper(
                                                dmc.Stack(
                                                    [
                                                        dmc.Text('Lowest Days of Cover', fw=600, mb='md'),
                                                        dmc.Box(
                                                            dcc.Graph(
                                                                id='inventory-stock-low',
                                                                figure={},
                                                                config={'displayModeBar': False},
                                                                style={'height': {'base': '250px', 'sm': '350px', 'lg': '400px'}},
                                                                responsive=True,
                                                            )
                                                        ),
                                                    ]
                                                ),
                                                p='md',
                                                radius='md',
                                                withBorder=True,
                                            ),
                                            span={"base": 12, "sm": 5},
                                        ),
                                    ],
                                    gutter={"base": "md", "lg": "lg"},
                                    mt='lg',
                                ),
                                dmc.Paper(
                                    dmc.Stack(
                                        [
                                            dmc.Text('Stock Levels Table', fw=600, mb='md'),
                                            dmc.Box(
                                                dmc.Table(
                                                    id='inventory-stock-table',
                                                    striped=True,
                                                    highlightOnHover=True,
                                                    withTableBorder=True,
                                                    horizontalSpacing='md',
                                                    verticalSpacing='xs',
                                                    fz='xs',
                                                    data={},
                                                ),
                                                h=420,
                                                style={'overflowY': 'auto'},
                                            ),
                                        ]
                                    ),
                                    p='md',
                                    radius='md',
                                    withBorder=True,
                                    mt='lg',
                                ),
                            ],
                            gap='md',
                        ),
                        value='stock-levels',
                    ),
                    dmc.TabsPanel(
                        dmc.Stack(
                            [
                                dmc.Paper(
                                    dmc.Stack(
                                        [
                                            dmc.Group(
                                                [
                                                    dmc.Stack(
                                                        [
                                                            dmc.Text('From:', fw=600),
                                                            dmc.DatePickerInput(
                                                                value=date.today(),
                                                                placeholder='Select date',
                                                                id='inventory-sell-date-from',
                                                            ),
                                                        ],
                                                        gap=4,
                                                    ),
                                                    dmc.Stack(
                                                        [
                                                            dmc.Text('Until:', fw=600),
                                                            dmc.DatePickerInput(
                                                                value=date.today(),
                                                                placeholder='Select date',
                                                                id='inventory-sell-date-until',
                                                            ),
                                                        ],
                                                        gap=4,
                                                    ),
                                                    dmc.Button('Apply', id='inventory-sell-apply', variant='filled', size='sm'),
                                                ],
                                                gap='xl',
                                                align='flex-end',
                                                wrap='wrap',
                                            ),
                                            dmc.Text('Snapshot date: —', size='xs', c='dimmed', id='inventory-sell-snapshot-label'),
                                        ],
                                        gap='xs',
                                    ),
                                    p='md',
                                    radius='md',
                                    withBorder=True,
                                    mt='md',
                                ),
                                dmc.Grid(
                                    [
                                        _simple_kpi_card('Sell-through %', 'inventory-sell-kpi-sellthrough', 3),
                                        _simple_kpi_card('Units Sold', 'inventory-sell-kpi-sold', 3),
                                        _simple_kpi_card('Units Received', 'inventory-sell-kpi-received', 3),
                                        _simple_kpi_card('Begin On-hand', 'inventory-sell-kpi-begin', 3),
                                    ],
                                    gutter={"base": "md", "lg": "lg"},
                                    mt='md',
                                ),
                                dmc.Grid(
                                    [
                                        dmc.GridCol(
                                            dmc.Paper(
                                                dmc.Stack(
                                                    [
                                                        dmc.Text('Sell-through by Category', fw=600, mb='md'),
                                                        dmc.Container(
                                                            dcc.Graph(
                                                                id='inventory-sell-category',
                                                                figure={},
                                                                config={'displayModeBar': False},
                                                                style={'height': '100%', 'width': '100%'},
                                                            ),
                                                            p=0,
                                                            fluid=True,
                                                            style={'height': '100%'},
                                                        ),
                                                    ]
                                                ),
                                                p='md',
                                                radius='md',
                                                withBorder=True,
                                                style={'height': '100%'},
                                            ),
                                            span={"base": 12, "sm": 7},
                                        ),
                                        dmc.GridCol(
                                            dmc.Paper(
                                                dmc.Stack(
                                                    [
                                                        dmc.Text('Top/Bottom Sell-through', fw=600, mb='md'),
                                                        dmc.Box(
                                                            dcc.Graph(
                                                                id='inventory-sell-top-bottom',
                                                                figure={},
                                                                config={'displayModeBar': False},
                                                                style={'height': {'base': '250px', 'sm': '350px', 'lg': '400px'}},
                                                                responsive=True,
                                                            )
                                                        ),
                                                    ]
                                                ),
                                                p='md',
                                                radius='md',
                                                withBorder=True,
                                            ),
                                            span={"base": 12, "sm": 5},
                                        ),
                                    ],
                                    gutter={"base": "md", "lg": "lg"},
                                    mt='lg',
                                ),
                                dmc.Paper(
                                    dmc.Stack(
                                        [
                                            dmc.Text('Sell-through Table', fw=600, mb='md'),
                                            dmc.Box(
                                                dmc.Table(
                                                    id='inventory-sell-table',
                                                    striped=True,
                                                    highlightOnHover=True,
                                                    withTableBorder=True,
                                                    horizontalSpacing='md',
                                                    verticalSpacing='xs',
                                                    fz='xs',
                                                    data={},
                                                ),
                                                h=420,
                                                style={'overflowY': 'auto'},
                                            ),
                                        ]
                                    ),
                                    p='md',
                                    radius='md',
                                    withBorder=True,
                                    mt='lg',
                                ),
                            ],
                            gap='md',
                        ),
                        value='sell-through',
                    ),
                    dmc.TabsPanel(
                        dmc.Stack(
                            [
                                dmc.Paper(
                                    dmc.Group(
                                        [
                                            dmc.Stack(
                                                [
                                                    dmc.Text('From:', fw=600),
                                                    dmc.DatePickerInput(
                                                        value=date.today(),
                                                        placeholder='Select date',
                                                        id='inventory-abc-date-from',
                                                    ),
                                                ],
                                                gap=4,
                                            ),
                                            dmc.Stack(
                                                [
                                                    dmc.Text('Until:', fw=600),
                                                    dmc.DatePickerInput(
                                                        value=date.today(),
                                                        placeholder='Select date',
                                                        id='inventory-abc-date-until',
                                                    ),
                                                ],
                                                gap=4,
                                            ),
                                            dmc.Button('Apply', id='inventory-abc-apply', variant='filled', size='sm'),
                                        ],
                                        gap='xl',
                                        align='flex-end',
                                        wrap='wrap',
                                    ),
                                    p='md',
                                    radius='md',
                                    withBorder=True,
                                    mt='md',
                                ),
                                dmc.Grid(
                                    [
                                        _abc_kpi_card('Class A SKUs', 'inventory-abc-kpi-a-count', 'inventory-abc-kpi-a-share', 'green'),
                                        _abc_kpi_card('Class B SKUs', 'inventory-abc-kpi-b-count', 'inventory-abc-kpi-b-share', 'orange'),
                                        _abc_kpi_card('Class C SKUs', 'inventory-abc-kpi-c-count', 'inventory-abc-kpi-c-share', 'red'),
                                    ],
                                    gutter={"base": "md", "lg": "lg"},
                                    mt='md',
                                ),
                                dmc.Grid(
                                    [
                                        dmc.GridCol(
                                            dmc.Paper(
                                                dmc.Stack(
                                                    [
                                                        dmc.Text('ABC Pareto Curve', fw=600, mb='md'),
                                                        dmc.Container(
                                                            dcc.Graph(
                                                                id='inventory-abc-pareto',
                                                                figure={},
                                                                config={'displayModeBar': False},
                                                                style={'height': '100%', 'width': '100%'},
                                                            ),
                                                            p=0,
                                                            fluid=True,
                                                            style={'height': '100%'},
                                                        ),
                                                    ]
                                                ),
                                                p='md',
                                                radius='md',
                                                withBorder=True,
                                                style={'height': '100%'},
                                            ),
                                            span={"base": 12, "sm": 7},
                                        ),
                                        dmc.GridCol(
                                            dmc.Paper(
                                                dmc.Stack(
                                                    [
                                                        dmc.Text('ABC Distribution by Category', fw=600, mb='md'),
                                                        dmc.Box(
                                                            dcc.Graph(
                                                                id='inventory-abc-category',
                                                                figure={},
                                                                config={'displayModeBar': False},
                                                                style={'height': {'base': '250px', 'sm': '350px', 'lg': '400px'}},
                                                                responsive=True,
                                                            )
                                                        ),
                                                    ]
                                                ),
                                                p='md',
                                                radius='md',
                                                withBorder=True,
                                            ),
                                            span={"base": 12, "sm": 5},
                                        ),
                                    ],
                                    gutter={"base": "md", "lg": "lg"},
                                    mt='lg',
                                ),
                                dmc.Paper(
                                    dmc.Stack(
                                        [
                                            dmc.Text('ABC Product Table', fw=600, mb='md'),
                                            dmc.Box(
                                                dmc.Table(
                                                    id='inventory-abc-table',
                                                    striped=True,
                                                    highlightOnHover=True,
                                                    withTableBorder=True,
                                                    horizontalSpacing='md',
                                                    verticalSpacing='xs',
                                                    fz='xs',
                                                    data={},
                                                ),
                                                h=420,
                                                style={'overflowY': 'auto'},
                                            ),
                                        ]
                                    ),
                                    p='md',
                                    radius='md',
                                    withBorder=True,
                                    mt='lg',
                                ),
                            ],
                            gap='md',
                        ),
                        value='abc-analysis',
                    ),
                ],
                value='abc-analysis',
                mt='md',
            ),
        ],
        size='100%',  # Design Policy: Full viewport width
        px='md',      # Design Policy: Horizontal padding
        py='lg',      # Design Policy: Vertical padding
    )


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


def _safe_label(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _format_snapshot_label(snapshot_date, prefix='Snapshot date'):
    if not snapshot_date:
        return f"{prefix}: —"
    return f"{prefix}: {snapshot_date.strftime('%d %b %Y')}"


def _format_stock_levels_as_of_label(stock_result) -> str:
    as_of_ts = stock_result.get('as_of_ts')
    location_id = stock_result.get('location_id')
    baseline_ts = stock_result.get('baseline_ts')
    if not as_of_ts:
        return "Snapshot: —"
    baseline_label = baseline_ts.strftime('%d %b %Y %H:%M') if baseline_ts else '—'
    loc_label = str(location_id) if location_id is not None else '—'
    return (
        f"Snapshot: {as_of_ts.strftime('%d %b %Y %H:%M')} (UTC+07) · "
        f"Loc {loc_label} · Baseline {baseline_label}"
    )


def _normalize_display_number(value: float, abs_tol: float = 1e-9) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    if abs(num) <= abs_tol:
        return 0.0
    return num


@dash.callback(
    Output('inventory-abc-pareto', 'figure'),
    Output('inventory-abc-category', 'figure'),
    Output('inventory-abc-table', 'data'),
    Output('inventory-abc-kpi-a-count', 'children'),
    Output('inventory-abc-kpi-b-count', 'children'),
    Output('inventory-abc-kpi-c-count', 'children'),
    Output('inventory-abc-kpi-a-share', 'children'),
    Output('inventory-abc-kpi-b-share', 'children'),
    Output('inventory-abc-kpi-c-share', 'children'),
    Input('inventory-abc-apply', 'n_clicks'),
    State('inventory-abc-date-from', 'value'),
    State('inventory-abc-date-until', 'value'),
    prevent_initial_call=False,
)
def update_abc_analysis(n_clicks, date_from, date_until):
    start_date = _parse_date(date_from) or date.today()
    end_date = _parse_date(date_until) or start_date

    abc_result = get_abc_analysis(start_date, end_date)
    items_df = abc_result['items']
    summary_df = abc_result['summary']
    categories_df = abc_result['categories']

    pareto_fig = build_abc_pareto_chart(items_df, start_date, end_date)
    category_fig = build_abc_category_distribution_chart(categories_df, start_date, end_date)

    summary_lookup = {row.get('abc_class'): row for row in summary_df.to_dict('records')}

    def _summary_value(label):
        row = summary_lookup.get(label, {}) if summary_lookup else {}
        count = int(row.get('sku_count', 0) or 0)
        share = float(row.get('revenue_share', 0) or 0)
        return count, share

    a_count, a_share = _summary_value('A')
    b_count, b_share = _summary_value('B')
    c_count, c_share = _summary_value('C')

    table_data = {
        'head': ['SKU', 'Category', 'Brand', 'Revenue', 'Units', 'Cumulative %', 'Class'],
        'body': []
    }

    if items_df.empty:
        table_data['body'].append(['No data available', '', '', '', '', '', ''])
    else:
        display_df = items_df.sort_values('revenue', ascending=False).head(50)
        for _, row in display_df.iterrows():
            revenue = float(row.get('revenue') or 0)
            quantity = float(row.get('quantity') or 0)
            cumulative = float(row.get('cumulative_share') or 0)
            table_data['body'].append([
                _safe_label(row.get('product_name'), f"Product {row.get('product_id', '')}"),
                _safe_label(row.get('product_category'), 'Unknown Category'),
                _safe_label(row.get('product_brand'), 'Unknown Brand'),
                f"Rp {revenue:,.0f}",
                f"{quantity:,.0f}",
                f"{cumulative:.1%}",
                _safe_label(row.get('abc_class'), 'C'),
            ])

    return (
        pareto_fig,
        category_fig,
        table_data,
        f"{a_count:,}",
        f"{b_count:,}",
        f"{c_count:,}",
        f"Revenue share: {a_share:.1%}",
        f"Revenue share: {b_share:.1%}",
        f"Revenue share: {c_share:.1%}",
    )


@dash.callback(
    Output('inventory-stock-cover', 'figure'),
    Output('inventory-stock-low', 'figure'),
    Output('inventory-stock-table', 'data'),
    Output('inventory-stock-kpi-onhand', 'children'),
    Output('inventory-stock-kpi-low', 'children'),
    Output('inventory-stock-kpi-dead', 'children'),
    Output('inventory-stock-snapshot-label', 'children'),
    Input('inventory-stock-apply', 'n_clicks'),
    State('inventory-stock-date', 'value'),
    prevent_initial_call=False,
)
def update_stock_levels(n_clicks, date_value):
    as_of_date = _parse_date(date_value) or date.today()
    if as_of_date < STOCK_LEDGER_BASELINE_DATE:
        as_of_date = STOCK_LEDGER_BASELINE_DATE
    stock_result = get_stock_levels_ledger(as_of_date)

    items_df = stock_result['items']
    summary = stock_result['summary']
    snapshot_date = stock_result.get('snapshot_date')
    display_date = snapshot_date or as_of_date

    cover_fig = build_stock_cover_distribution_chart(
        items_df,
        display_date,
        summary.get('lookback_days', DEFAULT_STOCK_LOOKBACK_DAYS),
        summary.get('low_stock_days', DEFAULT_LOW_STOCK_DAYS),
    )
    low_fig = build_low_stock_chart(
        items_df,
        display_date,
        summary.get('low_stock_days', DEFAULT_LOW_STOCK_DAYS),
    )

    table_data = {
        'head': ['SKU', 'Category', 'Brand', 'On-hand', 'Reserved', 'Avg Daily Sold', 'Days of Cover', 'Flags'],
        'body': []
    }

    if items_df.empty:
        table_data['body'].append(['No data available', '', '', '', '', '', '', ''])
    else:
        display_df = items_df.sort_values('on_hand_qty', ascending=False).head(50)
        for _, row in display_df.iterrows():
            on_hand = _normalize_display_number(row.get('on_hand_qty') or 0)
            reserved = _normalize_display_number(row.get('reserved_qty') or 0)
            avg_daily = _normalize_display_number(row.get('avg_daily_sold') or 0)
            days_cover = row.get('days_of_cover')
            days_label = '—' if pd.isna(days_cover) else f"{float(days_cover):.1f}"

            flags = []
            if bool(row.get('low_stock_flag')):
                flags.append('Low')
            if bool(row.get('dead_stock_flag')):
                flags.append('Dead')
            flags_label = ', '.join(flags) if flags else '—'

            table_data['body'].append([
                _safe_label(row.get('product_name'), f"Product {row.get('product_id', '')}"),
                _safe_label(row.get('product_category'), 'Unknown Category'),
                _safe_label(row.get('product_brand'), 'Unknown Brand'),
                f"{on_hand:,.0f}",
                f"{reserved:,.0f}",
                f"{avg_daily:,.2f}",
                days_label,
                flags_label,
            ])

    snapshot_label = _format_stock_levels_as_of_label(stock_result)

    return (
        cover_fig,
        low_fig,
        table_data,
        f"{_normalize_display_number(summary.get('total_on_hand', 0)):,.0f}",
        f"{summary.get('low_stock_count', 0):,}",
        f"{summary.get('dead_stock_count', 0):,}",
        snapshot_label,
    )


@dash.callback(
    Output('inventory-sell-category', 'figure'),
    Output('inventory-sell-top-bottom', 'figure'),
    Output('inventory-sell-table', 'data'),
    Output('inventory-sell-kpi-sellthrough', 'children'),
    Output('inventory-sell-kpi-sold', 'children'),
    Output('inventory-sell-kpi-received', 'children'),
    Output('inventory-sell-kpi-begin', 'children'),
    Output('inventory-sell-snapshot-label', 'children'),
    Input('inventory-sell-apply', 'n_clicks'),
    State('inventory-sell-date-from', 'value'),
    State('inventory-sell-date-until', 'value'),
    prevent_initial_call=False,
)
def update_sell_through(n_clicks, date_from, date_until):
    start_date = _parse_date(date_from) or date.today()
    end_date = _parse_date(date_until) or start_date

    sell_result = get_sell_through_analysis(start_date, end_date)
    items_df = sell_result['items']
    categories_df = sell_result['categories']
    summary = sell_result['summary']
    snapshot_date = sell_result.get('snapshot_date')

    category_fig = build_sell_through_by_category_chart(categories_df, start_date, end_date)
    top_bottom_fig = build_sell_through_top_bottom_chart(items_df, start_date, end_date)

    table_data = {
        'head': ['SKU', 'Category', 'Brand', 'Begin On-hand', 'Units Received', 'Units Sold', 'Sell-through'],
        'body': []
    }

    if items_df.empty:
        table_data['body'].append(['No data available', '', '', '', '', '', ''])
    else:
        display_df = items_df.sort_values('units_sold', ascending=False).head(50)
        for _, row in display_df.iterrows():
            begin_on_hand = float(row.get('begin_on_hand') or 0)
            units_received = float(row.get('units_received') or 0)
            units_sold = float(row.get('units_sold') or 0)
            sell_through = float(row.get('sell_through') or 0)

            table_data['body'].append([
                _safe_label(row.get('product_name'), f"Product {row.get('product_id', '')}"),
                _safe_label(row.get('product_category'), 'Unknown Category'),
                _safe_label(row.get('product_brand'), 'Unknown Brand'),
                f"{begin_on_hand:,.0f}",
                f"{units_received:,.0f}",
                f"{units_sold:,.0f}",
                f"{sell_through:.1%}",
            ])

    snapshot_label = _format_snapshot_label(snapshot_date)

    return (
        category_fig,
        top_bottom_fig,
        table_data,
        f"{summary.get('sell_through', 0):.1%}",
        f"{summary.get('units_sold', 0):,.0f}",
        f"{summary.get('units_received', 0):,.0f}",
        f"{summary.get('begin_on_hand', 0):,.0f}",
        snapshot_label,
    )
