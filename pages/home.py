import dash
from dash import dcc, Output, Input, State
import dash_mantine_components as dmc
import plotly.express as px
import pandas as pd
from datetime import date

from services.overview_metrics import get_total_overview_summary
from services.profit_metrics import query_profit_summary, query_profit_revenue_by_category


def _build_total_overview_figure(date_start, date_end=None):
    summary = get_total_overview_summary(date_start, date_end)
    date_start = summary['target_date_start']
    date_end = summary['target_date_end']

    profit_summary = query_profit_summary(date_start, date_end)
    today_amount = profit_summary.get('revenue', 0.0) or 0.0
    today_qty = profit_summary.get('quantity', 0.0) or 0.0

    categories_nested = query_profit_revenue_by_category(date_start, date_end)
    days = (date_end - date_start).days + 1
    prev_end = date_start.fromordinal(date_start.toordinal() - 1)
    prev_start = date_start.fromordinal(date_start.toordinal() - days)
    prev_profit_summary = query_profit_summary(prev_start, prev_end)
    prev_amount = prev_profit_summary.get('revenue', 0.0) or 0.0

    if categories_nested:
        records = [
            {
                'parent_category': parent,
                'category': child,
                'amount': amt,
            }
            for parent, child_map in categories_nested.items()
            for child, amt in child_map.items()
        ]
        df = pd.DataFrame(records)
        fig = px.sunburst(
            df,
            path=['parent_category', 'category'],
            values='amount',
            color='parent_category',
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
    else:
        df = pd.DataFrame()
        fig = px.sunburst()

    if df.empty:
        fig.update_layout(
            annotations=[dict(text='No revenue data available for the selected date.', x=0.5, y=0.5, showarrow=False, font=dict(size=14, color='gray'))]
        )

    delta_amount = today_amount - prev_amount
    delta_pct = (delta_amount / prev_amount * 100) if prev_amount else None

    if delta_pct is None:
        delta_text = f" vs prev period: Rp {delta_amount:,.0f}"
    else:
        delta_text = f" vs prev period: Rp {delta_amount:,.0f} ({delta_pct:+.1f}%)"

    # Title shows single date or range
    if date_start == date_end:
        title_str = f"Total Overview – {date_start.strftime('%d %b %Y')}"
    else:
        title_str = f"Total Overview – {date_start.strftime('%d %b %Y')} to {date_end.strftime('%d %b %Y')}"

    fig.update_layout(
        title=title_str,
        legend_title_text='Product Category',
        template='plotly_white',
        height=420,
        margin=dict(t=90, b=60, l=40, r=40),

        annotations=[
            dict(
                text=f"Revenue: Rp {today_amount:,.0f}",
                x=0,
                y=1.15,
                xref='paper',
                yref='paper',
                showarrow=False,
                font=dict(size=16, color='#1864ab'),
            ),
            dict(
                text=delta_text,
                x=0,
                y=1.07,
                xref='paper',
                yref='paper',
                showarrow=False,
                font=dict(size=12, color='#495057'),
            ),
            dict(
                text=f"Qty sold: {today_qty:,.0f}",
                x=0,
                y=1.0,
                xref='paper',
                yref='paper',
                showarrow=False,
                font=dict(size=12, color='#495057'),
            )
        ],
    )
    return fig


dash.register_page(__name__, path='/', name='Overview', title='Executive Overview')

layout = dmc.Container(
    [
        dmc.Title('Executive Dashboard', order=2, mb='xs'),
        dmc.Text('High-level overview of business performance and key metrics.', c='dimmed', mb='lg'),
        
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
                                        dmc.Text('Date Controls', fw=600, size='lg'),
                                        dmc.Badge('Time Period', color='gray', variant='light'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Divider(),
                                dmc.Group(
                                    [
                                        dmc.Button('Weekly', variant='light', size='xs', id='btn-weekly'),
                                        dmc.Button('Monthly', variant='light', size='xs', id='btn-monthly'),
                                        dmc.Button('Quarterly', variant='light', size='xs', id='btn-quarterly'),
                                        dmc.Button('Semesterly', variant='light', size='xs', id='btn-semesterly'),
                                        dmc.Button('Yearly', variant='light', size='xs', id='btn-yearly'),
                                    ],
                                    gap='xs',
                                ),
                                dmc.Group(
                                    [
                                        dmc.Stack(
                                            [
                                                dmc.Text('From:', fw=600),
                                                dmc.Group(
                                                    [
                                                        dmc.DatePickerInput(value=date.today(), placeholder='Select date', id='date-from'),
                                                        dmc.TimeInput(value='07:00', id='time-from'),
                                                    ],
                                                    gap='sm',
                                                ),
                                            ],
                                            gap=4,
                                        ),
                                        dmc.Stack(
                                            [
                                                dmc.Text('Until:', fw=600),
                                                dmc.Group(
                                                    [
                                                        dmc.DatePickerInput(value=date.today(), placeholder='Select date', id='date-until'),
                                                        dmc.TimeInput(value='23:30', id='time-until'),
                                                    ],
                                                    gap='sm',
                                                ),
                                            ],
                                            gap=4,
                                        ),
                                        dmc.Button('Apply', id='btn-apply-dates', variant='filled', size='sm'),
                                    ],
                                    gap='xl',
                                    wrap='wrap',
                                    align='flex-end',
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
                
                # Main Chart Card - Top Row
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Revenue Overview', fw=600, size='lg'),
                                        dmc.Badge('Live Data', color='gray', variant='light'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dcc.Graph(
                                    id='total-overview-fig',
                                    config={'displayModeBar': False},
                                ),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                    ),
                    span=8,
                ),
                
                # KPI Cards - Top Row
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Financial Health', fw=600, size='lg'),
                                        dmc.Badge('KPI', color='gray', variant='light'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Text('Revenue: Rp 0', id='kpi-revenue', size='xl', fw=600),
                                dmc.Text('vs prev period: Rp 0 (0.0%)', id='kpi-revenue-delta', size='sm', c='dimmed'),
                                dmc.Text('Gross profit: Rp 0', id='kpi-gross-profit', size='sm', c='dimmed'),
                                dmc.Text('Gross margin: 0.0%', id='kpi-gross-margin', size='sm', c='dimmed'),
                                dmc.Text('Avg txn value: Rp 0', id='kpi-atv', size='sm', c='dimmed'),
                                dmc.Text('Qty sold: 0', id='kpi-qty-sold', size='sm', c='dimmed'),
                                dmc.Text('Transactions: 0', id='kpi-transactions', size='sm', c='dimmed'),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                    ),
                    span=4,
                ),
            ],
            gutter='lg',
        ),
    ],
    size='100%',  # Design Policy: Full viewport width
    px='md',      # Design Policy: Horizontal padding
    py='lg',      # Design Policy: Vertical padding
)


@dash.callback(
    Output('total-overview-fig', 'figure'),
    Output('kpi-revenue', 'children'),
    Output('kpi-revenue-delta', 'children'),
    Output('kpi-gross-profit', 'children'),
    Output('kpi-gross-margin', 'children'),
    Output('kpi-atv', 'children'),
    Output('kpi-qty-sold', 'children'),
    Output('kpi-transactions', 'children'),
    Input('btn-apply-dates', 'n_clicks'),
    State('date-from', 'value'),
    State('date-until', 'value'),
    State('time-from', 'value'),
    State('time-until', 'value'),
    prevent_initial_call=False,
)
def update_total_overview(n_clicks, date_from_input, date_until, time_from, time_until):
    # Determine which input triggered the callback
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else 'initial_load'

    if not n_clicks:
        fig = px.sunburst()
        fig.update_layout(
            template='plotly_white',
            height=420,
            annotations=[
                dict(
                    text='Click Apply to load data.',
                    x=0.5,
                    y=0.5,
                    xref='paper',
                    yref='paper',
                    showarrow=False,
                    font=dict(size=14, color='gray'),
                )
            ],
        )
        return (
            fig,
            'Revenue: Rp 0',
            'vs prev period: Rp 0 (0.0%)',
            'Gross profit: Rp 0',
            'Gross margin: 0.0%',
            'Avg txn value: Rp 0',
            'Qty sold: 0',
            'Transactions: 0',
        )

    # Use date_from_input which is the *latest value* of the 'date-from' component
    # regardless of whether the button or the datepicker triggered the update.

    selected_date_str = date_from_input
    selected_date = date.today() # Default

    if selected_date_str:
        try:
            # DatePickerInput returns ISO format string (YYYY-MM-DD)
            selected_date = date.fromisoformat(selected_date_str)
        except (ValueError, TypeError):
            pass # Keep default date.today()

    # Parse start and end dates
    start_date = selected_date
    end_date = start_date
    if date_until:
        try:
            end_date = date.fromisoformat(date_until)
        except (ValueError, TypeError):
            pass

    fig = _build_total_overview_figure(start_date, end_date)

    try:
        profit_summary = query_profit_summary(start_date, end_date)
        revenue = profit_summary.get('revenue', 0.0) or 0.0
        gross_profit = profit_summary.get('gross_profit', 0.0) or 0.0
        gross_margin_pct = profit_summary.get('gross_margin_pct', 0.0) or 0.0
        atv = profit_summary.get('avg_transaction_value', 0.0) or 0.0
        qty = profit_summary.get('quantity', 0.0) or 0.0
        transactions = profit_summary.get('transactions', 0) or 0

        days = (end_date - start_date).days + 1
        prev_end = start_date.fromordinal(start_date.toordinal() - 1)
        prev_start = start_date.fromordinal(start_date.toordinal() - days)

        prev_profit_summary = query_profit_summary(prev_start, prev_end)
        prev_revenue = prev_profit_summary.get('revenue', 0.0) or 0.0

        delta_amount = revenue - prev_revenue
        delta_pct = (delta_amount / prev_revenue * 100) if prev_revenue else None

        if delta_pct is None:
            delta_text = f"vs prev period: Rp {delta_amount:,.0f}"
        else:
            delta_text = f"vs prev period: Rp {delta_amount:,.0f} ({delta_pct:+.1f}%)"

        return (
            fig,
            f"Revenue: Rp {revenue:,.0f}",
            delta_text,
            f"Gross profit: Rp {gross_profit:,.0f}",
            f"Gross margin: {gross_margin_pct:.1f}%",
            f"Avg txn value: Rp {atv:,.0f}",
            f"Qty sold: {qty:,.0f}",
            f"Transactions: {transactions:,}",
        )
    except Exception:
        return (
            fig,
            'Revenue: Rp 0',
            'vs prev period: Rp 0 (0.0%)',
            'Gross profit: Rp 0',
            'Gross margin: 0.0%',
            'Avg txn value: Rp 0',
            'Qty sold: 0',
            'Transactions: 0',
        )