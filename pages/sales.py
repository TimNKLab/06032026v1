import dash
from dash import dcc, html, Output, Input, State
import dash_mantine_components as dmc
from datetime import date

from services.sales_charts import build_revenue_trend_chart, build_category_breakdown_chart
from services.sales_metrics import get_revenue_comparison, get_top_products, get_hourly_sales_pattern

dash.register_page(
    __name__,
    path='/sales',
    name='Sales',
    title='Sales Performance'
)

def layout():
    return dmc.Container(
        [
            dmc.Title('Sales Performance', order=2),
            dmc.Text('Comprehensive sales metrics and performance insights.', c='dimmed'),
            
            # KPI Cards Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Total Revenue', size='sm', c='dimmed'),
                                dmc.Text('Rp 0', size='xl', fw=600, id='sales-kpi-total-revenue'),
                                dmc.Text('vs prev period: Rp 0 (0%)', size='xs', c='green', id='sales-kpi-total-revenue-change')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=3,
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Transactions', size='sm', c='dimmed'),
                                dmc.Text('0', size='xl', fw=600, id='sales-kpi-transactions'),
                                dmc.Text('vs prev period: 0 (0%)', size='xs', c='green', id='sales-kpi-transactions-change')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=3,
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Avg Transaction Value', size='sm', c='dimmed'),
                                dmc.Text('Rp 0', size='xl', fw=600, id='sales-kpi-avg-transaction-value'),
                                dmc.Text('vs prev period: Rp 0 (0%)', size='xs', c='red', id='sales-kpi-avg-transaction-value-change')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=3,
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Items Sold', size='sm', c='dimmed'),
                                dmc.Text('0', size='xl', fw=600, id='sales-kpi-items-sold'),
                                dmc.Text('vs prev period: 0 (0%)', size='xs', c='green', id='sales-kpi-items-sold-change')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=3,
                    ),
                ],
                gutter='lg',
                mt='md',
            ),
            
            # Date Filters
            dmc.Paper(
                dmc.Group(
                    [
                        dmc.Stack(
                            [
                                dmc.Text('From:', fw=600),
                                dmc.DatePickerInput(value=date.today(), placeholder='Select date', id='sales-date-from'),
                            ],
                            gap=4,
                        ),
                        dmc.Stack(
                            [
                                dmc.Text('Until:', fw=600),
                                dmc.DatePickerInput(value=date.today(), placeholder='Select date', id='sales-date-until'),
                            ],
                            gap=4,
                        ),
                        dmc.Button('Apply', id='sales-btn-apply', variant='filled', size='sm'),
                    ],
                    gap='xl',
                    align='flex-end',
                ),
                p='md',
                radius='md',
                withBorder=True,
                mt='md',
            ),
            
            # Charts Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Revenue Trend', fw=600, mb='md'),
                                dcc.Graph(
                                    id='sales-revenue-trend',
                                    figure={},  # Placeholder
                                    config={'displayModeBar': False},
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=8,
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Sales by Category', fw=600, mb='md'),
                                dcc.Graph(
                                    id='sales-category-breakdown',
                                    figure={},  # Placeholder
                                    config={'displayModeBar': False},
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=4,
                    ),
                ],
                gutter='lg',
                mt='lg',
            ),
            
            # Tables Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Top Products', fw=600, mb='md'),
                                dmc.Text('Top products table placeholder', c='dimmed')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=6,
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Hourly Sales Pattern', fw=600, mb='md'),
                                dmc.Text('Hourly heatmap placeholder', c='dimmed')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=6,
                    ),
                ],
                gutter='lg',
                mt='lg',
            ),
        ],
        size='lg',
        py='lg'
    )


# Callbacks for sales dashboard
@dash.callback(
    Output('sales-revenue-trend', 'figure'),
    Output('sales-category-breakdown', 'figure'),
    Input('sales-btn-apply', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=False,
)
def update_sales_charts(n_clicks, date_from, date_until):
    # Parse dates
    start_date = date.today()
    end_date = start_date
    
    if date_from:
        try:
            start_date = date.fromisoformat(date_from)
        except (ValueError, TypeError):
            pass
    
    if date_until:
        try:
            end_date = date.fromisoformat(date_until)
        except (ValueError, TypeError):
            pass
    
    # Build charts
    revenue_fig = build_revenue_trend_chart(start_date, end_date, 'daily')
    category_fig = build_category_breakdown_chart(start_date, end_date)
    
    return revenue_fig, category_fig


@dash.callback(
    Output('sales-kpi-total-revenue', 'children'),
    Output('sales-kpi-transactions', 'children'),
    Output('sales-kpi-avg-transaction-value', 'children'),
    Output('sales-kpi-items-sold', 'children'),
    Output('sales-kpi-total-revenue-change', 'children'),
    Output('sales-kpi-transactions-change', 'children'),
    Output('sales-kpi-avg-transaction-value-change', 'children'),
    Output('sales-kpi-items-sold-change', 'children'),
    Input('sales-btn-apply', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=True,
)
def update_kpi_cards(n_clicks, date_from, date_until):
    # Parse dates
    start_date = date.today()
    end_date = start_date
    
    if date_from:
        try:
            start_date = date.fromisoformat(date_from)
        except (ValueError, TypeError):
            pass
    
    if date_until:
        try:
            end_date = date.fromisoformat(date_until)
        except (ValueError, TypeError):
            pass
    
    # Get KPI data
    try:
        revenue_comparison = get_revenue_comparison(start_date, end_date)
        current = revenue_comparison['current']
        deltas = revenue_comparison['deltas']
        
        # Format KPI values
        revenue_text = f'Rp {current["revenue"]:,.0f}'
        transactions_text = f'{current["transactions"]:,.0f}'
        atv_text = f'Rp {current["avg_transaction_value"]:,.0f}'
        items_sold_text = f'{current["transactions"]:,.0f}'  # Using transactions as proxy for items
        
        # Format change texts with color indicators
        revenue_change_color = 'green' if deltas['revenue_pct'] >= 0 else 'red'
        transactions_change_color = 'green' if deltas['transactions_pct'] >= 0 else 'red'
        atv_change_color = 'green' if deltas['avg_transaction_value_pct'] >= 0 else 'red'
        items_change_color = 'green' if deltas['transactions_pct'] >= 0 else 'red'
        
        revenue_change_text = f'vs prev period: Rp {deltas["revenue"]:,.0f} ({deltas["revenue_pct"]:+.1f}%)'
        transactions_change_text = f'vs prev period: {deltas["transactions"]:,.0f} ({deltas["transactions_pct"]:+.1f}%)'
        atv_change_text = f'vs prev period: Rp {deltas["avg_transaction_value"]:,.0f} ({deltas["avg_transaction_value_pct"]:+.1f}%)'
        items_change_text = f'vs prev period: {deltas["transactions"]:,.0f} ({deltas["transactions_pct"]:+.1f}%)'
        
    except Exception as e:
        # Fallback values if data fetch fails
        revenue_text = 'Rp 0'
        transactions_text = '0'
        atv_text = 'Rp 0'
        items_sold_text = '0'
        revenue_change_text = 'vs prev period: Rp 0 (0%)'
        transactions_change_text = 'vs prev period: 0 (0%)'
        atv_change_text = 'vs prev period: Rp 0 (0%)'
        items_change_text = 'vs prev period: 0 (0%)'
    
    return (
        revenue_text,
        transactions_text,
        atv_text,
        items_sold_text,
        revenue_change_text,
        transactions_change_text,
        atv_change_text,
        items_change_text
    )