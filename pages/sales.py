import dash
from dash import dcc, html, Output, Input, State
import dash_mantine_components as dmc
from datetime import date

from services.sales_charts import (
    build_daily_revenue_chart,
    build_revenue_trend_chart,
    build_category_sankey_chart,
    build_hourly_heatmap_chart,
    build_sales_by_principal_chart,
)
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
                        span={"base": 6, "sm": 3},  
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
                        span={"base": 6, "sm": 3},
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Avg Basket Size', size='sm', c='dimmed'),
                                dmc.Text('Rp 0', size='xl', fw=600, id='sales-kpi-avg-transaction-value'),
                                dmc.Text('vs prev period: Rp 0 (0%)', size='xs', c='red', id='sales-kpi-avg-transaction-value-change')
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span={"base": 6, "sm": 3},
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
                        span={"base": 6, "sm": 3},
                    ),
                ],
                gutter={"base": "md", "lg": "lg"},  
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
                                dmc.Container(
                                    dcc.Graph(
                                        id='sales-revenue-trend',
                                        figure={},  # Placeholder
                                        config={'displayModeBar': False},
                                        style={'height': '100%', 'width': '100%'},
                                    ),
                                    p=0,
                                    fluid=True,
                                    style={'height': '100%'}
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                            style={'height': '100%'}
                        ),
                        span={"base": 12, "sm": 8},
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Sales by Principal', fw=600, mb='md'),
                                dmc.Box(
                                    dcc.Graph(
                                        id='sales-by-principal',
                                        figure={},  # Placeholder
                                        config={'displayModeBar': False},
                                        style={'height': {'base': '250px', 'sm': '350px', 'lg': '400px'}},
                                        responsive=True,
                                    )
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span={"base": 12, "sm": 4},  
                    ),
                ],
                gutter={"base": "md", "lg": "lg"},  
                mt='lg',
            ),
            
            # Tables Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Top Products', fw=600, mb='md'),
                                dmc.Box(
                                    dmc.Table(
                                        id='sales-top-products-table',
                                        striped=True,
                                        highlightOnHover=True,
                                        withTableBorder=True,
                                        horizontalSpacing="md",
                                        verticalSpacing="xs",
                                        fz='xs',
                                        # Table data will be populated via callback
                                        data={},  # Placeholder
                                    ),
                                    h=400,  # Fixed height
                                    style={"overflowY": "auto"}  # Vertical scrolling
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span={"base": 12, "sm": 6},  
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Hourly Sales Pattern', fw=600, mb='md'),
                                dmc.Box(
                                    dcc.Graph(
                                        id='sales-hourly-pattern',
                                        figure={},  # Placeholder
                                        config={'displayModeBar': False},
                                        style={'height': {'base': '250px', 'sm': '350px', 'lg': '400px'}},
                                        responsive=True,
                                    )
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span={"base": 12, "sm": 6},  
                    ),
                ],
                gutter={"base": "md", "lg": "lg"},  
                mt='lg',
            ),
            
            # Sales Flow Hierarchy Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Text('Sales Flow Hierarchy', fw=600, mb='md'),
                                dmc.Box(
                                    dcc.Graph(
                                        id='sales-category-breakdown',
                                        figure={},  # Placeholder
                                        config={'displayModeBar': False},
                                        style={'height': {'base': '400px', 'sm': '500px', 'lg': '600px'}},
                                        responsive=True,
                                    )
                                )
                            ]),
                            p='md',
                            radius='md',
                            withBorder=True,
                        ),
                        span=12,
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
    Input('sales-btn-apply', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=False,
)
def update_revenue_chart(n_clicks, date_from, date_until):
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
    
    # Build and return the daily revenue chart
    return build_daily_revenue_chart(start_date, end_date)


@dash.callback(
    Output('sales-category-breakdown', 'figure'),
    Output('sales-hourly-pattern', 'figure'),
    Input('sales-btn-apply', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=True,
)
def update_additional_charts(n_clicks, date_from, date_until):
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
    
    # Build Sankey chart
    sankey_fig = build_category_sankey_chart(start_date, end_date)
    
    # Build hourly heatmap
    hourly_fig = build_hourly_heatmap_chart(start_date, end_date)
    
    return sankey_fig, hourly_fig


@dash.callback(
    Output('sales-by-principal', 'figure'),
    Input('sales-btn-apply', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=True,
)
def update_sales_by_principal_chart(n_clicks, date_from, date_until):
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

    return build_sales_by_principal_chart(start_date, end_date)


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
        items_sold_text = f'{current["items_sold"]:,.0f}'
        
        # Format change texts with color indicators
        revenue_change_color = 'green' if deltas['revenue_pct'] >= 0 else 'red'
        transactions_change_color = 'green' if deltas['transactions_pct'] >= 0 else 'red'
        atv_change_color = 'green' if deltas['avg_transaction_value_pct'] >= 0 else 'red'
        items_change_color = 'green' if deltas['items_sold_pct'] >= 0 else 'red'
        
        revenue_change_text = f'vs prev period: Rp {deltas["revenue"]:,.0f} ({deltas["revenue_pct"]:+.1f}%)'
        transactions_change_text = f'vs prev period: {deltas["transactions"]:,.0f} ({deltas["transactions_pct"]:+.1f}%)'
        atv_change_text = f'vs prev period: Rp {deltas["avg_transaction_value"]:,.0f} ({deltas["avg_transaction_value_pct"]:+.1f}%)'
        items_change_text = f'vs prev period: {deltas["items_sold"]:+,.0f} ({deltas["items_sold_pct"]:+.1f}%)'
        
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


@dash.callback(
    Output('sales-top-products-table', 'data'),
    Input('sales-btn-apply', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=True,
)
def update_top_products_table(n_clicks, date_from, date_until):
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
    
    try:
        # Get top products data
        top_products_df = get_top_products(start_date, end_date, limit=20)
        
        if top_products_df.empty:
            return {
                'head': ['Name', 'Category', 'Quantity Sold', 'Total Unit Price'],
                'body': [['No data available', '', '', '']]
            }
        
        # Format the data for dmc.Table
        table_data = {
            'head': ['Name', 'Category', 'Quantity Sold', 'Total Unit Price'],
            'body': []
        }
        
        for _, row in top_products_df.iterrows():
            table_data['body'].append([
                str(row['product_name']),
                str(row['category']),
                f"{int(row['quantity_sold']):,}",
                f"Rp {row['total_unit_price']:,.0f}"
            ])
        
        return table_data
        
    except Exception as e:
        print(f"Error updating top products table: {e}")
        return {
            'head': ['Name', 'Category', 'Quantity Sold', 'Total Unit Price'],
            'body': [['Error loading data', '', '', '']]
        }