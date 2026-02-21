import time
import dash
from dash import dcc, html, Output, Input, State
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
from datetime import date, timedelta

# Calculate MTD (Month-to-Date) as sensible default
def get_mtd_dates():
    """Get Month-to-Date date range (first day of current month to today)."""
    today = date.today()
    first_day = date(today.year, today.month, 1)
    return first_day, today

# Performance investigation: temporarily using fixed date range
_INVESTIGATION_START_DATE, _INVESTIGATION_END_DATE = get_mtd_dates()

from services.sales_charts import (
    build_daily_revenue_chart,
    build_revenue_trend_chart,
    build_category_sankey_chart,
    build_hourly_heatmap_chart,
    build_sales_by_principal_chart,
)
from services.sales_metrics import get_revenue_comparison, get_top_products, get_hourly_sales_pattern
from services.profit_metrics import query_profit_summary

dash.register_page(
    __name__,
    path='/sales',
    name='Sales',
    title='Sales Performance'
)

def layout():
    # Get MTD dates for display
    mtd_start, mtd_end = get_mtd_dates()
    
    return dmc.Container(
        [
            dcc.Store(id='sales-query-context', data=None),
            
            # Page Header
            dmc.Paper(
                dmc.Stack([
                    dmc.Group([
                        dmc.Text('Sales Performance', size='xl', fw=700, c='blue'),
                        dmc.Text('Last updated: Just now', size='xs', c='dimmed', id='sales-last-updated'),
                    ], justify='space-between', align='center'),
                    dmc.Text(
                        f'Overview for {mtd_start.strftime("%d %b %Y")} - {mtd_end.strftime("%d %b %Y")} (Month-to-Date)',
                        size='sm', c='dimmed'
                    ),
                ]),
                p='lg',
                radius='lg',
                withBorder=True,
                shadow='sm',
                bg='blue.0',
                mb='lg',
            ),
            
            # Date Filters - Moved to top
            dmc.Paper(
                dmc.Stack([
                    dmc.Text('Date Range', fw=600, size='sm', c='dimmed'),
                    dmc.Group([
                        # Preset buttons
                        dmc.Button('MTD', id='sales-btn-mtd', variant='light', size='sm'),
                        dmc.Button('QTD', id='sales-btn-qtd', variant='light', size='sm'),
                        dmc.Button('YTD', id='sales-btn-ytd', variant='light', size='sm'),
                        dmc.Button('Last Month', id='sales-btn-last-month', variant='light', size='sm'),
                        
                        # Custom date range
                        dmc.Box(
                            dmc.Group([
                                dmc.DatePickerInput(value=mtd_start, placeholder='From', id='sales-date-from', size='sm'),
                                dmc.Text('→', size='sm', c='dimmed'),
                                dmc.DatePickerInput(value=mtd_end, placeholder='To', id='sales-date-until', size='sm'),
                                dmc.Button('Apply', id='sales-btn-apply', variant='filled', size='sm'),
                            ]),
                            ml='auto'
                        ),
                    ], gap='sm'),
                ]),
                p='lg',
                radius='lg',
                withBorder=True,
                shadow='sm',
                bg='gray.0',
                mb='lg',
            ),
            
            # KPI Cards Row - Top Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Total Revenue', size='sm', c='dimmed'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Text('Rp 0', size='xxl', fw=700, id='sales-kpi-total-revenue', c='blue.7'),
                                dmc.Text(id='sales-kpi-total-revenue-change')
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='sm',
                            bg='blue.0',
                        ),
                        span={"base": 12, "sm": 4},  
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Transactions', size='sm', c='dimmed'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Text('0', size='xl', fw=600, id='sales-kpi-transactions'),
                                dmc.Text(id='sales-kpi-transactions-change')
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='sm',
                        ),
                        span={"base": 12, "sm": 2},
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Avg Basket Size', size='sm', c='dimmed'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Text('Rp 0', size='xl', fw=600, id='sales-kpi-avg-transaction-value'),
                                dmc.Text(id='sales-kpi-avg-transaction-value-change')
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='sm',
                        ),
                        span={"base": 12, "sm": 3},
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Gross Margin', size='sm', c='dimmed'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Text('0%', size='xl', fw=600, id='sales-kpi-gross-margin'),
                                dmc.Text(id='sales-kpi-gross-margin-change')
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='sm',
                        ),
                        span={"base": 12, "sm": 3},
                    ),
                ],
                gutter={"base": "md", "lg": "lg"},  
            ),
            
            # Main Charts Row - Top Row
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Revenue Trend', fw=600, size='lg'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Box(
                                    dcc.Loading(
                                        dcc.Graph(
                                            id='sales-revenue-trend',
                                            figure={},  # Placeholder
                                            config={'displayModeBar': False},
                                            style={'height': '380px', 'width': '100%'},
                                        ),
                                        type='dot',
                                        color='#228be6'
                                    ),
                                    w='100%'
                                )
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='xl',
                            bg='white',
                            style={'height': '100%'}
                        ),
                        span={"base": 12, "md": 7},
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Sales by Principal', fw=600, size='lg'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Box(
                                    dcc.Loading(
                                        dcc.Graph(
                                            id='sales-by-principal',
                                            figure={},  # Placeholder
                                            config={'displayModeBar': False},
                                            style={'height': '380px'},
                                            responsive=True,
                                        ),
                                        type='dot',
                                        color='#228be6'
                                    )
                                )
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='xl',
                            bg='white',
                        ),
                        span={"base": 12, "md": 5},  
                    ),
                ],
                gutter={"base": "md", "lg": "lg"},  
                mt='lg',
            ),
            
            # Bottom Row Cards
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack([
                                dmc.Group(
                                    [
                                        dmc.Text('Top Products', fw=600, size='lg'),
                                        dmc.Badge('Best Sellers', color='gray', variant='light'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Box(
                                    dcc.Loading(
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
                                        type='dot',
                                        color='#228be6'
                                    ),
                                    h=400,  # Fixed height
                                    style={"overflowY": "auto"}  # Vertical scrolling
                                )
                            ]),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='xl',
                            bg='white',
                        ),
                        span={"base": 12, "sm": 12},  
                    ),
                    # Only Top Products remains; Hourly and Sankey moved to drilldown
                ],
                gutter={"base": "md", "lg": "lg"},  
                mt='lg',
            ),
            
            # Sales Flow Hierarchy removed; moved to drilldown
        ],
        size="xl",  # Design Policy: Max width for readability
        px='md',      # Design Policy: Horizontal padding
        py='lg',      # Design Policy: Vertical padding
    )


# Callbacks for sales dashboard
def _log_timing(name, start_time):
    elapsed = time.time() - start_time
    print(f"[TIMING] {name}: {elapsed:.3f}s")

@dash.callback(
    Output('sales-revenue-trend', 'figure'),
    Input('sales-query-context', 'data'),
    prevent_initial_call=False,
)
def update_revenue_chart(query_context):
    if not query_context:
        raise PreventUpdate
    start_time = time.time()
    print(f"[CALLBACK] update_revenue_chart triggered")
    # Parse dates
    start_date = date.fromisoformat(query_context['start_date'])
    end_date = date.fromisoformat(query_context['end_date'])
    
    # Build and return the daily revenue chart
    result = build_daily_revenue_chart(start_date, end_date)
    _log_timing('update_revenue_chart', start_time)
    return result


# update_additional_charts callback removed; charts moved to drilldown


@dash.callback(
    Output('sales-by-principal', 'figure'),
    Input('sales-query-context', 'data'),
    prevent_initial_call=False,
)
def update_sales_by_principal_chart(query_context):
    if not query_context:
        raise PreventUpdate
    start_time = time.time()
    print(f"[CALLBACK] update_sales_by_principal_chart triggered")
    start_date = date.fromisoformat(query_context['start_date'])
    end_date = date.fromisoformat(query_context['end_date'])
    
    result = build_sales_by_principal_chart(start_date, end_date)
    _log_timing('update_sales_by_principal_chart', start_time)
    return result


@dash.callback(
    Output('sales-kpi-total-revenue', 'children'),
    Output('sales-kpi-transactions', 'children'),
    Output('sales-kpi-avg-transaction-value', 'children'),
    Output('sales-kpi-gross-margin', 'children'),
    Output('sales-kpi-total-revenue-change', 'children'),
    Output('sales-kpi-transactions-change', 'children'),
    Output('sales-kpi-avg-transaction-value-change', 'children'),
    Output('sales-kpi-gross-margin-change', 'children'),
    Output('sales-query-context', 'data'),
    Output('sales-date-from', 'value'),
    Output('sales-date-until', 'value'),
    Output('sales-btn-mtd', 'variant'),
    Output('sales-btn-qtd', 'variant'),
    Output('sales-btn-ytd', 'variant'),
    Output('sales-btn-last-month', 'variant'),
    Output('sales-last-updated', 'children'),
    Input('sales-global-query-context', 'data'),
    Input('sales-btn-apply', 'n_clicks'),
    Input('sales-btn-mtd', 'n_clicks'),
    Input('sales-btn-qtd', 'n_clicks'),
    Input('sales-btn-ytd', 'n_clicks'),
    Input('sales-btn-last-month', 'n_clicks'),
    State('sales-date-from', 'value'),
    State('sales-date-until', 'value'),
    prevent_initial_call=False,
)
def update_kpi_cards(global_query_context, n_clicks, n_clicks_mtd, n_clicks_qtd, n_clicks_ytd, n_clicks_last_month, date_from, date_until):
    # Auto-load when global context is available or when any button is clicked
    if not any([global_query_context, n_clicks, n_clicks_mtd, n_clicks_qtd, n_clicks_ytd, n_clicks_last_month]):
        raise PreventUpdate
    
    start_time = time.time()
    print(f"[CALLBACK] update_kpi_cards triggered")
    
    # Use ctx.triggered_id to determine which button was actually clicked
    trigger_id = dash.callback_context.triggered_id if dash.callback_context.triggered else None
    print(f"[DEBUG] Trigger ID: {trigger_id}")
    
    # Determine date range based on trigger
    if (trigger_id is None or trigger_id == 'sales-global-query-context') and global_query_context:
        try:
            start_date = date.fromisoformat(global_query_context['start_date'])
            end_date = date.fromisoformat(global_query_context['end_date'])
            active_preset = None
            print("[ACTION] Syncing from global query context")
        except Exception:
            start_date, end_date = get_mtd_dates()
            active_preset = 'mtd'
    elif trigger_id == 'sales-btn-mtd':
        start_date, end_date = get_mtd_dates()
        active_preset = 'mtd'
        print("[ACTION] MTD button clicked")
    elif trigger_id == 'sales-btn-qtd':
        # Quarter-to-Date
        today = date.today()
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        start_date = date(today.year, quarter_start_month, 1)
        end_date = today
        active_preset = 'qtd'
        print("[ACTION] QTD button clicked")
    elif trigger_id == 'sales-btn-ytd':
        # Year-to-Date
        today = date.today()
        start_date = date(today.year, 1, 1)
        end_date = today
        active_preset = 'ytd'
        print("[ACTION] YTD button clicked")
    elif trigger_id == 'sales-btn-last-month':
        # Last Month
        today = date.today()
        if today.month == 1:
            start_date = date(today.year - 1, 12, 1)
            end_date = date(today.year - 1, 12, 31)
        else:
            start_date = date(today.year, today.month - 1, 1)
            # Last day of previous month
            end_date = date(today.year, today.month, 1) - timedelta(days=1)
        active_preset = 'last_month'
        print("[ACTION] Last Month button clicked")
    else:
        # Use custom dates or MTD as default
        start_date, end_date = get_mtd_dates()
        active_preset = None
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
        if trigger_id == 'sales-btn-apply':
            print("[ACTION] Apply button clicked")
    
    # Get KPI data
    kpi_start = time.time()
    try:
        # Get revenue data
        revenue_comparison = get_revenue_comparison(start_date, end_date)
        print(f"[TIMING] get_revenue_comparison: {time.time() - kpi_start:.3f}s")
        current = revenue_comparison['current']
        deltas = revenue_comparison['deltas']
        
        # Get profit data for Gross Margin
        profit_start = time.time()
        profit_summary = query_profit_summary(start_date, end_date)
        print(f"[TIMING] query_profit_summary: {time.time() - profit_start:.3f}s")
        
        # Calculate previous period for comparison
        days_diff = (end_date - start_date).days + 1
        prev_start = start_date - timedelta(days=days_diff)
        prev_end = start_date - timedelta(days=1)
        
        # Get previous period profit data
        prev_profit = query_profit_summary(prev_start, prev_end)
        
        # Format KPI values
        revenue_text = f'Rp {current["revenue"]:,.0f}'
        transactions_text = f'{current["transactions"]:,.0f}'
        atv_text = f'Rp {current["avg_transaction_value"]:,.0f}'
        
        # Gross Margin calculations
        current_margin = profit_summary.get('gross_margin_pct', 0)
        prev_margin = prev_profit.get('gross_margin_pct', 0)
        margin_change = current_margin - prev_margin
        margin_change_pct = (margin_change / prev_margin * 100) if prev_margin != 0 else 0
        
        gross_margin_text = f'{current_margin:.1f}%'
        
        # Format change texts with proper dmc.Text components with color
        revenue_change_color = 'green' if deltas['revenue_pct'] >= 0 else 'red'
        transactions_change_color = 'green' if deltas['transactions_pct'] >= 0 else 'red'
        atv_change_color = 'green' if deltas['avg_transaction_value_pct'] >= 0 else 'red'
        margin_change_color = 'green' if margin_change >= 0 else 'red'
        
        revenue_change_text = dmc.Text(
            f'vs prev period: Rp {deltas["revenue"]:,.0f} ({deltas["revenue_pct"]:+.1f}%)',
            size='xs', c=revenue_change_color
        )
        transactions_change_text = dmc.Text(
            f'vs prev period: {deltas["transactions"]:,.0f} ({deltas["transactions_pct"]:+.1f}%)',
            size='xs', c=transactions_change_color
        )
        atv_change_text = dmc.Text(
            f'vs prev period: Rp {deltas["avg_transaction_value"]:,.0f} ({deltas["avg_transaction_value_pct"]:+.1f}%)',
            size='xs', c=atv_change_color
        )
        margin_change_text = dmc.Text(
            f'vs prev period: {margin_change:+.1f}pp ({margin_change_pct:+.1f}%)',
            size='xs', c=margin_change_color
        )
        
    except Exception as e:
        print(f"Error in KPI calculation: {e}")
        # Fallback values if data fetch fails
        revenue_text = 'Rp 0'
        transactions_text = '0'
        atv_text = 'Rp 0'
        gross_margin_text = '0.0%'
        revenue_change_text = dmc.Text('vs prev period: Rp 0 (0%)', size='xs', c='gray')
        transactions_change_text = dmc.Text('vs prev period: 0 (0%)', size='xs', c='gray')
        atv_change_text = dmc.Text('vs prev period: Rp 0 (0%)', size='xs', c='gray')
        margin_change_text = dmc.Text('vs prev period: 0.0pp (0%)', size='xs', c='gray')
    
    # Set button variants based on active preset
    btn_mtd_variant = 'filled' if active_preset == 'mtd' else 'light'
    btn_qtd_variant = 'filled' if active_preset == 'qtd' else 'light'
    btn_ytd_variant = 'filled' if active_preset == 'ytd' else 'light'
    btn_last_month_variant = 'filled' if active_preset == 'last_month' else 'light'
    
    # Update last updated text
    last_updated_text = 'Last updated: Just now'
    
    _log_timing('update_kpi_cards', start_time)
    query_context = {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'n_clicks': int(n_clicks) if n_clicks is not None else 0,
    }

    return (
        revenue_text,
        transactions_text,
        atv_text,
        gross_margin_text,
        revenue_change_text,
        transactions_change_text,
        atv_change_text,
        margin_change_text,
        query_context,
        start_date.isoformat(),
        end_date.isoformat(),
        btn_mtd_variant,
        btn_qtd_variant,
        btn_ytd_variant,
        btn_last_month_variant,
        last_updated_text,
    )


@dash.callback(
    Output('sales-top-products-table', 'data'),
    Input('sales-query-context', 'data'),
    prevent_initial_call=False,
)
def update_top_products_table(query_context):
    if not query_context:
        raise PreventUpdate
    start_time = time.time()
    print(f"[CALLBACK] update_top_products_table triggered")
    # Parse dates
    start_date = date.fromisoformat(query_context['start_date'])
    end_date = date.fromisoformat(query_context['end_date'])
    
    try:
        # Get top products data
        query_start = time.time()
        top_products_df = get_top_products(start_date, end_date, limit=20)
        print(f"[TIMING] get_top_products: {time.time() - query_start:.3f}s")
        
        if top_products_df.empty:
            return {
                'head': ['Name', 'Category', 'Quantity Sold', 'Revenue'],
                'body': [['No data available', '', '', '']]
            }
        
        # Format the data for dmc.Table
        table_data = {
            'head': ['Name', 'Category', 'Quantity Sold', 'Revenue'],
            'body': []
        }
        
        for _, row in top_products_df.iterrows():
            table_data['body'].append([
                str(row['product_name']),
                str(row['category']),
                f"{int(row['quantity_sold']):,}",
                f"Rp {row['total_unit_price']:,.0f}"
            ])
        
        _log_timing('update_top_products_table', start_time)
        return table_data
        
    except Exception as e:
        print(f"Error updating top products table: {e}")
        _log_timing('update_top_products_table (error)', start_time)
        return {
            'head': ['Name', 'Category', 'Quantity Sold', 'Revenue'],
            'body': [['Error loading data', '', '', '']]
        }