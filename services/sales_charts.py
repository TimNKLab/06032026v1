import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import date

from services.sales_metrics import get_sales_trends_data, get_revenue_comparison


def build_revenue_trend_chart(start_date: date, end_date: date, period: str = 'daily') -> go.Figure:
    """
    Build a revenue trend chart for the specified date range.
    
    Args:
        start_date: Start date for the chart
        end_date: End date for the chart
        period: 'daily', 'weekly', or 'monthly' aggregation
    
    Returns:
        Plotly figure object
    """
    # Get trend data
    trends_df = get_sales_trends_data(start_date, end_date, period)
    
    if trends_df.empty:
        # Create empty chart with "No data" message
        fig = go.Figure()
        fig.add_annotation(
            text='No sales data available for the selected period.',
            x=0.5, y=0.5,
            xref='paper', yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray')
        )
        fig.update_layout(
            title=f'Revenue Trend – {period.capitalize()}',
            template='plotly_white',
            height=400
        )
        return fig
    
    # Create the trend chart
    fig = go.Figure()
    
    # Add revenue line
    fig.add_trace(go.Scatter(
        x=trends_df['date'],
        y=trends_df['revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#1864ab', width=3),
        hovertemplate='Date: %{x}<br>Revenue: Rp %{y:,.0f}<extra></extra>'
    ))
    
    # Add transactions on secondary y-axis
    fig.add_trace(go.Scatter(
        x=trends_df['date'],
        y=trends_df['transactions'],
        mode='lines+markers',
        name='Transactions',
        line=dict(color='#51cf66', width=2),
        yaxis='y2',
        hovertemplate='Date: %{x}<br>Transactions: %{y}<extra></extra>'
    ))
    
    # Format title based on period and date range
    if start_date == end_date:
        title = f'Revenue Trend – {start_date.strftime("%d %b %Y")} ({period.capitalize()})'
    else:
        title = f'Revenue Trend – {start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")} ({period.capitalize()})'
    
    # Update layout
    fig.update_layout(
        title=title,
        template='plotly_white',
        height=400,
        margin=dict(t=80, b=60, l=60, r=60),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        ),
        yaxis=dict(
            title='Revenue (Rp)',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray',
            tickprefix='Rp ',
            tickformat=',.0f'
        ),
        yaxis2=dict(
            title='Transactions',
            overlaying='y',
            side='right',
            showgrid=False
        )
    )
    
    return fig


def build_category_breakdown_chart(start_date: date, end_date: date) -> go.Figure:
    """
    Build a sales by category breakdown chart.
    
    Args:
        start_date: Start date for analysis
        end_date: End date for analysis
    
    Returns:
        Plotly figure object
    """
    from services.overview_metrics import get_total_overview_summary
    
    # Get category data (reuse existing function)
    summary = get_total_overview_summary(start_date, end_date)
    categories_nested = summary['categories_nested']
    
    if not categories_nested:
        # Create empty chart
        fig = go.Figure()
        fig.add_annotation(
            text='No category data available for the selected period.',
            x=0.5, y=0.5,
            xref='paper', yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray')
        )
        fig.update_layout(
            title='Sales by Category',
            template='plotly_white',
            height=400
        )
        return fig
    
    # Convert nested dict to DataFrame
    records = [
        {
            'parent_category': parent,
            'category': child,
            'revenue': amt,
        }
        for parent, child_map in categories_nested.items()
        for child, amt in child_map.items()
    ]
    df = pd.DataFrame(records)
    
    # Aggregate by parent category for donut chart
    parent_totals = df.groupby('parent_category')['revenue'].sum().reset_index()
    
    # Create donut chart
    fig = px.pie(
        parent_totals,
        values='revenue',
        names='parent_category',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    # Format title
    if start_date == end_date:
        title = f'Sales by Category – {start_date.strftime("%d %b %Y")}'
    else:
        title = f'Sales by Category – {start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'
    
    fig.update_traces(
        hovertemplate='Category: %{label}<br>Revenue: Rp %{value:,.0f}<br>%{percent}<extra></extra>',
        textinfo='label+percent',
        textposition='inside'
    )
    
    fig.update_layout(
        title=title,
        template='plotly_white',
        height=400,
        margin=dict(t=80, b=60, l=60, r=60),
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.01
        )
    )
    
    return fig
