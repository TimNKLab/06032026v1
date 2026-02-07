import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import date
from .cache import cache
from .profit_metrics import query_profit_trends, query_profit_by_product, query_profit_summary


@cache.memoize()
def build_profit_trends_chart(start_date: date, end_date: date, period: str = 'daily') -> go.Figure:
    """Build profit trends chart with revenue, COGS, and profit lines."""
    df = query_profit_trends(start_date, end_date, period)

    if df.empty or 'gross_profit' not in df.columns:
        fig = go.Figure()
        fig.add_annotation(
            text='No profit data available for the selected period.',
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray'),
        )
        fig.update_layout(
            title='Profit Trends',
            template='plotly_white',
            height=400,
        )
        return fig

    fig = go.Figure()

    # Add revenue line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#1f77b4', width=2),
        hovertemplate='Date: %{x}<br>Revenue: Rp %{y:,.0f}<extra></extra>'
    ))

    # Add COGS line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['cogs'],
        mode='lines+markers',
        name='COGS',
        line=dict(color='#ff7f0e', width=2),
        hovertemplate='Date: %{x}<br>COGS: Rp %{y:,.0f}<extra></extra>'
    ))

    # Add gross profit line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['gross_profit'],
        mode='lines+markers',
        name='Gross Profit',
        line=dict(color='#2ca02c', width=2),
        hovertemplate='Date: %{x}<br>Gross Profit: Rp %{y:,.0f}<extra></extra>'
    ))

    # Update layout
    if start_date == end_date:
        title = f'Profit Trends – {start_date.strftime("%d %b %Y")}'
    else:
        title = f'Profit Trends – {start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'

    fig.update_layout(
        title=title,
        template='plotly_white',
        height=400,
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
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
            title='Amount (Rp)',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray',
            tickprefix='Rp ',
            tickformat=',.0f'
        )
    )
    
    return fig


@cache.memoize()
def build_profit_margin_chart(start_date: date, end_date: date, period: str = 'daily') -> go.Figure:
    """Build profit margin percentage chart."""
    df = query_profit_trends(start_date, end_date, period)

    if df.empty or 'gross_margin_pct' not in df.columns:
        fig = go.Figure()
        fig.add_annotation(
            text='No profit margin data available for the selected period.',
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray'),
        )
        fig.update_layout(
            title='Profit Margin %',
            template='plotly_white',
            height=350,
        )
        return fig

    # Create color based on margin (green for positive, red for negative)
    colors = ['#2ca02c' if x >= 0 else '#d62728' for x in df['gross_margin_pct']]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['gross_margin_pct'],
        mode='lines+markers',
        name='Margin %',
        line=dict(width=2),
        marker=dict(color=colors, size=6),
        hovertemplate='Date: %{x}<br>Margin: %{y:.1f}%<extra></extra>'
    ))

    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.7)

    if start_date == end_date:
        title = f'Profit Margin % – {start_date.strftime("%d %b %Y")}'
    else:
        title = f'Profit Margin % – {start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'

    fig.update_layout(
        title=title,
        template='plotly_white',
        height=350,
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
        margin=dict(t=80, b=60, l=60, r=60),
        showlegend=False,
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        ),
        yaxis=dict(
            title='Margin %',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray',
            ticksuffix='%'
        )
    )
    
    return fig


@cache.memoize()
def build_top_products_profit_chart(start_date: date, end_date: date, limit: int = 20) -> go.Figure:
    """Build top products by profit chart."""
    df = query_profit_by_product(start_date, end_date, limit)

    if df.empty or 'total_profit' not in df.columns:
        fig = go.Figure()
        fig.add_annotation(
            text='No product profit data available for the selected period.',
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray'),
        )
        fig.update_layout(
            title='Top Products by Profit',
            template='plotly_white',
            height=400,
            showlegend=False,
        )
        return fig

    # Truncate long product names for display
    df['display_name'] = df['product_name'].str[:30] + (
        '...' if df['product_name'].str.len() > 30 else ''
    )

    fig = px.bar(
        df,
        x='total_profit',
        y='display_name',
        orientation='h',
        color='total_profit',
        color_continuous_scale='Blues',
        title='Top Products by Gross Profit'
    )

    fig.update_traces(
        hovertemplate='Product: %{y}<br>Gross Profit: Rp %{x:,.0f}<br>Margin: %{customdata:.1f}%<extra></extra>',
        customdata=df['profit_margin_pct']
    )

    if start_date == end_date:
        title = f'Top Products by Profit – {start_date.strftime("%d %b %Y")}'
    else:
        title = f'Top Products by Profit – {start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'

    fig.update_layout(
        title=title,
        template='plotly_white',
        height=min(400 + len(df) * 15, 800),
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
        margin=dict(t=80, b=60, l=200, r=60),
        coloraxis=dict(showscale=False),
        showlegend=False,
        xaxis=dict(
            title='Gross Profit (Rp)',
            tickprefix='Rp ',
            tickformat=',.0f'
        ),
        yaxis=dict(
            title='Product',
            categoryorder='total ascending'
        )
    )
    
    return fig


@cache.memoize()
def build_profit_kpi_cards(start_date: date, end_date: date) -> dict:
    """Build KPI card data for profit dashboard."""
    summary = query_profit_summary(start_date, end_date)
    
    return {
        'revenue': {
            'value': summary['revenue'],
            'label': 'Revenue',
            'format': 'currency',
            'change': 0  # TODO: implement period comparison
        },
        'gross_profit': {
            'value': summary['gross_profit'],
            'label': 'Gross Profit',
            'format': 'currency',
            'change': 0  # TODO: implement period comparison
        },
        'gross_margin': {
            'value': summary['gross_margin_pct'],
            'label': 'Gross Margin',
            'format': 'percentage',
            'change': 0  # TODO: implement period comparison
        },
        'transactions': {
            'value': summary['transactions'],
            'label': 'Transactions',
            'format': 'number',
            'change': 0  # TODO: implement period comparison
        }
    }
