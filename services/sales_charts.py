import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import date

from services.cache import cache

from services.sales_metrics import (
    get_sales_trends_data,
    get_daily_transaction_counts,
    get_revenue_comparison,
    get_hourly_sales_pattern,
    get_hourly_sales_heatmap_data,
    get_sales_by_principal,
)


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


@cache.memoize()
def build_sales_by_principal_chart(start_date: date, end_date: date, limit: int = 20) -> go.Figure:
    sales_df = get_sales_by_principal(start_date, end_date, limit=limit)

    if sales_df.empty:
        return go.Figure().update_layout(
            title='Sales by Principal',
            template='plotly_white',
            height=350,
            showlegend=False,
        )

    df = sales_df.copy()
    if 'principal' not in df.columns:
        df['principal'] = 'Unknown Principal'
    if 'revenue' not in df.columns:
        df['revenue'] = 0

    df['principal'] = df['principal'].fillna('').astype(str).replace({'': 'Unknown Principal'})
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    df = df.sort_values('revenue', ascending=False).head(limit)

    fig = px.icicle(
        df,
        path=['principal'],
        values='revenue',
        color='revenue',
        color_continuous_scale='Blues',
        branchvalues='total',
    )

    if start_date == end_date:
        title = f'Sales by Principal – {start_date.strftime("%d %b %Y")}'
    else:
        title = f'Sales by Principal – {start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'

    fig.update_traces(
        hovertemplate='Principal: %{label}<br>Revenue: Rp %{value:,.0f}<extra></extra>'
    )
    fig.update_layout(
        title=title,
        template='plotly_white',
        height=350,
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
        margin=dict(t=80, b=60, l=80, r=60),
        coloraxis=dict(showscale=False),
        showlegend=False,
    )

    return fig


@cache.memoize()
def build_daily_revenue_chart(start_date: date, end_date: date) -> go.Figure:
    """
    Build a daily revenue line chart for the specified date range.
    """
    # Get the full sales data including revenue
    trends_df = get_sales_trends_data(start_date, end_date, 'daily')

    if trends_df.empty or 'revenue' not in trends_df.columns:
        fig = go.Figure()
        fig.add_annotation(
            text='No revenue data available for the selected period.',
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray'),
        )
        fig.update_layout(
            title='Daily Revenue',
            template='plotly_white',
            height=400,
        )
        return fig

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=trends_df['date'],
            y=trends_df['revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color='#1864ab', width=3),
            hovertemplate='Date: %{x}<br>Revenue: Rp %{y:,.0f}<extra></extra>',
        )
    )

    if start_date == end_date:
        title = f'Daily Revenue – {start_date.strftime("%d %b %Y")}'
    else:
        title = (
            f'Daily Revenue – {start_date.strftime("%d %b %Y")} to '
            f'{end_date.strftime("%d %b %Y")}'
        )

    fig.update_layout(
        title=title,
        template='plotly_white',
        height=400,
        font=dict(
            family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
        ),
        margin=dict(t=80, b=60, l=60, r=60),
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray',
        ),
        yaxis=dict(
            title='Revenue (Rp)',
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray',
            tickformat=',.0f',
            tickprefix='Rp ',
        ),
    )

    return fig


@cache.memoize()
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
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
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


@cache.memoize()
def build_category_sankey_chart(start_date: date, end_date: date) -> go.Figure:
    """
    Build a Sankey diagram showing sales flow from parent_category > category > brand.
    
    Args:
        start_date: Start date for analysis
        end_date: End date for analysis
    
    Returns:
        Plotly figure object with Sankey diagram
    """
    from services.overview_metrics import get_total_overview_summary
    
    # Get category and brand data
    summary = get_total_overview_summary(start_date, end_date)
    categories_nested = summary['categories_nested']
    brands_nested = summary.get('brands_nested', {})
    
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
            title='Sales Flow – Category Hierarchy',
            template='plotly_white',
            height=500
        )
        return fig
    
    # Build nodes and links for Sankey diagram
    nodes = []
    sources = []
    targets = []
    values = []
    
    # Track node indices
    node_to_index = {}
    current_index = 0
    
    # Determine if we have brand data
    has_brand_data = bool(brands_nested)
    
    if has_brand_data:
        # Step 1: Aggregate revenue for all categories
        category_revenues = {}
        for parent_category, child_map in brands_nested.items():
            for category, brand_map in child_map.items():
                category_key = f"{parent_category} > {category}"
                category_revenues[category_key] = sum(brand_map.values())
        
        # Step 2: Get top 20 categories by revenue
        top_categories = sorted(category_revenues.items(), key=lambda x: x[1], reverse=True)[:20]
        top_category_set = set()
        for category_key, _ in top_categories:
            parent_category, category = category_key.split(" > ", 1)
            top_category_set.add((parent_category, category))
        
        # Step 3: Collect all brands within top categories and get top 30 brands
        brand_revenues = {}
        for parent_category, child_map in brands_nested.items():
            for category, brand_map in child_map.items():
                if (parent_category, category) in top_category_set:
                    for brand, revenue in brand_map.items():
                        if brand not in brand_revenues:
                            brand_revenues[brand] = 0
                        brand_revenues[brand] += revenue
        
        # Get top 30 brands by revenue within top categories
        top_brands = sorted(brand_revenues.items(), key=lambda x: x[1], reverse=True)[:30]
        top_brand_set = {brand for brand, _ in top_brands}
        
        # Three-level flow: parent_category > category > brand (limited to top 20 categories, top 30 brands)
        for parent_category, child_map in brands_nested.items():
            # Only process categories that are in top 20
            valid_categories = [(cat, brand_map) for cat, brand_map in child_map.items() 
                              if (parent_category, cat) in top_category_set]
            
            if not valid_categories:
                continue
                
            # Add parent category node if not exists
            if parent_category not in node_to_index:
                node_to_index[parent_category] = current_index
                nodes.append(parent_category)
                current_index += 1
            
            parent_index = node_to_index[parent_category]
            
            # Add category nodes and brand links
            for category, brand_map in valid_categories:
                if category not in node_to_index:
                    node_to_index[category] = current_index
                    nodes.append(category)
                    current_index += 1
                
                category_index = node_to_index[category]
                
                # Filter brands to only top 30 and calculate category total
                filtered_brand_map = {brand: revenue for brand, revenue in brand_map.items() if brand in top_brand_set}
                
                if filtered_brand_map:
                    # Add link from parent to category
                    category_total = sum(filtered_brand_map.values())
                    sources.append(parent_index)
                    targets.append(category_index)
                    values.append(category_total)
                    
                    # Add brand nodes and links (only top 30)
                    for brand, revenue in filtered_brand_map.items():
                        if brand not in node_to_index:
                            node_to_index[brand] = current_index
                            nodes.append(brand)
                            current_index += 1
                        
                        brand_index = node_to_index[brand]
                        
                        sources.append(category_index)
                        targets.append(brand_index)
                        values.append(revenue)
    else:
        # Two-level flow: parent_category > category (fallback)
        for parent_category, child_map in categories_nested.items():
            # Add parent category node if not exists
            if parent_category not in node_to_index:
                node_to_index[parent_category] = current_index
                nodes.append(parent_category)
                current_index += 1
            
            parent_index = node_to_index[parent_category]
            
            # Add child category nodes and links
            for category, revenue in child_map.items():
                if category not in node_to_index:
                    node_to_index[category] = current_index
                    nodes.append(category)
                    current_index += 1
                
                child_index = node_to_index[category]
                
                sources.append(parent_index)
                targets.append(child_index)
                values.append(revenue)
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color=px.colors.qualitative.Set3 * ((len(nodes) // 12) + 1)
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            hovertemplate='%{source.label} → %{target.label}<br>Revenue: Rp %{value:,.0f}<extra></extra>'
        )
    )])
    
    # Format title based on data availability
    if start_date == end_date:
        date_str = start_date.strftime("%d %b %Y")
    else:
        date_str = f'{start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'
    
    if has_brand_data:
        title = f'Sales Flow – Top 20 Categories > Top 30 Brands – {date_str}'
    else:
        title = f'Sales Flow – Category Hierarchy – {date_str}'
    
    fig.update_layout(
        title=title,
        template='plotly_white',
        height=500,
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif", size=10),
        margin=dict(t=80, b=60, l=60, r=60)
    )
    
    return fig


@cache.memoize()
def build_hourly_heatmap_chart(start_date: date, end_date: date) -> go.Figure:
    """
    Build a heatmap showing hourly sales pattern across multiple days.
    
    Args:
        start_date: Start date for the analysis
        end_date: End date for the analysis
    
    Returns:
        Plotly figure object with heatmap
    """
    heatmap_df = get_hourly_sales_heatmap_data(start_date, end_date)

    if heatmap_df.empty:
        # Create empty chart
        fig = go.Figure()
        fig.add_annotation(
            text='No hourly sales data available for the selected period.<br>Store hours: 07:00-23:00 (Bangkok Time)',
            x=0.5, y=0.5,
            xref='paper', yref='paper',
            showarrow=False,
            font=dict(size=14, color='gray')
        )
        fig.update_layout(
            title='Hourly Sales Pattern (Store Hours: 07:00-23:00)',
            template='plotly_white',
            height=400
        )
        return fig

    heatmap_df = heatmap_df.copy()
    heatmap_df['date'] = pd.to_datetime(heatmap_df['date']).dt.strftime('%Y-%m-%d')
    
    # Create pivot table for heatmap
    heatmap_data = heatmap_df.pivot_table(
        values='revenue', 
        index='date', 
        columns='hour', 
        fill_value=0
    )
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=[f"{h:02d}:00" for h in heatmap_data.columns],
        y=heatmap_data.index,
        colorscale='Viridis',
        hovertemplate='Date: %{y}<br>Hour: %{x}<br>Revenue: Rp %{z:,.0f}<extra></extra>',
        colorbar=dict(title="Revenue (Rp)")
    ))
    
    # Format title
    if start_date == end_date:
        date_str = start_date.strftime("%d %b %Y")
        title = f'Hourly Sales Pattern (Store Hours: 07:00-23:00) – {date_str}'
    else:
        date_str = f'{start_date.strftime("%d %b %Y")} to {end_date.strftime("%d %b %Y")}'
        title = f'Hourly Sales Pattern (Store Hours: 07:00-23:00) – {date_str}'
    
    fig.update_layout(
        title=title,
        template='plotly_white',
        height=400,
        xaxis_title='Hour of Day (Bangkok Time)',
        yaxis_title='Date',
        font=dict(family="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif", size=10),
        margin=dict(t=80, b=60, l=80, r=60)
    )
    
    return fig
