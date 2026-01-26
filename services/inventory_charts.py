from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

ABC_COLORS = {
    "A": "#2f9e44",
    "B": "#f08c00",
    "C": "#e03131",
}

SELL_THROUGH_COLORS = {
    "Top": "#2f9e44",
    "Bottom": "#e03131",
}


def _build_empty_figure(message: str, title: str, height: int = 400) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=14, color="gray"),
    )
    fig.update_layout(title=title, template="plotly_white", height=height)
    return fig


def _format_date_range(start_date: date, end_date: date) -> str:
    if start_date == end_date:
        return start_date.strftime("%d %b %Y")
    return f"{start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}"


def build_abc_pareto_chart(
    items: pd.DataFrame,
    start_date: date,
    end_date: date,
    limit: int = 30,
) -> go.Figure:
    if items is None or items.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No sales data available for the selected period.",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=14, color="gray"),
        )
        fig.update_layout(
            title="ABC Pareto",
            template="plotly_white",
            height=400,
        )
        return fig

    df = items.copy()
    df["revenue"] = pd.to_numeric(df.get("revenue"), errors="coerce").fillna(0)
    df["product_name"] = df.get("product_name", "").fillna("")
    df = df.sort_values("revenue", ascending=False).reset_index(drop=True)

    total_revenue = float(df["revenue"].sum())
    top_n = min(limit, len(df))
    df_plot = df.head(top_n).copy()

    if total_revenue > 0:
        df_plot["cumulative_share"] = df_plot["revenue"].cumsum() / total_revenue
    else:
        df_plot["cumulative_share"] = 0.0

    df_plot["sku_label"] = df_plot["product_name"].replace({"": "Unknown"})

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df_plot["sku_label"],
            y=df_plot["revenue"],
            name="Revenue",
            marker_color="#1864ab",
            hovertemplate="SKU: %{x}<br>Revenue: Rp %{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_plot["sku_label"],
            y=df_plot["cumulative_share"],
            name="Cumulative Share",
            mode="lines+markers",
            yaxis="y2",
            marker=dict(color="#343a40"),
            hovertemplate="SKU: %{x}<br>Cumulative: %{y:.1%}<extra></extra>",
        )
    )

    if start_date == end_date:
        title_date = start_date.strftime("%d %b %Y")
    else:
        title_date = f"{start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}"

    title = f"ABC Pareto (Top {top_n} SKUs) - {title_date}"

    fig.update_layout(
        title=title,
        template="plotly_white",
        height=420,
        margin=dict(t=80, b=70, l=60, r=60),
        xaxis=dict(title="SKU", tickangle=35),
        yaxis=dict(title="Revenue (Rp)", tickprefix="Rp ", tickformat=",.0f"),
        yaxis2=dict(
            title="Cumulative Share",
            overlaying="y",
            side="right",
            tickformat=".0%",
            range=[0, 1],
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def build_stock_cover_distribution_chart(
    items: pd.DataFrame,
    as_of_date: date,
    lookback_days: int,
    low_stock_days: int,
    bins: int = 20,
) -> go.Figure:
    if items is None or items.empty:
        return _build_empty_figure(
            "No stock snapshot data available for the selected date.",
            "Days of Cover Distribution",
            height=400,
        )

    df = items.copy()
    df["days_of_cover"] = pd.to_numeric(df.get("days_of_cover"), errors="coerce")
    df = df[df["days_of_cover"].notna()]

    if df.empty:
        return _build_empty_figure(
            "No sales history found to compute days of cover.",
            "Days of Cover Distribution",
            height=400,
        )

    fig = px.histogram(
        df,
        x="days_of_cover",
        nbins=bins,
        color_discrete_sequence=["#4c6ef5"],
    )

    fig.update_layout(
        title=f"Days of Cover Distribution (as of {as_of_date.strftime('%d %b %Y')})",
        template="plotly_white",
        height=400,
        margin=dict(t=80, b=60, l=60, r=40),
        xaxis=dict(title="Days of Cover"),
        yaxis=dict(title="SKU Count"),
    )

    fig.add_vline(
        x=low_stock_days,
        line_dash="dash",
        line_color="#fa5252",
        annotation_text=f"Low stock ({low_stock_days}d)",
        annotation_position="top",
    )

    return fig


def build_low_stock_chart(
    items: pd.DataFrame,
    as_of_date: date,
    low_stock_days: int,
    limit: int = 10,
) -> go.Figure:
    if items is None or items.empty:
        return _build_empty_figure(
            "No stock snapshot data available for the selected date.",
            "Low Stock SKUs",
            height=400,
        )

    df = items.copy()
    df["days_of_cover"] = pd.to_numeric(df.get("days_of_cover"), errors="coerce")
    df["product_name"] = df.get("product_name", "").fillna("")
    df = df[df.get("low_stock_flag") == True]

    if df.empty:
        return _build_empty_figure(
            "No low-stock SKUs found for the selected date.",
            "Low Stock SKUs",
            height=400,
        )

    df = df.sort_values("days_of_cover", ascending=True).head(limit)
    df["sku_label"] = df["product_name"].replace({"": "Unknown"})

    fig = px.bar(
        df,
        x="days_of_cover",
        y="sku_label",
        orientation="h",
        color="days_of_cover",
        color_continuous_scale="Reds",
    )

    fig.update_layout(
        title=f"Lowest Days of Cover (≤ {low_stock_days} days)",
        template="plotly_white",
        height=400,
        margin=dict(t=80, b=60, l=80, r=40),
        xaxis=dict(title="Days of Cover"),
        yaxis=dict(title="SKU", automargin=True),
        coloraxis_showscale=False,
    )

    fig.update_traces(
        hovertemplate="SKU: %{y}<br>Days of cover: %{x:.1f}<extra></extra>"
    )

    return fig


def build_sell_through_by_category_chart(
    categories: pd.DataFrame,
    start_date: date,
    end_date: date,
    limit: int = 12,
) -> go.Figure:
    if categories is None or categories.empty:
        return _build_empty_figure(
            "No sell-through data available for the selected period.",
            "Sell-through by Category",
            height=400,
        )

    df = categories.copy()
    df["sell_through"] = pd.to_numeric(df.get("sell_through"), errors="coerce").fillna(0)
    df["units_sold"] = pd.to_numeric(df.get("units_sold"), errors="coerce").fillna(0)
    df["product_category"] = df.get("product_category", "Unknown Category").fillna("Unknown Category")

    df = df.sort_values("units_sold", ascending=False).head(limit)
    date_range = _format_date_range(start_date, end_date)

    fig = px.bar(
        df,
        x="product_category",
        y="sell_through",
        color="sell_through",
        color_continuous_scale="Blues",
    )

    fig.update_layout(
        title=f"Sell-through by Category - {date_range}",
        template="plotly_white",
        height=400,
        margin=dict(t=80, b=70, l=60, r=40),
        xaxis=dict(title="Category", tickangle=30),
        yaxis=dict(title="Sell-through", tickformat=".0%"),
        coloraxis_showscale=False,
    )

    fig.update_traces(
        hovertemplate="Category: %{x}<br>Sell-through: %{y:.1%}<extra></extra>"
    )

    return fig


def build_sell_through_top_bottom_chart(
    items: pd.DataFrame,
    start_date: date,
    end_date: date,
    limit: int = 8,
) -> go.Figure:
    if items is None or items.empty:
        return _build_empty_figure(
            "No sell-through data available for the selected period.",
            "Top/Bottom Sell-through SKUs",
            height=400,
        )

    df = items.copy()
    df["sell_through"] = pd.to_numeric(df.get("sell_through"), errors="coerce")
    df["product_name"] = df.get("product_name", "").fillna("")
    df = df[df["sell_through"].notna()]

    if df.empty:
        return _build_empty_figure(
            "No sell-through data available for the selected period.",
            "Top/Bottom Sell-through SKUs",
            height=400,
        )

    top_df = df.sort_values("sell_through", ascending=False).head(limit).copy()
    bottom_df = df.sort_values("sell_through", ascending=True).head(limit).copy()

    top_df["group"] = "Top"
    bottom_df["group"] = "Bottom"

    combined = (
        pd.concat([top_df, bottom_df], ignore_index=True)
        .drop_duplicates(subset=["product_id", "group"], keep="first")
    )

    combined["sku_label"] = combined["product_name"].replace({"": "Unknown"})
    date_range = _format_date_range(start_date, end_date)

    fig = px.bar(
        combined,
        x="sell_through",
        y="sku_label",
        color="group",
        orientation="h",
        color_discrete_map=SELL_THROUGH_COLORS,
    )

    fig.update_layout(
        title=f"Top/Bottom Sell-through SKUs - {date_range}",
        template="plotly_white",
        height=420,
        margin=dict(t=80, b=60, l=90, r=40),
        xaxis=dict(title="Sell-through", tickformat=".0%"),
        yaxis=dict(title="SKU", automargin=True),
        legend_title_text="Group",
    )

    fig.update_traces(
        hovertemplate="SKU: %{y}<br>Sell-through: %{x:.1%}<extra></extra>"
    )

    return fig


def build_abc_category_distribution_chart(
    categories: pd.DataFrame,
    start_date: date,
    end_date: date,
    limit: int = 15,
) -> go.Figure:
    if categories is None or categories.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No category data available for the selected period.",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=14, color="gray"),
        )
        fig.update_layout(
            title="ABC Distribution by Category",
            template="plotly_white",
            height=400,
        )
        return fig

    df = categories.copy()
    df["revenue"] = pd.to_numeric(df.get("revenue"), errors="coerce").fillna(0)
    df["product_category"] = df.get("product_category", "Unknown Category").fillna("Unknown Category")
    df["abc_class"] = df.get("abc_class", "C").fillna("C")

    totals = (
        df.groupby("product_category", as_index=False)
        .agg(total_revenue=("revenue", "sum"))
        .sort_values("total_revenue", ascending=False)
    )

    top_categories = totals.head(limit)["product_category"].tolist()
    df = df[df["product_category"].isin(top_categories)]

    df["abc_class"] = pd.Categorical(df["abc_class"], ["A", "B", "C"], ordered=True)
    df = df.sort_values(["product_category", "abc_class"])

    if start_date == end_date:
        title_date = start_date.strftime("%d %b %Y")
    else:
        title_date = f"{start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}"

    fig = px.bar(
        df,
        x="product_category",
        y="revenue",
        color="abc_class",
        barmode="stack",
        color_discrete_map=ABC_COLORS,
    )

    fig.update_layout(
        title=f"ABC Distribution by Category - {title_date}",
        template="plotly_white",
        height=420,
        margin=dict(t=80, b=70, l=60, r=60),
        xaxis=dict(title="Category", tickangle=30),
        yaxis=dict(title="Revenue (Rp)", tickprefix="Rp ", tickformat=",.0f"),
        legend_title_text="ABC Class",
    )

    fig.update_traces(
        hovertemplate="Category: %{x}<br>Revenue: Rp %{y:,.0f}<br>Class: %{legendgroup}<extra></extra>"
    )

    return fig
