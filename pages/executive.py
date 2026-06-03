"""Executive Summary — C-level one-page dashboard.

Single page with 4 KPI cards (Revenue, Gross Profit, COGS, Transactions)
and a sparkline trend chart. Data comes from DuckDB in-memory via
`services/duckdb_connector.py` — zero ETL/Odoo imports.

Design follows the Cohere Design System established in app.py theme:
- Space Grotesk headings, Inter body
- 22px border radius cards
- Minimal shadows
- Cohere enterprise color palette
"""

import dash
from dash import dcc, Output, Input, State
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
import plotly.graph_objects as go
from datetime import date, timedelta
import time

from services.duckdb_connector import (
    query_profit_summary,
    query_profit_trends,
)
from components import create_loading_modal


# ── Page registration ──────────────────────────────────────────────
dash.register_page(__name__, path='/executive', name='Executive Summary',
                   title='NKDash — Executive Summary')


# ── Constants ──────────────────────────────────────────────────────
SPARKLINE_HEIGHT = 200


# ── Helpers ────────────────────────────────────────────────────────

def _coerce_date(v):
    """Coerce a value to date or return None."""
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        try:
            return date.fromisoformat(v)
        except Exception:
            pass
    return None


def _fmt_rp(value):
    """Format a number as Indonesian Rupiah string."""
    if value is None:
        return 'Rp –'
    return f'Rp {value:,.0f}'


def _fmt_pct(value):
    """Format a number as percentage string."""
    if value is None:
        return '–'
    return f'{value:.1f}%'


def _fmt_num(value):
    """Format a number with thousands separator."""
    if value is None:
        return '–'
    return f'{value:,}'


def _build_sparkline(start_date, end_date):
    """Build a dual-axis sparkline: Revenue bars + Profit margin line."""
    trends = query_profit_trends(start_date, end_date, period='daily')

    fig = go.Figure()

    if trends is None or trends.empty:
        fig.update_layout(
            template='plotly_white', height=SPARKLINE_HEIGHT,
            margin=dict(t=20, b=30, l=50, r=20),
            annotations=[dict(text='No data for selected range',
                              x=0.5, y=0.5, xref='paper', yref='paper',
                              showarrow=False, font=dict(size=14, color='gray'))],
        )
        return fig

    # Ensure date column is datetime
    if trends['date'].dtype == 'object':
        trends['date'] = __import__('pandas').to_datetime(trends['date'])

    # Revenue bars
    fig.add_trace(go.Bar(
        x=trends['date'],
        y=trends['revenue'],
        name='Revenue',
        marker_color='#228be6',
        marker_opacity=0.7,
        yaxis='y',
    ))

    # Profit margin line (on secondary axis)
    margin_data = []
    for _, row in trends.iterrows():
        rev = row.get('revenue', 0) or 0
        gp = row.get('gross_profit', 0) or 0
        margin_data.append((gp / rev * 100) if rev > 0 else 0)

    fig.add_trace(go.Scatter(
        x=trends['date'],
        y=margin_data,
        name='Margin %',
        mode='lines+markers',
        line=dict(color='#40c057', width=2),
        marker=dict(size=4),
        yaxis='y2',
    ))

    title = (start_date.strftime('%d %b %Y') if start_date == end_date
             else f"{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}")

    fig.update_layout(
        template='plotly_white', height=SPARKLINE_HEIGHT,
        title=dict(text=title, font=dict(size=13), x=0, xanchor='left'),
        margin=dict(t=45, b=30, l=50, r=50),
        legend=dict(orientation='h', yanchor='bottom', y=1.02,
                    xanchor='left', x=0, font=dict(size=11), title_text=''),
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis=dict(tickprefix='Rp ', tickformat=',.0f', title=''),
        yaxis2=dict(overlaying='y', side='right', ticksuffix='%',
                    title='', rangemode='tozero'),
        xaxis=dict(title=''),
        barmode='group',
        bargap=0.2,
    )

    return fig


def _kpi_card(label, value_id, value_default, subtitle_id=None,
              subtitle_default=None, color='blue', icon=None):
    """Build a Cohere-styled KPI card with colored top border."""
    children = [
        dmc.Group([
            dmc.Text(icon, size='lg') if icon else None,
            dmc.Text(label, size='xs', c='dimmed', fw=700,
                     style={'textTransform': 'uppercase', 'letterSpacing': '0.5px'}),
        ], gap='xs', align='center'),
        dmc.Space(h=4),
        dmc.Text(value_default, id=value_id, size='xl', fw=700),
    ]
    if subtitle_id:
        children += [
            dmc.Space(h=4),
            dmc.Text(subtitle_default, id=subtitle_id, size='xs', c='dimmed'),
        ]

    return dmc.Paper(
        dmc.Stack(children, gap=0),
        p='lg', radius='xl', withBorder=True, shadow='xs',
        style={'borderTop': f'3px solid var(--mantine-color-{color}-6)', 'flex': '1'},
    )


# ── Layout ─────────────────────────────────────────────────────────

layout = dmc.Container([
    dcc.Location(id='exec-location', refresh=False),

    # Loading modal
    create_loading_modal(
        modal_id='exec-loading-modal',
        status_id='exec-loading-status',
        error_id='exec-loading-error',
        cancel_id='exec-cancel',
        title='Loading Executive Summary',
        show_cancel=False,
        show_progress=True,
    ),

    # Trigger store
    dcc.Store(id='exec-trigger', storage_type='memory', data=None),
    dcc.Store(id='exec-view-state', storage_type='session'),

    # ── Header bar ──────────────────────────────────────────────
    dmc.Paper(
        dmc.Group([
            dmc.Title('Executive Summary', order=4),
            dmc.Group([
                dmc.Button('W', variant='subtle', size='xs', id='exec-btn-weekly'),
                dmc.Button('M', variant='subtle', size='xs', id='exec-btn-monthly'),
                dmc.Button('Q', variant='subtle', size='xs', id='exec-btn-quarterly'),
                dmc.Button('Y', variant='subtle', size='xs', id='exec-btn-yearly'),
                dmc.Divider(orientation='vertical', style={'height': '24px'}),
                dmc.DatePickerInput(
                    value=date.today().replace(day=1), id='exec-date-from',
                    size='xs', w=130, persistence=True, persistence_type='session',
                ),
                dmc.Text('–', c='dimmed', size='sm'),
                dmc.DatePickerInput(
                    value=date.today(), id='exec-date-until',
                    size='xs', w=130, persistence=True, persistence_type='session',
                ),
                dmc.Divider(orientation='vertical', style={'height': '24px'}),
                dmc.Button('Apply', id='exec-btn-apply', variant='filled', size='xs'),
            ], gap=6, align='center', wrap='wrap'),
        ], justify='space-between', align='center', wrap='wrap'),
        p='xs', px='md', radius='md', withBorder=True, shadow='xs', mb='sm',
    ),

    # ── KPI Cards Row ───────────────────────────────────────────
    dmc.Grid([
        dmc.GridCol(
            _kpi_card('Revenue', 'exec-kpi-revenue', 'Rp 0',
                      'exec-kpi-revenue-delta', '– vs previous period', 'blue', '💰'),
            span=3, style={'display': 'flex'},
        ),
        dmc.GridCol(
            _kpi_card('Gross Profit', 'exec-kpi-gp', 'Rp 0',
                      'exec-kpi-margin', '0.0% margin', 'teal', '📈'),
            span=3, style={'display': 'flex'},
        ),
        dmc.GridCol(
            _kpi_card('COGS', 'exec-kpi-cogs', 'Rp 0',
                      'exec-kpi-cogs-pct', '0.0% of revenue', 'red', '📦'),
            span=3, style={'display': 'flex'},
        ),
        dmc.GridCol(
            _kpi_card('Transactions', 'exec-kpi-txn', '0',
                      'exec-kpi-atv', 'Rp 0 avg', 'violet', '🛒'),
            span=3, style={'display': 'flex'},
        ),
    ], gutter='sm', mb='sm'),

    # ── Trend Chart ─────────────────────────────────────────────
    dmc.Paper(
        dmc.Stack([
            dmc.Group([
                dmc.Text('Revenue & Margin Trend', fw=600, size='sm'),
                dmc.Badge('Live', color='green', variant='dot', size='sm'),
            ], justify='space-between'),
            dcc.Graph(id='exec-sparkline', config={'displayModeBar': False}),
        ], gap='xs'),
        p='md', pt='sm', radius='xl', withBorder=True, shadow='xs',
    ),
], size='100%', px='md', py='sm')


# ══════════════════════════════════════════════════════════════════
#  Callbacks
# ══════════════════════════════════════════════════════════════════

@dash.callback(
    Output('exec-loading-modal', 'opened', allow_duplicate=True),
    Output('exec-loading-status', 'children', allow_duplicate=True),
    Output('exec-loading-error', 'style', allow_duplicate=True),
    Output('exec-trigger', 'data'),
    Input('exec-btn-apply', 'n_clicks'),
    Input('exec-btn-weekly', 'n_clicks'),
    Input('exec-btn-monthly', 'n_clicks'),
    Input('exec-btn-quarterly', 'n_clicks'),
    Input('exec-btn-yearly', 'n_clicks'),
    prevent_initial_call=True,
)
def exec_open_modal(_apply, _weekly, _monthly, _quarterly, _yearly):
    """Open loading modal and trigger data fetch."""
    ctx = dash.callback_context
    trig = getattr(ctx, 'triggered_id', None)
    if not trig:
        raise PreventUpdate

    return (
        True,
        'Loading executive data…',
        {'display': 'none'},
        {'triggered_id': trig, 'nonce': time.time()},
    )


@dash.callback(
    Output('exec-sparkline', 'figure'),
    Output('exec-kpi-revenue', 'children'),
    Output('exec-kpi-revenue-delta', 'children'),
    Output('exec-kpi-gp', 'children'),
    Output('exec-kpi-margin', 'children'),
    Output('exec-kpi-cogs', 'children'),
    Output('exec-kpi-cogs-pct', 'children'),
    Output('exec-kpi-txn', 'children'),
    Output('exec-kpi-atv', 'children'),
    Output('exec-date-from', 'value'),
    Output('exec-date-until', 'value'),
    Output('exec-loading-modal', 'opened', allow_duplicate=True),
    Output('exec-loading-status', 'children', allow_duplicate=True),
    Output('exec-loading-error', 'style', allow_duplicate=True),
    Input('exec-trigger', 'data'),
    State('exec-date-from', 'value'),
    State('exec-date-until', 'value'),
    prevent_initial_call=True,
)
def exec_update(trigger_data, dfrom_st, duntil_st):
    """Fetch data and update all KPI cards + sparkline."""
    ctx = dash.callback_context
    trig = None
    if trigger_data and isinstance(trigger_data, dict):
        trig = trigger_data.get('triggered_id')
    if not trig:
        raise PreventUpdate

    today = date.today()

    # ── Determine date range ────────────────────────────────────
    def _preset(key):
        if key == 'weekly':
            return today - timedelta(days=6), today
        if key == 'monthly':
            return today.replace(day=1), today
        if key == 'quarterly':
            q = (today.month - 1) // 3
            return date(today.year, q * 3 + 1, 1), today
        if key == 'yearly':
            return date(today.year, 1, 1), today
        return today, today

    preset_map = {
        'exec-btn-weekly': 'weekly',
        'exec-btn-monthly': 'monthly',
        'exec-btn-quarterly': 'quarterly',
        'exec-btn-yearly': 'yearly',
    }

    if trig in preset_map:
        start, end = _preset(preset_map[trig])
    else:
        start = _coerce_date(dfrom_st) or today.replace(day=1)
        end = _coerce_date(duntil_st) or today

    # ── Build sparkline ─────────────────────────────────────────
    fig = _build_sparkline(start, end)

    # ── Fetch KPIs ──────────────────────────────────────────────
    try:
        ps = query_profit_summary(start, end)
        rev = ps.get('revenue', 0) or 0
        cogs = ps.get('cogs', 0) or 0
        gp = ps.get('gross_profit', 0) or 0
        gm = ps.get('gross_margin_pct', 0) or 0
        atv = ps.get('avg_transaction_value', 0) or 0
        txns = ps.get('transactions', 0) or 0

        # Previous period comparison
        days = (end - start).days + 1
        p_end = date.fromordinal(start.toordinal() - 1)
        p_start = date.fromordinal(start.toordinal() - days)
        ps_prev = query_profit_summary(p_start, p_end)
        p_rev = ps_prev.get('revenue', 0) or 0

        delta = rev - p_rev
        dpct = (delta / p_rev * 100) if p_rev else None
        dtxt = (f"{dpct:+.1f}% vs prev ({_fmt_rp(delta)})"
                if dpct is not None else f"{_fmt_rp(delta)} vs prev")

        cogs_pct = (cogs / rev * 100) if rev > 0 else 0

        kpi_rev = _fmt_rp(rev)
        kpi_rev_delta = dtxt
        kpi_gp = _fmt_rp(gp)
        kpi_margin = f'{gm:.1f}% margin'
        kpi_cogs = _fmt_rp(cogs)
        kpi_cogs_pct = f'{cogs_pct:.1f}% of revenue'
        kpi_txn = _fmt_num(txns)
        kpi_atv = f'{_fmt_rp(atv)} avg'

    except Exception as exc:
        print(f'[executive] query error: {exc}')
        kpi_rev = 'Rp 0'
        kpi_rev_delta = '–'
        kpi_gp = 'Rp 0'
        kpi_margin = '0.0% margin'
        kpi_cogs = 'Rp 0'
        kpi_cogs_pct = '0.0% of revenue'
        kpi_txn = '0'
        kpi_atv = 'Rp 0 avg'

    return (
        fig,
        kpi_rev, kpi_rev_delta,
        kpi_gp, kpi_margin,
        kpi_cogs, kpi_cogs_pct,
        kpi_txn, kpi_atv,
        start, end,
        False, 'Complete', {'display': 'none'},
    )
