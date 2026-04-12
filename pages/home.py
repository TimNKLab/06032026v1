import dash
from dash import dcc, Output, Input, State
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
import plotly.express as px
from datetime import date, timedelta
import time
from typing import Tuple

from services.profit_metrics import query_profit_summary, query_profit_trends
from components import create_loading_modal

CHART_HEIGHT = 380

dash.register_page(__name__, path='/', name='Overview', title='Executive Overview')

# ── helpers ───────────────────────────────────────────────────────

def _empty_fig(msg='No data'):
    fig = px.bar()
    fig.update_layout(
        template='plotly_white', height=CHART_HEIGHT,
        margin=dict(t=30, b=30, l=50, r=20),
        annotations=[dict(text=msg, x=.5, y=.5, xref='paper', yref='paper',
                          showarrow=False, font=dict(size=14, color='gray'))],
    )
    return fig


def _build_figure(d_start, d_end, period='daily'):
    trends = query_profit_trends(d_start, d_end, period=period)
    if trends is None or trends.empty:
        return _empty_fig('No data for selected range.')
    df = trends[['date', 'revenue', 'gross_profit']].rename(
        columns={'revenue': 'Revenue', 'gross_profit': 'Gross Profit'})
    fig = px.bar(df, x='date', y=['Revenue', 'Gross Profit'], barmode='group',
                 color_discrete_map={'Revenue': '#228be6', 'Gross Profit': '#40c057'})
    title = (d_start.strftime('%d %b %Y') if d_start == d_end
             else f"{d_start.strftime('%d %b %Y')} → {d_end.strftime('%d %b %Y')}")
    fig.update_layout(
        title=dict(text=title, font=dict(size=13), x=0, xanchor='left'),
        template='plotly_white', height=CHART_HEIGHT,
        margin=dict(t=45, b=30, l=50, r=20),
        yaxis=dict(tickprefix='Rp ', tickformat=',.0f'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02,
                    xanchor='left', x=0, font=dict(size=11), title_text=''),
        plot_bgcolor='rgba(0,0,0,0)',
    )
    fig.update_xaxes(title_text='')
    fig.update_yaxes(title_text='')
    return fig


def _coerce(v):
    if isinstance(v, date): return v
    if isinstance(v, str):
        try: return date.fromisoformat(v)
        except Exception: pass
    return None


def _kpi_card(label, vid, vdef, sid=None, sdef=None, color='blue'):
    ch = [
        dmc.Text(label, size='xs', c='dimmed', fw=700,
                 style={'textTransform': 'uppercase', 'letterSpacing': '0.5px'}),
        dmc.Space(h=4),
        dmc.Text(vdef, id=vid, size='xl', fw=700),
    ]
    if sid:
        ch += [dmc.Space(h=4), dmc.Text(sdef, id=sid, size='xs', c='dimmed')]
    return dmc.Paper(
        dmc.Stack(ch, gap=0), p='md', radius='md', withBorder=True, shadow='xs',
        style={'borderTop': f'3px solid var(--mantine-color-{color}-6)', 'flex': '1'},
    )


# ── layout ────────────────────────────────────────────────────────

layout = dmc.Container([
    dcc.Location(id='overview-location', refresh=False),
    
    # Loading modal for data fetch operations
    create_loading_modal(
        modal_id='overview-loading-modal',
        status_id='overview-loading-status',
        error_id='overview-loading-error',
        cancel_id='overview-cancel',
        title="Loading Dashboard Data",
        show_cancel=False,  # Quick synchronous operation
        show_progress=True,
    ),
    
    # Trigger store for two-callback pattern
    dcc.Store(id='overview-execute-trigger', storage_type='memory', data=None),
    
    dmc.Paper(
        dmc.Group([
            dmc.Title('Executive Dashboard', order=4),
            dmc.Group([
                dmc.Button('W', variant='subtle', size='xs', id='btn-weekly'),
                dmc.Button('M', variant='subtle', size='xs', id='btn-monthly'),
                dmc.Button('Q', variant='subtle', size='xs', id='btn-quarterly'),
                dmc.Button('S', variant='subtle', size='xs', id='btn-semesterly'),
                dmc.Button('Y', variant='subtle', size='xs', id='btn-yearly'),
                dmc.Divider(orientation='vertical', style={'height': '24px'}),
                dmc.DatePickerInput(
                    value=date.today(), id='date-from', size='xs', w=130,
                    persistence=True, persistence_type='session',   # ← NEW
                ),
                dmc.TimeInput(
                    value='07:00', id='time-from', size='xs', w=70,
                    persistence=True, persistence_type='session',   # ← NEW
                ),
                dmc.Text('–', c='dimmed', size='sm'),
                dmc.DatePickerInput(
                    value=date.today(), id='date-until', size='xs', w=130,
                    persistence=True, persistence_type='session',   # ← NEW
                ),
                dmc.TimeInput(
                    value='23:30', id='time-until', size='xs', w=70,
                    persistence=True, persistence_type='session',   # ← NEW
                ),
                dmc.Divider(orientation='vertical', style={'height': '24px'}),
                dmc.SegmentedControl(
                    id='overview-period', value='daily', size='xs',
                    data=[{'label': 'D', 'value': 'daily'},
                          {'label': 'W', 'value': 'weekly'},
                          {'label': 'M', 'value': 'monthly'}],
                    persistence=True, persistence_type='session',   # ← NEW
                ),
                dmc.Button('Apply', id='btn-apply-dates', variant='filled', size='xs'),
            ], gap=6, align='center', wrap='wrap'),
        ], justify='space-between', align='center', wrap='wrap'),
        p='xs', px='md', radius='md', withBorder=True, shadow='xs', mb='sm',
    ),

    dmc.Grid([
        dmc.GridCol(_kpi_card('Revenue', 'kpi-revenue', 'Rp 0',
                              'kpi-revenue-delta', '–', 'blue'), span=3, style={'display':'flex'}),
        dmc.GridCol(_kpi_card('Gross Profit', 'kpi-gross-profit', 'Rp 0',
                              'kpi-gross-margin', '0.0% margin', 'teal'), span=3, style={'display':'flex'}),
        dmc.GridCol(_kpi_card('Avg Transaction', 'kpi-atv', 'Rp 0',
                              color='violet'), span=3, style={'display':'flex'}),
        dmc.GridCol(_kpi_card('Volume', 'kpi-qty-sold', '0 items',
                              'kpi-transactions', '0 transactions', 'orange'), span=3, style={'display':'flex'}),
    ], gutter='sm', mb='sm'),

    dmc.Paper(
        dmc.Stack([
            dmc.Group([
                dmc.Text('Revenue & Profit Trend', fw=600, size='sm'),
                dmc.Badge('Live', color='green', variant='dot', size='sm'),
            ], justify='space-between'),
            dcc.Graph(id='total-overview-fig', config={'displayModeBar': False}),
        ], gap='xs'),
        p='md', pt='sm', radius='md', withBorder=True, shadow='xs',
    ),
], size='100%', px='md', py='sm')


# ══════════════════════════════════════════════════════════════════
#  Two-callback pattern — trigger (open modal) then execute (query)
# ══════════════════════════════════════════════════════════════════

@dash.callback(
    Output('overview-loading-modal', 'opened', allow_duplicate=True),
    Output('overview-loading-status', 'children', allow_duplicate=True),
    Output('overview-loading-error', 'style', allow_duplicate=True),
    Output('overview-execute-trigger', 'data'),
    Input('btn-apply-dates',  'n_clicks'),
    Input('overview-period',  'value'),
    Input('btn-weekly',       'n_clicks'),
    Input('btn-monthly',      'n_clicks'),
    Input('btn-quarterly',    'n_clicks'),
    Input('btn-semesterly',   'n_clicks'),
    Input('btn-yearly',       'n_clicks'),
    prevent_initial_call=True,
)
def overview_open_modal_and_trigger(_apply_n, _period_in, _weekly_n, _monthly_n, _quarterly_n, _semesterly_n, _yearly_n):
    ctx = dash.callback_context
    trig = getattr(ctx, 'triggered_id', None)
    if not trig:
        raise PreventUpdate

    return (
        True,
        'Loading…',
        {'display': 'none'},
        {'triggered_id': trig, 'nonce': time.time()},
    )

@dash.callback(
    Output('total-overview-fig',  'figure'),
    Output('kpi-revenue',         'children'),
    Output('kpi-revenue-delta',   'children'),
    Output('kpi-gross-profit',    'children'),
    Output('kpi-gross-margin',    'children'),
    Output('kpi-atv',             'children'),
    Output('kpi-qty-sold',        'children'),
    Output('kpi-transactions',    'children'),
    Output('sales-global-query-context', 'data'),
    Output('overview-view-state',        'data'),
    Output('overview-period',     'value'),
    Output('date-from',           'value'),
    Output('date-until',          'value'),
    Output('overview-loading-modal', 'opened', allow_duplicate=True),      # NK_20260408: Modal control
    Output('overview-loading-status', 'children', allow_duplicate=True),   # NK_20260408: Status text
    Output('overview-loading-error', 'style', allow_duplicate=True),       # NK_20260408: Error visibility
    Input('overview-execute-trigger', 'data'),
    State('overview-period',  'value'),
    State('date-from',        'value'),
    State('date-until',       'value'),
    State('time-from',       'value'),
    State('time-until',      'value'),
    State('overview-view-state', 'data'),
    prevent_initial_call=True,  # NK_20260408: No auto-load on page visit
)
def update_overview(execute_trigger,
                    period_in,
                    dfrom_st,
                    duntil_st,
                    tfrom_st,
                    tuntil_st,
                    view_state):

    NO = dash.no_update
    ctx = dash.callback_context
    trig = None
    if execute_trigger and isinstance(execute_trigger, dict):
        trig = execute_trigger.get('triggered_id')
    if not trig:
        raise PreventUpdate

    # ── DEBUG — remove once it works ──────────────────────────
    print(f"[overview] trig={trig}  "
          f"vs={'HAS ' + str(len(view_state)) + ' keys' if view_state else 'NONE'}")

    def _preset_range(key: str) -> Tuple[date, date]:
        today = date.today()
        if key == 'weekly':
            return (today - timedelta(days=6), today)
        if key == 'monthly':
            return (today.replace(day=1), today)
        if key == 'quarterly':
            q = (today.month - 1) // 3
            m0 = q * 3 + 1
            return (date(today.year, m0, 1), today)
        if key == 'semesterly':
            m0 = 1 if today.month <= 6 else 7
            return (date(today.year, m0, 1), today)
        if key == 'yearly':
            return (date(today.year, 1, 1), today)
        return (today, today)

    # Base values
    start = _coerce(dfrom_st) or date.today()
    end = _coerce(duntil_st) or start
    period = period_in or 'daily'

    # Handle preset range buttons
    preset_clicked = {
        'btn-weekly': 'weekly',
        'btn-monthly': 'monthly',
        'btn-quarterly': 'quarterly',
        'btn-semesterly': 'semesterly',
        'btn-yearly': 'yearly',
    }
    if trig in preset_clicked:
        start, end = _preset_range(preset_clicked[trig])

    # If the user changed the period segmented control, keep dates but update grouping
    if trig == 'overview-period' and period_in:
        period = period_in

    # If user clicked Apply, just compute using the current state values
    if trig == 'btn-apply-dates':
        pass

    # ── render (compute from selected dates + period) ─────────────────────────────────────────
    fig = _build_figure(start, end, period)

    global_ctx = dict(start_date=start.isoformat(), end_date=end.isoformat(),
                      period=period, source='overview')

    # ── view_state: ONLY small scalars, NO figure ─────────
    vs = dict(
        has_data=True,
        date_from=start.isoformat(),
        date_until=end.isoformat(),
        period=period,
        time_from=tfrom_st or '07:00',
        time_until=tuntil_st or '23:30',
    )

    try:
        ps   = query_profit_summary(start, end)
        rev  = ps.get('revenue', 0) or 0
        gp   = ps.get('gross_profit', 0) or 0
        gm   = ps.get('gross_margin_pct', 0) or 0
        atv  = ps.get('avg_transaction_value', 0) or 0
        qty  = ps.get('quantity', 0) or 0
        txns = ps.get('transactions', 0) or 0

        days    = (end - start).days + 1
        p_end   = date.fromordinal(start.toordinal() - 1)
        p_start = date.fromordinal(start.toordinal() - days)
        p_rev   = (query_profit_summary(p_start, p_end).get('revenue', 0) or 0)
        delta   = rev - p_rev
        dpct    = (delta / p_rev * 100) if p_rev else None
        dtxt    = (f"{dpct:+.1f}% vs prev (Rp {delta:,.0f})"
                   if dpct is not None else f"Rp {delta:,.0f} vs prev")

        vs.update(
            kpi_revenue=f"Rp {rev:,.0f}",       kpi_revenue_delta=dtxt,
            kpi_gross_profit=f"Rp {gp:,.0f}",   kpi_gross_margin=f"{gm:.1f}% margin",
            kpi_atv=f"Rp {atv:,.0f}",
            kpi_qty_sold=f"{qty:,.0f} items",    kpi_transactions=f"{txns:,} transactions",
        )
    except Exception as exc:
        print(f"[overview] query error: {exc}")
        vs.update(
            kpi_revenue='Rp 0', kpi_revenue_delta='–',
            kpi_gross_profit='Rp 0', kpi_gross_margin='0.0% margin',
            kpi_atv='Rp 0', kpi_qty_sold='0 items', kpi_transactions='0 transactions',
        )

    print(f"[overview] APPLY → storing view_state with {len(vs)} keys")

    return (
        fig,
        vs['kpi_revenue'],      vs['kpi_revenue_delta'],
        vs['kpi_gross_profit'], vs['kpi_gross_margin'],
        vs['kpi_atv'],          vs['kpi_qty_sold'],
        vs['kpi_transactions'],
        global_ctx, vs,
        period,
        start,
        end,
        False,  # Close modal on success
        'Complete',
        {'display': 'none'},  # Hide error
    )

