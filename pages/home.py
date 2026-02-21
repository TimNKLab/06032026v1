import dash
from dash import dcc, Output, Input, State
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
import plotly.express as px
from datetime import date

from services.profit_metrics import query_profit_summary, query_profit_trends

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
#  SINGLE callback — Apply, restore, first-visit
# ══════════════════════════════════════════════════════════════════

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
    Input('app-location',    'pathname'),
    Input('btn-apply-dates', 'n_clicks'),
    State('overview-period', 'value'),
    State('date-from',       'value'),
    State('date-until',      'value'),
    State('time-from',       'value'),
    State('time-until',      'value'),
    State('overview-view-state', 'data'),
    prevent_initial_call=False,
)
def update_overview(pathname, n_clicks,
                    period_st, dfrom_st, duntil_st, tfrom_st, tuntil_st,
                    view_state):

    NO = dash.no_update
    trigs = {t['prop_id'].split('.')[0] for t in (dash.callback_context.triggered or [])}

    # ── DEBUG — remove once it works ──────────────────────────
    print(f"[overview] trigs={trigs}  pathname={pathname}  n_clicks={n_clicks}  "
          f"vs={'HAS ' + str(len(view_state)) + ' keys' if view_state else 'NONE'}")

    # ignore if we're on a different page
    if pathname != '/':
        raise PreventUpdate

    # ── APPLY pressed ─────────────────────────────────────────
    if 'btn-apply-dates' in trigs:
        start  = _coerce(dfrom_st)  or date.today()
        end    = _coerce(duntil_st) or start
        period = period_st or 'daily'

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
        )

    # ── NAV-BACK: restore from view_state (rebuild fig) ──────
    if view_state and view_state.get('has_data'):
        print("[overview] RESTORING from view_state")
        vs = view_state
        start  = _coerce(vs['date_from'])  or date.today()
        end    = _coerce(vs['date_until']) or start
        period = vs.get('period', 'daily')

        # rebuild the figure from params — this is fast
        fig = _build_figure(start, end, period)

        return (
            fig,
            vs.get('kpi_revenue',      'Rp 0'),
            vs.get('kpi_revenue_delta', '–'),
            vs.get('kpi_gross_profit', 'Rp 0'),
            vs.get('kpi_gross_margin', '0.0% margin'),
            vs.get('kpi_atv',         'Rp 0'),
            vs.get('kpi_qty_sold',    '0 items'),
            vs.get('kpi_transactions','0 transactions'),
            NO, NO,
        )

    # ── FIRST VISIT ───────────────────────────────────────────
    print("[overview] FIRST VISIT — empty state")
    return (
        _empty_fig('Click Apply to load data.'),
        'Rp 0', '–', 'Rp 0', '0.0% margin',
        'Rp 0', '0 items', '0 transactions',
        NO, NO,
    )