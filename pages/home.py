import dash
from dash import dcc, Output, Input, State
import dash_mantine_components as dmc
import plotly.express as px
import pandas as pd
from datetime import date

from services.overview_metrics import get_total_overview_summary


def _build_total_overview_figure(date_start, date_end=None):
    summary = get_total_overview_summary(date_start, date_end)
    date_start = summary['target_date_start']
    date_end = summary['target_date_end']
    today_amount = summary['today_amount']
    today_qty = summary['today_qty']
    categories_nested = summary['categories_nested']
    prev_amount = summary['prev_amount']

    if categories_nested:
        records = [
            {
                'parent_category': parent,
                'category': child,
                'amount': amt,
            }
            for parent, child_map in categories_nested.items()
            for child, amt in child_map.items()
        ]
        df = pd.DataFrame(records)
        fig = px.sunburst(
            df,
            path=['parent_category', 'category'],
            values='amount',
            color='parent_category',
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
    else:
        df = pd.DataFrame()
        fig = px.sunburst()

    if df.empty:
        fig.update_layout(
            annotations=[dict(text='No POS data available for the selected date.', x=0.5, y=0.5, showarrow=False, font=dict(size=14, color='gray'))]
        )

    delta_amount = today_amount - prev_amount
    delta_pct = (delta_amount / prev_amount * 100) if prev_amount else None

    if delta_pct is None:
        delta_text = f" vs prev period: Rp {delta_amount:,.0f}"
    else:
        delta_text = f" vs prev period: Rp {delta_amount:,.0f} ({delta_pct:+.1f}%)"

    # Title shows single date or range
    if date_start == date_end:
        title_str = f"Total Overview – {date_start.strftime('%d %b %Y')}"
    else:
        title_str = f"Total Overview – {date_start.strftime('%d %b %Y')} to {date_end.strftime('%d %b %Y')}"

    fig.update_layout(
        title=title_str,
        legend_title_text='Product Category',
        template='plotly_white',
        height=420,
        margin=dict(t=90, b=60, l=40, r=40),

        annotations=[
            dict(
                text=f"Revenue: Rp {today_amount:,.0f}",
                x=0,
                y=1.15,
                xref='paper',
                yref='paper',
                showarrow=False,
                font=dict(size=16, color='#1864ab'),
            ),
            dict(
                text=delta_text,
                x=0,
                y=1.07,
                xref='paper',
                yref='paper',
                showarrow=False,
                font=dict(size=12, color='#495057'),
            ),
            dict(
                text=f"Qty sold: {today_qty:,.0f}",
                x=0,
                y=1.0,
                xref='paper',
                yref='paper',
                showarrow=False,
                font=dict(size=12, color='#495057'),
            )
        ],
    )

    return fig


dash.register_page(__name__, path='/', name='Overview', title='Executive Overview')

layout = dmc.Container(
    [
        dmc.Title('Summary Overview', order=2),
        dmc.Text('High-level summary placeholder. Replace with KPI highlights, alerts, and quick stats.', c='dimmed'),
        dmc.Paper(
            dmc.Stack(
                [
                    dmc.Group(
                        [
                            dmc.Button('Weekly', variant='light', size='xs', id='btn-weekly'),
                            dmc.Button('Monthly', variant='light', size='xs', id='btn-monthly'),
                            dmc.Button('Quarterly', variant='light', size='xs', id='btn-quarterly'),
                            dmc.Button('Semesterly', variant='light', size='xs', id='btn-semesterly'),
                            dmc.Button('Yearly', variant='light', size='xs', id='btn-yearly'),
                        ],
                        gap='xs',
                    ),
                    dmc.Group(
                        [
                            dmc.Stack(
                                [
                                    dmc.Text('From:', fw=600),
                                    dmc.Group(
                                        [
                                            dmc.DatePickerInput(value=date.today(), placeholder='Select date', id='date-from'),
                                            dmc.TimeInput(value='07:00', id='time-from'),
                                        ],
                                        gap='sm',
                                    ),
                                ],
                                gap=4,
                            ),
                            dmc.Stack(
                                [
                                    dmc.Text('Until:', fw=600),
                                    dmc.Group(
                                        [
                                            dmc.DatePickerInput(value=date.today(), placeholder='Select date', id='date-until'),
                                            dmc.TimeInput(value='23:30', id='time-until'),
                                        ],
                                        gap='sm',
                                    ),
                                ],
                                gap=4,
                            ),
                            dmc.Button('Apply', id='btn-apply-dates', variant='filled', size='sm'),
                        ],
                        gap='xl',
                        wrap='wrap',
                        align='flex-end',
                    ),
                ],
                gap='md',
            ),
            p='md',
            radius='md',
            withBorder=True,
            mt='md',
        ),
        dmc.Grid(
            [
                dmc.GridCol(
                    dmc.Paper(
                        dcc.Graph(
                            id='total-overview-fig',
                            figure=_build_total_overview_figure(date.today(), date.today()),
                            config={'displayModeBar': False},
                        ),
                        p='md',
                        radius='md',
                        withBorder=True,
                    ),
                    span=6,
                ),
                dmc.GridCol(
                    dmc.Paper('Financial Health', p='md', radius='md', withBorder=True),
                    span=6,
                ),
                dmc.GridCol(
                    dmc.Paper('Customer Experience', p='md', radius='md', withBorder=True),
                    span=4,
                ),
                dmc.GridCol(
                    dmc.Paper('Inventory Management', p='md', radius='md', withBorder=True),
                    span=4,
                ),
                dmc.GridCol(
                    dmc.Paper('Operational Efficiency', p='md', radius='md', withBorder=True),
                    span=4,
                ),
            ],
            gutter='lg',
            mt='lg',
        ),
    ],
    size='lg',
    py='lg'
)


@dash.callback(
    Output('total-overview-fig', 'figure'),
    Input('btn-apply-dates', 'n_clicks'),
    State('date-from', 'value'),
    State('date-until', 'value'),
    State('time-from', 'value'),
    State('time-until', 'value'),
    prevent_initial_call=False,
)
def update_total_overview(n_clicks, date_from_input, date_until, time_from, time_until):
    # Determine which input triggered the callback
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else 'initial_load'

    # Use date_from_input which is the *latest value* of the 'date-from' component
    # regardless of whether the button or the datepicker triggered the update.

    selected_date_str = date_from_input
    selected_date = date.today() # Default

    if selected_date_str:
        try:
            # DatePickerInput returns ISO format string (YYYY-MM-DD)
            selected_date = date.fromisoformat(selected_date_str)
        except (ValueError, TypeError):
            pass # Keep default date.today()

    # Parse start and end dates
    start_date = selected_date
    end_date = start_date
    if date_until:
        try:
            end_date = date.fromisoformat(date_until)
        except (ValueError, TypeError):
            pass

    return _build_total_overview_figure(start_date, end_date)