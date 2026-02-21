import dash
from dash import dcc
from dash import html, Output, Input
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
from datetime import date

from services.sales_charts import (
    build_category_sankey_chart,
    build_hourly_heatmap_chart,
)


dash.register_page(
    __name__,
    path='/sales-drilldown',
    name='Sales Drilldowns',
    title='Sales Drilldowns'
)


def layout():
    return dmc.Container(
        [
            dmc.Stack(
                [
                    dmc.Group(
                        [
                            dmc.Stack(
                                [
                                    dmc.Title('Sales Drilldowns', order=3),
                                    dmc.Text(
                                        'Hourly pattern and category flow (Sankey). Uses the same date range as Sales.',
                                        size='sm',
                                        c='dimmed',
                                    ),
                                ],
                                gap=2,
                            ),
                            html.A(
                                dmc.Button(
                                    'Back to Sales',
                                    variant='light',
                                    size='sm',
                                ),
                                href='/sales'
                            ),
                        ],
                        justify='space-between',
                        align='flex-start',
                        wrap='wrap',
                    ),
                    dmc.Alert(
                        'Open the Sales page and click Apply (or choose a preset) to set the date range. Then return here.',
                        color='blue',
                        variant='light',
                    ),
                ],
                gap='sm',
            ),
            dmc.Grid(
                [
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack(
                                [
                                    dmc.Text('Hourly Sales Pattern', fw=600, size='lg'),
                                    dmc.Box(
                                        dcc.Graph(
                                            id='sales-drilldown-hourly-pattern',
                                            figure={},
                                            config={'displayModeBar': False},
                                            style={'height': '420px', 'width': '100%'},
                                        ),
                                        w='100%',
                                    ),
                                ],
                                gap='sm',
                            ),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='md',
                            bg='white',
                        ),
                        span=12,
                    ),
                    dmc.GridCol(
                        dmc.Paper(
                            dmc.Stack(
                                [
                                    dmc.Text('Sales Flow Hierarchy', fw=600, size='lg'),
                                    dmc.Box(
                                        dcc.Graph(
                                            id='sales-drilldown-category-breakdown',
                                            figure={},
                                            config={'displayModeBar': False},
                                            style={'height': '600px', 'width': '100%'},
                                        ),
                                        w='100%',
                                    ),
                                ],
                                gap='sm',
                            ),
                            p='md',
                            radius='lg',
                            withBorder=True,
                            shadow='md',
                            bg='white',
                        ),
                        span=12,
                    ),
                ],
                gutter='lg',
                mt='lg',
            ),
        ],
        size='100%',
        px='md',
        py='lg',
    )


@dash.callback(
    Output('sales-drilldown-hourly-pattern', 'figure'),
    Output('sales-drilldown-category-breakdown', 'figure'),
    Input('sales-global-query-context', 'data'),
    prevent_initial_call=False,
)
def update_drilldown_charts(query_context):
    if not query_context:
        raise PreventUpdate

    start_date = date.fromisoformat(query_context['start_date'])
    end_date = date.fromisoformat(query_context['end_date'])

    hourly_fig = build_hourly_heatmap_chart(start_date, end_date)
    sankey_fig = build_category_sankey_chart(start_date, end_date)

    return hourly_fig, sankey_fig
