import dash
from dash import html


dash.register_page(
    __name__,
    path='/operational',
    name='Operational Efficiency',
    title='Operational Efficiency'
)

layout = html.Div([
    html.H2('Operational Efficiency'),
    html.P('Placeholder operational dashboard. Replace with uplift metrics, campaign ROI, and redemption funnels.')
])
