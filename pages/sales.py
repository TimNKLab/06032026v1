import dash
from dash import html

dash.register_page(
    __name__,
    path='/sales',
    name='Sales',
    title='Sales Performance'
)

def layout():
    return html.Div([
        html.H2('Sales Performance'),
        html.P('Placeholder sales dashboard. Replace with revenue trends, top products, and regional split insights.')
    ])