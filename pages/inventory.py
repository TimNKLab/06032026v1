import dash
from dash import html


dash.register_page(
    __name__,
    path='/inventory',
    name='Inventory Management',
    title='Inventory Health'
)

layout = html.Div([
    html.H2('Inventory Health'),
    html.P('Placeholder inventory dashboard. Replace with stock levels, sell-through, and aging analysis.')
])
