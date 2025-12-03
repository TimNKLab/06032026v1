import dash
from dash import html


dash.register_page(
    __name__,
    path='/customer',
    name='Customer Experience',
    title='Customer Experience'
)

layout = html.Div([
    html.H2('Customer Experience'),
    html.P('Placeholder NKLab workspace. Replace with prototypes, sandbox widgets, and experimentation notes.')
])
