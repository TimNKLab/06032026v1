import dash
from dash import html

dash.register_page(__name__, path='/')

layout = html.Div([
    html.H2('Home Page'),
    html.P('This is the home page of the New Khatulistiwa Sales KPI Dashboard.')
])