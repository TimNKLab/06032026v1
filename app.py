import dash
from dash import Dash, html, dcc

app = Dash(__name__, use_pages=True)

app.layout = html.Div([
    html.H1('New Khatulistiwa Sales KPI Dashboard'),
    dcc.Link('Home', href='/'),
    html.Br(),
    dcc.Link('Analytics', href='/analytics'),
    html.Br(),
    dash.page_container
])

if __name__ == '__main__':
    app.run(debug=True)