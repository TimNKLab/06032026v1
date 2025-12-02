import dash
from dash import html, dash_table
from odoorpc_connector import get_odoo_data

dash.register_page(__name__)

def layout():
    # Fetch data from Odoo
    sales_data = get_odoo_data()

    if not sales_data:
        return html.Div([
            html.H2('Sales Analytics'),
            html.P('Could not fetch sales data from Odoo.'),
        ])
        
    return html.Div([
        html.H2('Sales Analytics'),
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in sales_data[0].keys()],
            data=sales_data,
        )
    ])