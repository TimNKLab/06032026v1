"""
Example: Export AGGrid to XLSX (Excel) in Dash

AGGrid's built-in csvExportParams only supports CSV.
For XLSX, use a custom callback that:
1. Reads the grid's rowData via State
2. Converts to pandas DataFrame
3. Writes to an in-memory BytesIO buffer with to_excel
4. Returns dcc.send_data_document for browser download

This pattern works for any AGGrid table.
"""

import dash
from dash import dcc, Input, Output, State
import dash_ag_grid as dag
import pandas as pd
from io import BytesIO

# Sample data
SAMPLE_DATA = [
    {"product_name": "SKU001", "on_hand_qty": 120, "reserved_qty": 5},
    {"product_name": "SKU002", "on_hand_qty": 85, "reserved_qty": 12},
]

COLUMN_DEFS = [
    {"field": "product_name", "headerName": "SKU"},
    {"field": "on_hand_qty", "headerName": "On-hand", "type": "numericColumn"},
    {"field": "reserved_qty", "headerName": "Reserved", "type": "numericColumn"},
]

@dash.callback(
    Output("inventory-stock-table-xlsx", "data", allow_duplicate=True),
    Input("inventory-stock-export-xlsx", "n_clicks"),
    State("inventory-stock-table", "rowData"),
    prevent_initial_call=True,
)
def export_stock_xlsx(n_clicks, row_data):
    if not n_clicks or not row_data:
        raise dash.exceptions.PreventUpdate

    # Convert rowData to DataFrame
    df = pd.DataFrame(row_data)

    # Write to in-memory Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Stock Levels")
    output.seek(0)

    return dcc.send_data_document(
        output.read(),
        filename="stock_levels.xlsx",
        type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# Example layout snippet you can add to any page:
"""
dmc.Button(
    "Export Excel",
    id="inventory-stock-export-xlsx",
    variant="light",
    size="xs",
),
dcc.Download(id="inventory-stock-table-xlsx"),
"""

# Notes:
# - Add dcc.Download somewhere in your layout (can be hidden)
# - Use a unique Download id per table to avoid conflicts
# - You can customize sheet names, add multiple sheets, or format cells in ExcelWriter
# - For large datasets, consider chunking or server-side generation
"""
