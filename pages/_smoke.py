import dash
from dash import dcc
import dash_mantine_components as dmc

dash.register_page(__name__, path="/_smoke", name="Smoke", title="Smoke Test")

layout = dmc.Container(
    dmc.Stack(
        [
            dmc.Title("Track B Smoke Test", order=2),
            dmc.Text("If you see this, Dash + DMC 2.x is working."),
            dmc.Button("Test Button", variant="filled"),
        ],
        gap="md",
    ),
    size="sm",
    py="md"
)
