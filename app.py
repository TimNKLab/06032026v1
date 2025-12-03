import dash
from dash import Dash
import dash_mantine_components as dmc

# Required for Dash 2.x compatibility with Mantine (no-op on newer versions)
if hasattr(dash, "_dash_renderer"):
    dash._dash_renderer._set_react_version("18.2.0")

NAV_LINKS = [
    ("Overview", "/"),
    ("Sales", "/sales"),
    ("Inventory Management", "/inventory"),
    ("Operational Efficiency", "/operational"),
    ("Customer Experience", "/customer"),
]

app = Dash(__name__, use_pages=True, external_stylesheets=dmc.styles.ALL)


def sidebar_links():
    return [
        dmc.NavLink(label=label, href=href, variant="subtle", fw=500)
        for label, href in NAV_LINKS
    ]


app.layout = dmc.MantineProvider(
    dmc.AppShell(
        padding="md",
        navbar={
            "width": 240,
            "breakpoint": "sm",
        },
        children=[
            dmc.AppShellNavbar(
                p="md",
                children=[
                    dmc.Stack(
                        [
                            dmc.Title("New Khatulistiwa KPI", order=3),
                            dmc.Divider(),
                            *sidebar_links(),
                        ],
                        gap="sm",
                    )
                ],
            ),
            dmc.AppShellMain(
                dmc.Container(dash.page_container, size="responsive", px="md", py="lg"),
            )
        ],
    ),
)


if __name__ == '__main__':
    app.run(debug=True)