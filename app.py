import dash
from dash import Dash, dcc, html, Output, Input, State
import dash_mantine_components as dmc

# Track B: Dash 2.14.2 + DMC 2.4.0 compatibility
# Enforce React 18 for DMC 2.x
try:
    from dash._dash_renderer import _set_react_version
    _set_react_version("18.2.0")
except (ImportError, AttributeError):
    pass

# Runtime version guard
def _check_versions():
    import dash_mantine_components as dmc_local
    dash_version = dash.__version__
    try:
        dmc_version = dmc_local.__version__
    except Exception:
        dmc_version = getattr(dmc_local, "__version__", "unknown")
    if not (dash_version.startswith("2.14.") and dmc_version.startswith("2.4.")):
        raise RuntimeError(
            f"Version mismatch. Expected Track B: Dash 2.14.x + DMC 2.4.x. "
            f"Found Dash {dash_version}, DMC {dmc_version}. "
            f"Update requirements.txt and reinstall."
        )
_check_versions()

NAV_LINKS = [
    ("Overview", "/"),
    ("Sales", "/sales"),
    ("Inventory Management", "/inventory"),
    ("Operational Efficiency", "/operational"),
    ("Customer Experience", "/customer"),
]

app = Dash(__name__, use_pages=True, external_stylesheets=["/assets/custom.css"], suppress_callback_exceptions=True)


def sidebar_links():
    return [
        dmc.NavLink(label=label, href=href, variant="subtle", fw=500)
        for label, href in NAV_LINKS
    ]


# Expose Flask server for Gunicorn
server = app.server

app.layout = dmc.MantineProvider(
    theme={
        "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        "headings": {
            "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
            "fontWeight": "600"
        }
    },
    children=dmc.AppShell(
        id="appshell",
        padding="sm",
        navbar={
            "width": 240,
            "breakpoint": "sm",
            "collapsed": {"mobile": True, "desktop": False},
        },
        header={
            "height": 60,
            "collapseOffset": 60,
        },
        children=[
            # Hamburger menu in header
            dmc.AppShellHeader(
                children=[
                    dmc.Group(
                        children=[
                            dmc.Burger(
                                id="nav-burger",
                                opened=False,
                                size="sm",
                                visibleFrom="base",
                                hiddenFrom="sm",
                            ),
                            dmc.Title(
                                "New Khatulistiwa KPI", 
                                order=4, 
                                ml="md",
                                hiddenFrom="base",
                                visibleFrom="sm"
                            ),
                        ],
                        h="100%",
                        px="md",
                        align="center"
                    )
                ]
            ),
            # Collapsible navbar
            dmc.AppShellNavbar(
                id="app-navbar",
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


# Callback to toggle navbar
@app.callback(
    Output("appshell", "navbar"),
    Input("nav-burger", "opened"),
    State("appshell", "navbar"),
    prevent_initial_call=False,
)
def toggle_navbar(opened, navbar):
    # Control the collapsed state of the navbar
    navbar["collapsed"] = {"mobile": not opened, "desktop": False}
    print(f"Toggle navbar: opened={opened} -> collapsed.mobile={not opened}")  # Debug log
    return navbar


server = app.server

if __name__ == '__main__':
    app.run(debug=True)