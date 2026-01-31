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
    ("Customer Experience", "/customer"),
    ("Data Sync", "/operational")
]

app = Dash(__name__, use_pages=True, external_stylesheets=["/assets/custom.css"], suppress_callback_exceptions=True)


def header_nav_links():
    return [
        dmc.Anchor(
            dmc.Text(label, size="sm", c="gray.7"),
            href=href,
            mx="sm",
            underline=False
        )
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
        header={
            "height": 80,
        },
        children=[
            # Header with title and navigation links
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
                                order=3, 
                                c="blue.6"
                            ),
                            dmc.Group(
                                id="nav-links",
                                children=header_nav_links(),
                                gap="lg",
                                ml="xl",
                                visibleFrom="sm",
                                hiddenFrom="base"
                            ),
                        ],
                        h="100%",
                        px="md",
                        align="center",
                        justify="space-between"
                    ),
                    # Mobile navigation drawer
                    dmc.Drawer(
                        id="mobile-nav-drawer",
                        opened=False,
                        position="right",
                        size="xs",
                        padding="md",
                        title="Navigation",
                        children=[
                            dmc.Stack(
                                children=[
                                    dmc.Anchor(
                                        dmc.Text(label, size="md", c="gray.7"),
                                        href=href,
                                        py="xs"
                                    )
                                    for label, href in NAV_LINKS
                                ],
                                gap="xs"
                            )
                        ]
                    )
                ]
            ),
            dmc.AppShellMain(
                dmc.Container(dash.page_container, size="responsive", px="md", py="lg"),
            )
        ],
    ),
)


# Callback to toggle mobile navigation drawer
@app.callback(
    Output("mobile-nav-drawer", "opened"),
    Input("nav-burger", "opened"),
    prevent_initial_call=True,
)
def toggle_mobile_drawer(opened):
    return opened


server = app.server

if __name__ == '__main__':
    app.run(debug=True)