import dash
from dash import Dash, dcc, html, Output, Input, State
import dash_mantine_components as dmc
from services.cache import init_cache

import importlib
import pkgutil

from datetime import date

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
    ("Sales Drilldowns", "/sales-drilldown"),
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

init_cache(server)

# Pre-create DuckDB views and materialized views in background thread
import threading
import time as time_module

def _precreate_views():
    """Pre-create DuckDB views and materialized views for instant queries."""
    time_module.sleep(2)  # Wait for app to initialize
    try:
        from services.duckdb_connector import ensure_duckdb_view_groups, ensure_materialized_views
        print("[STARTUP] Pre-creating DuckDB views...")
        start = time_module.time()
        # Create fast aggregate views first (for Sales page)
        ensure_duckdb_view_groups({"sales_agg", "overview", "dims"})
        elapsed = time_module.time() - start
        print(f"[STARTUP] DuckDB views ready in {elapsed:.1f}s")
        
        # Load materialized views for ultra-fast queries (< 50ms)
        print("[STARTUP] Loading materialized views into memory...")
        start = time_module.time()
        ensure_materialized_views({
            "mv_sales_daily",
            "mv_sales_by_product", 
            "mv_sales_by_principal",
            "mv_profit_daily"
        })
        elapsed = time_module.time() - start
        print(f"[STARTUP] Materialized views ready in {elapsed:.1f}s")
    except Exception as e:
        print(f"[STARTUP] Failed to pre-create views: {e}")

view_thread = threading.Thread(target=_precreate_views, daemon=True)
view_thread.start()

app.layout = dmc.MantineProvider(
    theme={
        # Cohere Design System Theme Configuration
        # Typography: Space Grotesk (display) + Inter (body)
        "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        "headings": {
            "fontFamily": "'Space Grotesk', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
            "fontWeight": "500",
            "sizes": {
                "h1": {"fontSize": "48px", "lineHeight": "1.1", "letterSpacing": "-0.5px"},
                "h2": {"fontSize": "32px", "lineHeight": "1.2", "letterSpacing": "-0.3px"},
                "h3": {"fontSize": "24px", "lineHeight": "1.3"},
                "h4": {"fontSize": "18px", "lineHeight": "1.3"},
            }
        },
        # Border Radius: 22px signature Cohere roundness
        "radius": {
            "xs": "4px",
            "sm": "8px",
            "md": "12px",
            "lg": "16px",
            "xl": "22px",
        },
        # Colors: Cohere Enterprise Palette
        "colors": {
            # Primary: Interaction Blue (#1863dc)
            "blue": [
                "#f0f4ff",  # 0 - lightest
                "#d6e3ff",  # 1
                "#adc8ff",  # 2
                "#84a9f9",  # 3
                "#5a8aed",  # 4
                "#1863dc",  # 5 - primary interaction blue
                "#0f50b8",  # 6
                "#083d94",  # 7
                "#042a70",  # 8
                "#011a4d",  # 9 - darkest
            ],
            # Grays: Cool-toned for enterprise feel
            "gray": [
                "#fafafa",  # 0 - snow
                "#f2f2f2",  # 1 - lightest gray (card borders)
                "#e5e7eb",  # 2 - border light
                "#d9d9dd",  # 3 - border cool
                "#c5c5c9",  # 4
                "#93939f",  # 5 - muted slate
                "#6e6e78",  # 6
                "#4a4a52",  # 7
                "#2d2d33",  # 8 - deep dark
                "#17171c",  # 9 - near black
            ],
            # Dark/Black for dark buttons
            "dark": [
                "#f8f9fa",
                "#e9ecef",
                "#dee2e6",
                "#ced4da",
                "#adb5bd",
                "#6c757d",
                "#495057",
                "#212121",  # 7 - near black
                "#17171c",  # 8 - deep dark
                "#000000",  # 9 - black
            ],
        },
        "primaryColor": "blue",
        "primaryShade": {"light": 5, "dark": 6},
        # Shadow: Minimal (Cohere is nearly shadow-free)
        "shadows": {
            "xs": "none",
            "sm": "0 1px 3px rgba(0, 0, 0, 0.04)",
            "md": "0 4px 12px rgba(0, 0, 0, 0.05)",
            "lg": "0 8px 24px rgba(0, 0, 0, 0.06)",
            "xl": "0 16px 48px rgba(0, 0, 0, 0.08)",
        },
        "defaultRadius": "xl",  # 22px for cards
        # Spacing scale
        "spacing": {
            "xs": "4px",
            "sm": "8px",
            "md": "16px",
            "lg": "24px",
            "xl": "32px",
        },
        # Focus ring
        "focusRing": "always",
        "focusRingStyles": {
            "styles": {"outline": "2px solid #1863dc", "outlineOffset": "2px"}
        },
    },
    children=dmc.AppShell(
        id="appshell",
        padding="sm",
        header={
            "height": 80,
        },
        children=[
            dcc.Location(id='app-location', refresh=False),
            dcc.Store(
                id='sales-global-query-context',
                storage_type='session',
                data={
                    'start_date': date.today().replace(day=1).isoformat(),
                    'end_date': date.today().isoformat(),
                    'period': 'daily',
                    'source': 'default_mtd',
                },
            ),
            dcc.Store(id='overview-view-state', storage_type='session'),
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


def _build_validation_layout():
    try:
        import pages  # type: ignore
        for mod in pkgutil.iter_modules(pages.__path__):
            if mod.name.startswith("__"):
                continue
            importlib.import_module(f"pages.{mod.name}")
    except Exception:
        pass

    page_layouts = []
    for page in dash.page_registry.values():
        layout = page.get("layout")
        if layout is None:
            continue
        try:
            page_layouts.append(layout() if callable(layout) else layout)
        except Exception:
            continue
    return html.Div([app.layout, *page_layouts])


app.validation_layout = _build_validation_layout()


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