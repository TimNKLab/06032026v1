"""
Reusable loading modal component for NKDash.

Provides two patterns:
1. Full modal with status, progress, error state, and cancel button
2. Simple overlay spinner for quick operations

Usage:
    from components import create_loading_modal

    layout = dmc.Container([
        create_loading_modal(
            modal_id='my-loading-modal',
            status_id='my-loading-status',
            error_id='my-loading-error',
            cancel_id='my-cancel',
            title="Loading Data",
            show_cancel=False,  # For quick synchronous operations
        ),
        # ... rest of layout
    ])
"""
import dash_mantine_components as dmc
from dash import html


def create_loading_modal(
    modal_id: str,
    status_id: str,
    error_id: str,
    cancel_id: str,
    title: str = "Loading Data",
    show_cancel: bool = True,
    show_progress: bool = True,
):
    """
    Reusable loading modal with status, progress, error state, and cancel button.
    
    The modal has three states:
    1. Loading: Shows spinner + status text
    2. Error: Shows error message + retry button
    3. Complete: Modal closes automatically
    
    Args:
        modal_id: Unique ID for the modal component
        status_id: ID for status text element
        error_id: ID for error message element (hidden when no error)
        cancel_id: ID for cancel button
        title: Modal title text
        show_cancel: Whether to show cancel button (for long operations)
        show_progress: Whether to show progress bar (vs indeterminate spinner)
    
    Returns:
        dmc.Modal component ready to be added to layout
    
    Example callback pattern:
        @dash.callback(
            Output('chart-id', 'figure'),
            Output('my-loading-modal', 'opened'),
            Output('my-loading-status', 'children'),
            Output('my-loading-error', 'style'),
            Input('apply-button', 'n_clicks'),
            State('date-from', 'value'),
            State('date-until', 'value'),
            prevent_initial_call=True,
        )
        def load_data(n_clicks, date_from, date_until):
            if n_clicks is None:
                raise PreventUpdate
            
            try:
                # Open modal at start of callback
                # Query data (synchronous - user sees spinner)
                data = query_data(date_from, date_until)
                figure = build_figure(data)
                
                # Success: close modal, clear error
                return (
                    figure,
                    False,  # Close modal
                    "Complete",
                    {"display": "none"},  # Hide error
                )
            except Exception as e:
                # Error: keep modal open, show error
                return (
                    dash.no_update,
                    True,  # Keep modal open
                    f"Error: {str(e)}",
                    {"display": "block"},  # Show error
                )
    """
    content = [
        dmc.Text("Initializing...", id=status_id, size="sm", c="dimmed", ta="center"),
    ]
    
    if show_progress:
        # Use indeterminate progress for operations without measurable progress
        # Animated striped bar at 0% looks like indeterminate
        content.append(
            dmc.Progress(
                id=f"{modal_id}-progress",
                value=0,
                striped=True,
                animated=True,
                mt="sm",
            )
        )
    else:
        # Simple spinner for quick operations
        content.append(
            dmc.Center(
                dmc.Loader(variant="dots", size="lg"),
                mt="lg",
            )
        )
    
    # Error message (hidden by default)
    content.append(
        dmc.Box(
            id=error_id,
            style={"display": "none"},
            children=[
                dmc.Alert(
                    "An error occurred while loading data.",
                    title="Error",
                    color="red",
                    variant="filled",
                    mt="sm",
                ),
                dmc.Button(
                    "Retry",
                    id=f"{modal_id}-retry",
                    variant="light",
                    color="blue",
                    mt="sm",
                    fullWidth=True,
                ),
            ],
        )
    )
    
    # Cancel button for long-running operations
    if show_cancel:
        content.append(
            dmc.Button(
                "Cancel",
                id=cancel_id,
                variant="subtle",
                color="gray",
                mt="md",
                fullWidth=True,
            )
        )
    
    return dmc.Modal(
        id=modal_id,
        opened=False,
        title=title,
        size="md",
        centered=True,
        withCloseButton=False,  # Prevent accidental close during operation
        closeOnClickOutside=False,  # Prevent accidental close
        children=[
            dmc.Box(
                content,
                p="md",
            )
        ],
    )


def create_simple_spinner_overlay(spinner_id: str, container_id: str, children=None):
    """
    Simple overlay spinner for quick operations (<2s).
    Use this instead of full modal for simple chart updates.
    
    This wraps content in a relative Box with LoadingOverlay.
    The LoadingOverlay must have children to overlay over.
    
    Args:
        spinner_id: ID for the LoadingOverlay component
        container_id: ID for the container Box
        children: Content to overlay over (will be wrapped)
    
    Returns:
        dmc.Box with LoadingOverlay and children
    
    Example:
        layout = dmc.Container([
            create_simple_spinner_overlay(
                spinner_id='chart-spinner',
                container_id='chart-container',
                children=[
                    dcc.Graph(id='my-chart', figure={}),
                ]
            ),
        ])
        
        @dash.callback(
            Output('my-chart', 'figure'),
            Output('chart-spinner', 'visible'),
            Input('apply-button', 'n_clicks'),
            prevent_initial_call=True,
        )
        def update_chart(n_clicks):
            # Spinner shows automatically while callback runs
            figure = build_figure()
            return figure, False  # Hide spinner when done
    """
    return dmc.Box(
        id=container_id,
        pos="relative",
        children=[
            dmc.LoadingOverlay(
                id=spinner_id,
                visible=False,
                overlayProps={"radius": "sm", "blur": 2},
                zIndex=1000,
            ),
            *(children or []),
        ],
    )
