"""Reusable data freshness badge component.

Usage in any page::

    from components.freshness_badge import create_freshness_badge, register_freshness_callback

    # In layout, add:
    create_freshness_badge('my-page')

    # After layout definition, register callback:
    register_freshness_callback('my-page')

The badge auto-refreshes every 60 seconds via ``dcc.Interval``.
"""

import dash
from dash import dcc, Output, Input
import dash_mantine_components as dmc

from services.versioned_cache import get_data_freshness


def create_freshness_badge(prefix: str):
    """Return a dmc.Group containing a freshness badge + auto-refresh interval.

    Args:
        prefix: Unique prefix for element IDs (e.g. 'overview', 'sales')

    Returns:
        dmc.Group with Badge + hidden dcc.Interval
    """
    return dmc.Group([
        dmc.Badge(
            'Loading…',
            id=f'{prefix}-freshness-badge',
            color='gray',
            variant='light',
            size='sm',
            radius='md',
        ),
        # Auto-refresh every 60 seconds
        dcc.Interval(
            id=f'{prefix}-freshness-interval',
            interval=60_000,  # ms
            n_intervals=0,
        ),
    ], gap='xs', align='center', style={'display': 'inline-flex'})


def register_freshness_callback(prefix: str):
    """Register a callback that updates the freshness badge.

    Must be called at module level (not inside layout function).

    Args:
        prefix: Same prefix used in ``create_freshness_badge``
    """
    @dash.callback(
        Output(f'{prefix}-freshness-badge', 'children'),
        Output(f'{prefix}-freshness-badge', 'color'),
        Input(f'{prefix}-freshness-interval', 'n_intervals'),
    )
    def _update_freshness(_n):
        info = get_data_freshness()
        return info['label'], info['color']
