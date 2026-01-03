"""Thread-safe Odoo connection pooling."""
import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional

from etl.config import CONNECTION_TIMEOUT
from odoorpc_connector import get_odoo_connection


@dataclass
class ConnectionState:
    connection: Optional[Any] = None
    last_used: Optional[float] = None


_thread_local = threading.local()


def _get_connection_state() -> ConnectionState:
    """Get thread-local connection state."""
    if not hasattr(_thread_local, 'conn_state'):
        _thread_local.conn_state = ConnectionState()
    return _thread_local.conn_state


@contextmanager
def get_pooled_odoo_connection():
    """Thread-safe connection pooling for Odoo."""
    state = _get_connection_state()
    current_time = time.time()

    # Reuse if valid
    if (state.connection is not None and
        state.last_used is not None and
        current_time - state.last_used < CONNECTION_TIMEOUT):
        try:
            # Verify connection is still alive
            state.connection.env['res.users'].search([], limit=1)
            state.last_used = current_time
            yield state.connection
            return
        except Exception:
            state.connection = None

    # Create new connection
    state.connection = get_odoo_connection()
    state.last_used = current_time

    if state.connection is None:
        raise Exception("Failed to establish Odoo connection")

    yield state.connection
