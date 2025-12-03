import odoorpc
from dotenv import load_dotenv
import os
from functools import wraps
import time

# Ensure environment variables are loaded immediately upon import
load_dotenv()

class OdooConnectionManager:
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_connection(self):
        if self._connection is None:
            self._connection = _create_connection()
        return self._connection

def retry_odoo(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
            return None
        return wrapper
    return decorator

def _create_connection():
    host = os.environ.get('ODOO_HOST')
    port = int(os.environ.get('ODOO_PORT', 443))
    protocol = os.environ.get('ODOO_PROTOCOL', 'jsonrpc+ssl')
    db = os.environ.get('ODOO_DB') or os.environ.get('ODOO_DATABASE')
    username = os.environ.get('ODOO_USERNAME') or os.environ.get('ODOO_USER')
    password = os.environ.get('ODOO_API_KEY') or os.environ.get('ODOO_PASSWORD')
    
    if not all([host, db, username, password]):
        print("Warning: Odoo connection environment variables (host, db, username, password/api_key) are missing.")
        return None

    try:
        odoo = odoorpc.ODOO(host, protocol=protocol, port=port)
        odoo.login(db, username, password)
        return odoo
    except Exception as e:
        print(f"Error connecting to Odoo: {e}")
        return None

def get_odoo_connection():
    """
    Gets a reusable Odoo connection using the connection manager.
    Returns a logged-in odoorpc.ODOO object or None if configuration is missing or connection fails.
    """
    return OdooConnectionManager().get_connection()