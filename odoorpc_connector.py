import odoorpc
from dotenv import load_dotenv
import os

# Ensure environment variables are loaded immediately upon import
load_dotenv()

def get_odoo_connection():
    """
    Loads Odoo credentials from environment variables and establishes a connection.
    Returns a logged-in odoorpc.ODOO object or None if configuration is missing or connection fails.
    """
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

def get_odoo_data():
    """
    Connects to Odoo and fetches sales data.
    This is a synchronous and blocking call.
    """
    odoo = get_odoo_connection()
    
    if odoo is None:
        return []
        
    # Example: Fetch sales order data
    if 'sale.order' in odoo.env:
        SaleOrder = odoo.env['sale.order']
        # Fetch up to 10 sales orders for initial setup testing
        sale_orders = SaleOrder.search_read([], ['name', 'amount_total'], limit=10)
        return sale_orders
    else:
        return []

if __name__ == '__main__':
    # For testing the connection
    data = get_odoo_data()
    print(data)