import time
import schedule
import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
import sys
import os

# Add parent directory to path so it can find `etl` module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.config import DATA_LAKE_ROOT
from etl.tasks import get_task
from etl.extract import pos, invoices, inventory_moves, stock_quants

# Set up logging to the data lake
DATA_LAKE_PATH = Path(DATA_LAKE_ROOT).resolve()
ADMIN_DIR = DATA_LAKE_PATH / "admin"
LOG_DIR = ADMIN_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"scheduler_{datetime.now().strftime('%Y%m')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

QUEUE_DB = ADMIN_DIR / "etl_queue.sqlite"
STATE_FILE = ADMIN_DIR / "etl_state.json"

def write_state(status: str):
    """Write the current state to JSON so Streamlit can read it."""
    state = {
        "last_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_status": status
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def run_full_daily_pipeline(target_date: str):
    """
    Executes the entire ETL chain from Raw to Aggregates natively in Python.
    Replaces the old Celery chains in `etl/pipelines/daily.py`.
    """
    logger.info(f"🚀 STARTING FULL PIPELINE FOR: {target_date}")
    
    # 1. Extract & Load POS
    logger.info("-> Processing POS Data...")
    raw_pos = pos.extract_pos_order_lines(target_date)
    get_task('save_raw_data')(raw_pos)
    get_task('clean_pos_data')(raw_pos, target_date)
    get_task('update_star_schema')(target_date)
    
    # 2. Extract & Load Sales Invoices
    logger.info("-> Processing Sales Invoices...")
    raw_inv_out = invoices.extract_sales_invoice_lines(target_date)
    get_task('save_raw_sales_invoice_lines')(raw_inv_out)
    get_task('clean_sales_invoice_lines')(raw_inv_out, target_date)
    get_task('update_invoice_sales_star_schema')(target_date)
    
    # 3. Extract & Load Dimensions (Products, Locations, etc)
    logger.info("-> Refreshing Dimensions...")
    get_task('refresh_dimensions')(['products', 'locations', 'uoms', 'partners', 'users', 'companies', 'lots'])
    
    # 4. Extract & Load Inventory
    logger.info("-> Processing Inventory...")
    raw_moves = inventory_moves.extract_inventory_moves(target_date)
    get_task('save_raw_inventory_moves')(raw_moves)
    get_task('clean_inventory_moves')(raw_moves, target_date)
    get_task('update_inventory_moves_star_schema')(target_date)
    
    # 5. Extract & Load Quants (Snapshot)
    logger.info("-> Processing Stock Quants...")
    raw_quants = stock_quants.extract_stock_quants(target_date)
    get_task('save_raw_stock_quants')(raw_quants)
    get_task('clean_stock_quants')(raw_quants, target_date)
    get_task('update_stock_quants_star_schema')(target_date)
    
    # 6. Aggregates & Profit (The "Ujung")
    logger.info("-> Materializing Profit & Aggregates...")
    get_task('update_product_cost_events')(target_date)
    get_task('update_product_cost_latest_daily')(target_date)
    get_task('update_sales_lines_profit')(target_date)
    get_task('update_profit_aggregates')(target_date)
    get_task('update_sales_aggregates')(target_date)
    
    logger.info(f"✅ FULL PIPELINE COMPLETED FOR: {target_date}")

def process_queue():
    """Polls the SQLite database for pending tasks and executes them."""
    if not QUEUE_DB.exists():
        return
        
    try:
        with sqlite3.connect(QUEUE_DB) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
            row = cursor.fetchone()
            
            if row:
                task_id = row['id']
                task_type = row['task_type']
                target_date = row['target_date']
                
                # Mark as running
                conn.execute("UPDATE queue SET status = 'running' WHERE id = ?", (task_id,))
                conn.commit()
                write_state("running")
                
                logger.info(f"Picked up task {task_id}: {task_type} for {target_date}")
                
                try:
                    if task_type == 'daily_pipeline':
                        run_full_daily_pipeline(target_date)
                    elif task_type == 'refresh_dimensions':
                        get_task('refresh_dimensions')(['products', 'locations', 'uoms', 'partners', 'users', 'companies', 'lots'])
                    else:
                        logger.warning(f"Unknown task type: {task_type}")
                        
                    # Mark as completed
                    conn.execute("UPDATE queue SET status = 'completed' WHERE id = ?", (task_id,))
                    conn.commit()
                    write_state("success")
                    
                except Exception as e:
                    logger.error(f"Task {task_id} failed: {e}")
                    conn.execute("UPDATE queue SET status = 'failed' WHERE id = ?", (task_id,))
                    conn.commit()
                    write_state("failed")
                    
    except Exception as e:
        logger.error(f"Queue processing error: {e}")

# =====================================================================
# Main Loop (The Daemon)
# =====================================================================
if __name__ == "__main__":
    logger.info("ETL Scheduler Daemon Started.")
    
    # 1. Scheduled Background Jobs (Every night at 02:00)
    schedule.every().day.at("02:00").do(
        lambda: run_full_daily_pipeline(datetime.now().strftime("%Y-%m-%d"))
    )
    
    # 2. Polling Loop (Checks Streamlit UI Queue every 10 seconds)
    while True:
        process_queue()
        schedule.run_pending()
        time.sleep(10)
