import os
import json
import sqlite3
import pandas as pd
from pathlib import Path
from etl.config import DATA_LAKE_ROOT

# Resolve paths based on the central ETL Config
DATA_LAKE_PATH = Path(DATA_LAKE_ROOT).resolve()
ADMIN_DIR = DATA_LAKE_PATH / "admin"
QUEUE_DB = ADMIN_DIR / "etl_queue.sqlite"
STATE_FILE = ADMIN_DIR / "etl_state.json"
LOG_DIR = ADMIN_DIR / "logs"

def init_admin_directories():
    """Ensure all admin directories and queue databases exist."""
    ADMIN_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    with sqlite3.connect(QUEUE_DB) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                target_date TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

def get_dir_size(path='.'):
    total = 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += get_dir_size(entry.path)
    except Exception:
        pass
    return total

def format_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def scan_data_lake():
    """Scan raw, clean, and star-schema partitions."""
    if not DATA_LAKE_PATH.exists():
        return []

    data = []
    for zone in ['raw', 'clean', 'star-schema']:
        zone_path = DATA_LAKE_PATH / zone
        if zone_path.exists():
            for folder in zone_path.iterdir():
                if folder.is_dir():
                    size = get_dir_size(folder)
                    file_count = sum(1 for _ in folder.rglob('*.parquet'))
                    data.append({
                        "Zone": zone,
                        "Dataset": folder.name,
                        "Files": file_count,
                        "Size": format_size(size),
                        "Raw Size (Bytes)": size
                    })
    return data

def enqueue_task(task_type: str, target_date_str: str):
    """Write a new job to the SQLite queue."""
    with sqlite3.connect(QUEUE_DB) as conn:
        conn.execute(
            "INSERT INTO queue (task_type, target_date) VALUES (?, ?)", 
            (task_type, target_date_str)
        )

def get_queue_status():
    """Fetch the latest 10 items in the queue."""
    try:
        with sqlite3.connect(QUEUE_DB) as conn:
            return pd.read_sql_query("SELECT * FROM queue ORDER BY created_at DESC LIMIT 10", conn)
    except Exception:
        return pd.DataFrame()

def get_etl_state():
    """Read the latest JSON state written by the scheduler."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def get_log_files():
    """Return a list of available log files."""
    if not LOG_DIR.exists():
        return []
    logs = list(LOG_DIR.glob("*.log"))
    logs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return logs
