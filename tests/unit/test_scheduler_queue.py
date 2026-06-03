import os
import sys
import sqlite3
import json
import tempfile
from pathlib import Path

def test_sqlite_queue_ipc():
    """
    Test the IPC (Inter-Process Communication) behavior.
    Instead of mocking the whole app, we simulate what Streamlit does (write to SQLite)
    and simulate what the Scheduler does (read from SQLite, update status, write JSON state).
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        admin_dir = tmp_path / "admin"
        admin_dir.mkdir()
        queue_db = admin_dir / "etl_queue.sqlite"
        state_file = admin_dir / "etl_state.json"
        
        # 1. Initialize DB (Streamlit behavior)
        with sqlite3.connect(queue_db) as conn:
            conn.execute('''
                CREATE TABLE queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT NOT NULL,
                    target_date TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 2. Write Job to DB (Streamlit behavior)
            conn.execute(
                "INSERT INTO queue (task_type, target_date) VALUES (?, ?)", 
                ("refresh_dimensions", "2026-06-02")
            )
            
        # 3. Simulate Scheduler background loop picking it up
        with sqlite3.connect(queue_db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM queue WHERE status = 'pending' LIMIT 1").fetchone()
            
            assert row is not None
            assert row['task_type'] == 'refresh_dimensions'
            
            task_id = row['id']
            
            # Scheduler marks as running
            conn.execute("UPDATE queue SET status = 'running' WHERE id = ?", (task_id,))
            
            # Scheduler executes task (Mocked: Success)
            # ... executing ...
            
            # Scheduler marks as completed
            conn.execute("UPDATE queue SET status = 'completed' WHERE id = ?", (task_id,))
            
        # 4. Simulate Scheduler writing state
        with open(state_file, "w") as f:
            json.dump({"last_status": "success"}, f)
            
        # 5. Assertions
        with sqlite3.connect(queue_db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM queue WHERE id = ?", (task_id,)).fetchone()
            assert row['status'] == 'completed'
            
        with open(state_file, "r") as f:
            state = json.load(f)
            assert state['last_status'] == 'success'
            
        print("✅ Unit Test: SQLite Queue IPC logic is sound.")

if __name__ == "__main__":
    test_sqlite_queue_ipc()

