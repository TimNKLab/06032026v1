# etl_runner.py
import sys
import os
import subprocess
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from pathlib import Path
import json
import threading
import queue

# Ensure the app can import etl_tasks
sys.path.append(str(Path(__file__).parent))
from etl_tasks import (
    date_range_etl_pipeline,
    refresh_dimensions_incremental,
    ETLStatus
)

class ETLLogger:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self.text_widget.config(state=tk.DISABLED)
        
    def log(self, message, level="INFO"):
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")
        self.text_widget.see(tk.END)
        self.text_widget.config(state=tk.DISABLED)
        self.text_widget.update_idletasks()

class ETLRunnerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL Runner")
        self.root.geometry("800x600")
        self.setup_ui()
        self.log_queue = queue.Queue()
        self.after_id = None
        
    def setup_ui(self):
        # Date range selection
        ttk.Label(self.root, text="Start Date:").grid(row=0, column=0, padx=5, pady=5)
        self.start_date = ttk.Entry(self.root, width=12)
        self.start_date.grid(row=0, column=1, padx=5, pady=5)
        self.start_date.insert(0, (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
        
        ttk.Label(self.root, text="End Date:").grid(row=0, column=2, padx=5, pady=5)
        self.end_date = ttk.Entry(self.root, width=12)
        self.end_date.grid(row=0, column=3, padx=5, pady=5)
        self.end_date.insert(0, datetime.now().strftime('%Y-%m-%d'))
        
        # Buttons
        ttk.Button(self.root, text="Check Data", command=self.check_data).grid(row=0, column=4, padx=5, pady=5)
        ttk.Button(self.root, text="Run ETL", command=self.run_etl).grid(row=0, column=5, padx=5, pady=5)
        ttk.Button(self.root, text="Refresh Cashiers", command=self.refresh_cashiers).grid(row=0, column=6, padx=5, pady=5)
        
        # Log display
        self.log_text = scrolledtext.ScrolledText(self.root, wrap=tk.WORD, width=100, height=30)
        self.log_text.grid(row=1, column=0, columnspan=7, padx=5, pady=5, sticky="nsew")
        
        # Configure grid weights
        self.root.grid_rowconfigure(1, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        
        self.logger = ETLLogger(self.log_text)
        
    def log(self, message, level="INFO"):
        self.log_queue.put((message, level))
        if not self.after_id:
            self.process_log_queue()
            
    def process_log_queue(self):
        try:
            while True:
                message, level = self.log_queue.get_nowait()
                self.logger.log(message, level)
        except queue.Empty:
            pass
            
        self.after_id = self.root.after(100, self.process_log_queue)
        
    def check_data(self):
        start_date = self.start_date.get()
        end_date = self.end_date.get()
        
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start > end:
                messagebox.showerror("Error", "Start date must be before end date")
                return
                
            self.log(f"Checking data from {start_date} to {end_date}...")
            
            # Check data lake directories
            data_lake = Path(os.getenv('DATA_LAKE_ROOT', '/app/data-lake'))
            raw_path = data_lake / 'raw' / 'pos_order_lines'
            clean_path = data_lake / 'clean' / 'pos_order_lines'
            
            delta = end - start
            for i in range(delta.days + 1):
                date = start + timedelta(days=i)
                date_str = date.strftime('%Y-%m-%d')
                year, month, day = date_str.split('-')
                
                raw_file = raw_path / f'year={year}' / f'month={month}' / f'day={day}' / f'pos_order_lines_{date_str}.parquet'
                clean_file = clean_path / f'year={year}' / f'month={month}' / f'day={day}' / f'pos_order_lines_clean_{date_str}.parquet'
                
                status = []
                if raw_file.exists():
                    status.append("RAW")
                if clean_file.exists():
                    status.append("CLEAN")
                    
                self.log(f"{date_str}: {' | '.join(status) if status else 'No data'}")
                
        except ValueError as e:
            messagebox.showerror("Error", f"Invalid date format. Use YYYY-MM-DD. Error: {e}")
            
    def run_etl(self):
        start_date = self.start_date.get()
        end_date = self.end_date.get()
        
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start > end:
                messagebox.showerror("Error", "Start date must be before end date")
                return
                
            self.log(f"Starting ETL for {start_date} to {end_date}...")
            
            # Run in background to keep UI responsive
            def run_in_background():
                try:
                    # Call the ETL pipeline directly
                    result = date_range_etl_pipeline(start_date, end_date)
                    self.log(f"ETL completed: {result}")
                except Exception as e:
                    self.log(f"Error running ETL: {str(e)}", "ERROR")
                    
            thread = threading.Thread(target=run_in_background, daemon=True)
            thread.start()
            
        except ValueError as e:
            messagebox.showerror("Error", f"Invalid date format. Use YYYY-MM-DD. Error: {e}")

    def refresh_cashiers(self):
        """Refresh the cashier dimension table."""
        self.log("Starting cashier dimension refresh...")
        
        def run_in_background():
            try:
                # Call the dimension refresh for cashiers only
                result = refresh_dimensions_incremental(targets=['cashiers'])
                if result.get('updated', False):
                    count = result.get('targets', {}).get('cashiers', 0)
                    self.log(f"Cashier dimension refreshed successfully: {count} employees")
                else:
                    error = result.get('error', 'Unknown error')
                    self.log(f"Error refreshing cashiers: {error}", "ERROR")
            except Exception as e:
                self.log(f"Error refreshing cashiers: {str(e)}", "ERROR")
                
        thread = threading.Thread(target=run_in_background, daemon=True)
        thread.start()

if __name__ == "__main__":
    root = tk.Tk()
    app = ETLRunnerApp(root)
    root.mainloop()