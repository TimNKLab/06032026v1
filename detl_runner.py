import subprocess
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import json
import threading
import os

class DockerETLRunner:
    def __init__(self, root):
        self.root = root
        self.setup_ui()
        
    def setup_ui(self):
        self.root.title("Docker ETL Runner")
        
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
        ttk.Button(self.root, text="Run ETL in Docker", command=self.run_docker_etl).grid(row=0, column=4, padx=5, pady=5)

        # Dimension refresh controls
        ttk.Label(self.root, text="Dimensions:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.dimension_vars = {
            "products": tk.BooleanVar(value=True),
            "categories": tk.BooleanVar(value=True),
            "brands": tk.BooleanVar(value=True),
            "cashiers": tk.BooleanVar(value=False),
        }
        for idx, (name, var) in enumerate(self.dimension_vars.items(), start=1):
            ttk.Checkbutton(self.root, text=name.title(), variable=var).grid(row=1, column=idx, padx=5, pady=5, sticky="w")

        ttk.Button(self.root, text="Refresh Dimensions", command=self.refresh_dimensions).grid(row=1, column=4, padx=5, pady=5)
        
        # Log display
        self.log_text = scrolledtext.ScrolledText(self.root, wrap=tk.WORD, width=80, height=20)
        self.log_text.grid(row=2, column=0, columnspan=6, padx=5, pady=5, sticky="nsew")
        
        # Configure grid weights
        self.root.grid_rowconfigure(2, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        
    def log(self, message):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.root.update_idletasks()
        
    def _run_async_command(self, cmd, success_message="Command completed successfully", failure_message="Command failed"):
        self.log(f"Running command: {' '.join(cmd)}")

        def run_command():
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            for line in process.stdout:
                self.log(line.strip())

            process.wait()
            if process.returncode == 0:
                self.log(success_message)
            else:
                self.log(f"{failure_message} (exit code {process.returncode})")

        threading.Thread(target=run_command, daemon=True).start()

    def run_docker_etl(self):
        start_date = self.start_date.get()
        end_date = self.end_date.get()
        
        try:
            # Validate dates
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start > end:
                messagebox.showerror("Error", "Start date must be before end date")
                return
                
            self.log(f"Starting ETL in Docker for {start_date} to {end_date}...")
            
            # Build the Docker command
            cmd = [
                "docker-compose", "run", "--rm",
                "-e", f"START_DATE={start_date}",
                "-e", f"END_DATE={end_date}",
                "celery-worker",
                "python", "-c",
                f"from etl_tasks import date_range_etl_pipeline; date_range_etl_pipeline('{start_date}', '{end_date}')"
            ]
            
            self._run_async_command(cmd, success_message="ETL completed successfully!", failure_message="ETL failed")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to run ETL: {str(e)}")

    def refresh_dimensions(self):
        start_date = self.start_date.get()
        end_date = self.end_date.get()
        selected_targets = [name for name, var in self.dimension_vars.items() if var.get()]

        if not selected_targets:
            messagebox.showerror("Error", "Select at least one dimension to refresh")
            return

        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            if start > end:
                messagebox.showerror("Error", "Start date must be before end date")
                return

            targets_json = json.dumps(selected_targets)
            self.log(f"Refreshing dimensions {selected_targets} for {start_date} to {end_date}...")

            cmd = [
                "docker-compose", "run", "--rm",
                "-e", f"START_DATE={start_date}",
                "-e", f"END_DATE={end_date}",
                "celery-worker",
                "python", "-c",
                (
                    "from etl_tasks import refresh_dimensions_incremental; "
                    f"refresh_dimensions_incremental(targets={targets_json})"
                ),
            ]

            self._run_async_command(cmd, success_message="Dimension refresh completed!", failure_message="Dimension refresh failed")

        except ValueError:
            messagebox.showerror("Error", "Please enter valid dates in YYYY-MM-DD format")
        except Exception as exc:
            messagebox.showerror("Error", f"Failed to refresh dimensions: {exc}")


if __name__ == "__main__":
    root = tk.Tk()
    app = DockerETLRunner(root)
    root.mainloop()