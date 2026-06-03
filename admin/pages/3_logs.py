import streamlit as st
import sys
import os

# Ensure import paths resolve
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from admin.theme import apply_cohere_theme
from admin.core import get_log_files

st.set_page_config(page_title="Scheduler Logs", page_icon="📋", layout="wide")
apply_cohere_theme()

st.title("📋 ETL Scheduler Logs")

log_files = get_log_files()

if not log_files:
    st.info("No log files found.")
else:
    selected_log = st.selectbox("Select Log File", options=[f.name for f in log_files])
    
    if selected_log:
        file_path = log_files[0].parent / selected_log
        st.write(f"**Viewing:** `{selected_log}`")
        
        try:
            with open(file_path, "r") as f:
                content = f.read()
                if not content:
                    st.code("File is empty.", language="text")
                else:
                    st.code(content, language="log")
        except Exception as e:
            st.error(f"Could not read log file: {e}")

    if st.button("Refresh Logs"):
        st.rerun()
