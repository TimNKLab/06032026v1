import streamlit as st
import sys
import os
from datetime import datetime

# Ensure import paths resolve
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from admin.theme import apply_cohere_theme
from admin.core import enqueue_task, get_queue_status

st.set_page_config(page_title="ETL Trigger", page_icon="🚀", layout="wide")
apply_cohere_theme()

st.title("🚀 Manual ETL Trigger")
st.markdown("Enqueue data extraction and transformation tasks.")

st.subheader("Queue New Task")

col1, col2 = st.columns(2)

with col1:
    task_type = st.selectbox(
        "Task Type",
        options=[
            "daily_pipeline", 
            "refresh_dimensions",
            "backfill_history"
        ]
    )
    
with col2:
    if task_type == "backfill_history":
        date_range = st.date_input("Date Range", value=(datetime.now(), datetime.now()))
        target_date_str = f"{date_range[0]} to {date_range[1]}" if len(date_range) == 2 else str(date_range[0])
    else:
        target_date = st.date_input("Target Date")
        target_date_str = str(target_date)

if st.button("Enqueue Task", type="primary"):
    try:
        enqueue_task(task_type, target_date_str)
        st.success(f"Successfully enqueued `{task_type}` for `{target_date_str}`")
    except Exception as e:
        st.error(f"Failed to enqueue task: {str(e)}")

st.divider()

st.subheader("Current Queue Status")
if st.button("Refresh Queue"):
    df = get_queue_status()
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("Queue is empty or unavailable.")
