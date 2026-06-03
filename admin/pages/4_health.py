import streamlit as st
import sys
import os

# Ensure import paths resolve
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from admin.theme import apply_cohere_theme
from admin.core import get_etl_state

st.set_page_config(page_title="ETL Health", page_icon="❤️", layout="wide")
apply_cohere_theme()

st.title("❤️ ETL Health Status")
st.markdown("Overview of the ETL system's operational status.")

state = get_etl_state()

if not state:
    st.warning("No state file found. The ETL scheduler may not have run yet.")
else:
    last_run = state.get("last_run_time", "Unknown")
    status = state.get("last_status", "Unknown")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if status == "success":
            st.success(f"**Last Run Status:** SUCCESS ✅")
        elif status == "failed":
            st.error(f"**Last Run Status:** FAILED ❌")
        else:
            st.info(f"**Last Run Status:** {status}")
            
    with col2:
        st.info(f"**Last Run Time:** {last_run}")
        
    st.divider()
    
    st.subheader("Raw State Data")
    st.json(state)

if st.button("Refresh Status"):
    st.rerun()
