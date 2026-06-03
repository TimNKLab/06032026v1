import streamlit as st
import pandas as pd
import sys
import os

# Ensure import paths resolve
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from admin.theme import apply_cohere_theme
from admin.core import scan_data_lake, format_size

st.set_page_config(page_title="Partition Scanner", page_icon="📊", layout="wide")
apply_cohere_theme()

st.title("📊 Data Lake Scanner")
st.markdown("Scan and monitor raw, clean, and star-schema partitions.")

if st.button("Refresh Scan"):
    with st.spinner("Scanning data lake..."):
        results = scan_data_lake()
        if results:
            df = pd.DataFrame(results)
            # Sort by Zone then Size
            df = df.sort_values(by=["Zone", "Raw Size (Bytes)"], ascending=[True, False])
            
            # Display metrics
            total_size = format_size(df["Raw Size (Bytes)"].sum())
            st.metric("Total Data Lake Size", total_size)
            
            # Display dataframe
            st.dataframe(
                df.drop(columns=["Raw Size (Bytes)"]), 
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Data lake is empty or uninitialized.")
