import streamlit as st
import sys
import os

# Fix PYTHONPATH issue when running directly inside the container
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin.theme import apply_cohere_theme
from admin.core import init_admin_directories

st.set_page_config(
    page_title="NKDash Admin Hub",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_cohere_theme()
init_admin_directories()

st.title("⚙️ NKDash Admin Hub")
st.markdown("""
Welcome to the **NKDash Admin Hub**. This interface is strictly for maintenance, monitoring, and triggering background ETL tasks.

### 🧭 Navigation
- **📊 Scanner**: Visualize Data Lake partitions and sizes.
- **🚀 Trigger**: Manually enqueue ETL jobs.
- **📋 Logs**: Monitor background scheduler output.
- **❤️ Health**: View overall system and ETL state.

---
*Note: Operations performed here interact directly with the single source of truth Data Lake defined in `etl.config`.*
""")
