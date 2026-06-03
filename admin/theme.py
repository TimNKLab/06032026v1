import streamlit as st

def apply_cohere_theme():
    """Inject CSS to align Streamlit with Dash's Cohere Design System."""
    cohere_css = """
    <style>
        /* Import Google Fonts used in Dash */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Space+Grotesk:wght@400;500;600&display=swap');
        
        /* Base Typography */
        html, body, [class*="st-"] {
            font-family: 'Inter', -apple-system, sans-serif;
            color: #212121;
            background-color: #fafafa;
        }

        /* Headings Typography */
        h1, h2, h3 {
            font-family: 'Space Grotesk', sans-serif !important;
            color: #000000 !important;
            letter-spacing: -0.5px;
        }

        /* Card / Container Styling (The Cohere 22px signature radius) */
        div[data-testid="stVerticalBlock"] > div {
            background-color: #ffffff;
            border: 1px solid #f2f2f2;
            border-radius: 22px;
            padding: 1rem;
        }
        
        /* Remove inner border from nested blocks */
        div[data-testid="stVerticalBlock"] > div > div[data-testid="stVerticalBlock"] > div {
            border: none;
            padding: 0;
            background-color: transparent;
        }

        /* Primary Button */
        button[kind="primary"] {
            background-color: #1863dc !important;
            color: #ffffff !important;
            border-radius: 9999px !important; /* Pill shape */
            font-weight: 500;
        }

        /* Secondary Button (Ghost/Transparent) */
        button[kind="secondary"] {
            background-color: transparent !important;
            color: #000000 !important;
            border: 1px solid #d9d9dd !important;
            border-radius: 9999px !important;
        }
        
        button[kind="secondary"]:hover {
            color: #1863dc !important;
            border-color: #1863dc !important;
        }

        /* Hide Streamlit specific clutters */
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        footer {visibility: hidden;}
        .stDeployButton {display:none;}
    </style>
    """
    st.markdown(cohere_css, unsafe_allow_html=True)
