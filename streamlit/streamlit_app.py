import time

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Gold Price Dashboard", layout="wide")

with open("ws_dashboard.html", "r") as f:
    html_content = f.read()
html_content += f"\n<!-- {time.time()} -->"

components.html(html_content, height=1700, scrolling=True)
