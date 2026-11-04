# app.py
import streamlit as st


# Add a navigation sidebar
apps = {
    "Archiver": "apps/archiver",
    "Search": "apps/search",
    "Restore": "apps/restore",
}

st.sidebar.title("S3 Archiver Menu")
selection = st.sidebar.radio("Go to", list(apps.keys()))

## Load the selected page
with open(f"{apps[selection]}-web.py", "r") as f:
    code = f.read()

exec(code)
