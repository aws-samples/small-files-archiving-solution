import streamlit as st
import subprocess
import pandas as pd
import io
import os

# Initialize session state variables if they don't exist
if 'parsed_df' not in st.session_state:
    st.session_state.parsed_df = None
if 'selected_tar_name' not in st.session_state:
    st.session_state.selected_tar_name = None
if 'selected_start_byte' not in st.session_state:
    st.session_state.selected_start_byte = None
if 'selected_stop_byte' not in st.session_state:
    st.session_state.selected_stop_byte = None
if 'selected_index' not in st.session_state:
    st.session_state.selected_index = None
if 'selected_bucket_name' not in st.session_state:
    st.session_state.selected_bucket_name= None

def run_restore_script(bucket_name, key_name, start_byte, stop_byte):
    program = "apps/restore.py"
    cmd = [
        'python3', program,
        '--bucket_name', bucket_name, 
        '--key_name', key_name, 
        '--start_byte', start_byte, 
        '--stop_byte', stop_byte
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout, result.stderr

st.title("Restoring a file in Amazon S3")

bucket_name = st.text_input("Enter the bucket name:", value=st.session_state.selected_bucket_name or "")
tar_name = st.text_input("Enter the tar name (tar file):", value=st.session_state.selected_tar_name or "")
start_byte = st.text_input("Enter the start byte:", value=st.session_state.selected_start_byte or "")
stop_byte = st.text_input("Enter the stop byte:", value=st.session_state.selected_stop_byte or "")

if st.button("Restore"):
    if tar_name and start_byte and stop_byte:
        st.info("Restoring... Please wait.")
        stdout, stderr = run_restore_script(bucket_name, tar_name, start_byte, stop_byte)
        
        if stdout:
            st.success("Restore completed successfully!")
            st.text("Output:")
            st.code(stdout)
        
        if stderr:
            st.error("An error occurred during the restore process:")
            st.code(stderr)
    else:
        st.warning("Please fill in all the fields before restoring.")
