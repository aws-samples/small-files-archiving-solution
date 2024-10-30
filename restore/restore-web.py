import streamlit as st
import subprocess
import os

def run_restore_script(bucket_name, key_name, start_byte, stop_byte):
    cmd = ['python3', 'restore_tar.py', 
           '--bucket_name', bucket_name, 
           '--key_name', key_name, 
           '--start_byte', start_byte, 
           '--stop_byte', stop_byte]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout, result.stderr

st.title("S3 Tar Archive Restore Tool")

st.write("This tool allows you to restore a specific part of a tar archive from an S3 bucket.")


bucket_name = st.text_input("Enter the bucket name :", value="your-bucket")
key_name = st.text_input("Enter the key name (tar file):", value="archives/archive_20241030_065337_0010.tar")
start_byte = st.text_input("Enter the start byte:", value="363008")
stop_byte = st.text_input("Enter the stop byte:", value="369663")

if st.button("Restore"):
        st.info("Restoring... Please wait.")
        stdout, stderr = run_restore_script(bucket_name, key_name, start_byte, stop_byte)
        
        if stdout:
            st.success("Restore completed successfully!")
            st.text("Output:")
            st.code(stdout)
        
        if stderr:
            st.error("An error occurred during the restore process:")
            st.code(stderr)
else:
    st.warning("Please fill in all the fields before restoring.")

st.write("Note: Make sure 'restore_tar.py' is in the same directory as this Streamlit script.")

