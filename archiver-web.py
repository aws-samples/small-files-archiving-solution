import streamlit as st
import subprocess

def run_archiver(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
    stdout, stderr = process.communicate()
    return stdout, stderr

st.title("File Archiver")

# Initialize session state
if 'page' not in st.session_state:
    st.session_state.page = 1

if 'src_type' not in st.session_state:
    st.session_state.src_type = None

if 'dst_type' not in st.session_state:
    st.session_state.dst_type = None

# Page 1: Source and Destination Type Selection
if st.session_state.page == 1:
    st.header("Step 1: Select Source and Destination Types")
    
    st.session_state.src_type = st.selectbox("Select Source Type", ['fs', 's3'], key='src_type_select')
    st.session_state.dst_type = st.selectbox("Select Destination Type", ['fs', 's3'], key='dst_type_select')

    if st.button("Next"):
        st.session_state.page = 2

# Page 2: Parameter Input and Execution
elif st.session_state.page == 2:
    st.header("Step 2: Enter Parameters and Run")

    function_name = f"{st.session_state.src_type}to{st.session_state.dst_type}"
    
    st.subheader(f"Parameters for {function_name}")

    # Common parameters
    log_level = st.selectbox("Log Level", ['INFO', 'DEBUG', 'WARNING', 'ERROR'])
    max_process = st.number_input("Max Process", min_value=1, value=4)
    combine = st.selectbox("Combine Method", ['size', 'count'])

    # Function-specific parameters
    if function_name == "fstos3":
        src_path = st.text_input("Source Path")
        dst_bucket = st.text_input("Destination Bucket")
        dst_path = st.text_input("Destination Path")
        max_tarfile_size = st.number_input("Max Tarfile Size", value=107374100)
        
        command = f"python3 archiver.py --src-type fs --dst-type s3 --src-path {src_path} --dst-bucket {dst_bucket} --dst-path {dst_path} --log-level {log_level} --max-process {max_process} --combine {combine} --max-tarfile-size {max_tarfile_size}"

    elif function_name == "fstos3_input":
        input_file = st.text_input("Input File")
        dst_bucket = st.text_input("Destination Bucket")
        dst_path = st.text_input("Destination Path")
        max_tarfile_size = st.number_input("Max Tarfile Size", value=107374100)
        
        command = f"python3 archiver.py --src-type fs --dst-type s3 --input-file {input_file} --dst-bucket {dst_bucket} --dst-path {dst_path} --log-level {log_level} --max-process {max_process} --combine {combine} --max-tarfile-size {max_tarfile_size}"

    elif function_name == "fstofs":
        src_path = st.text_input("Source Path")
        dst_path = st.text_input("Destination Path")
        max_tarfile_size = st.number_input("Max Tarfile Size", value=1073741824)
        
        command = f"python3 archiver.py --src-type fs --dst-type fs --src-path {src_path} --dst-path {dst_path} --log-level {log_level} --max-process {max_process} --combine {combine} --max-tarfile-size {max_tarfile_size}"

    elif function_name == "s3tofs":
        src_bucket = st.text_input("Source Bucket")
        src_path = st.text_input("Source Path")
        dst_path = st.text_input("Destination Path")
        max_file_number = st.number_input("Max File Number", value=100)
        
        command = f"python3 archiver.py --src-type s3 --dst-type fs --src-bucket {src_bucket} --src-path {src_path} --dst-path {dst_path} --log-level {log_level} --max-process {max_process} --combine {combine} --max-file-number {max_file_number}"

    elif function_name == "s3tos3":
        src_bucket = st.text_input("Source Bucket")
        src_path = st.text_input("Source Path")
        dst_bucket = st.text_input("Destination Bucket")
        dst_path = st.text_input("Destination Path")
        max_tarfile_size = st.number_input("Max Tarfile Size", value=136870912)
        
        command = f"python3 archiver.py --src-type s3 --dst-type s3 --src-bucket {src_bucket} --src-path {src_path} --dst-bucket {dst_bucket} --dst-path {dst_path} --log-level {log_level} --max-process {max_process} --combine {combine} --max-tarfile-size {max_tarfile_size}"

    if st.button("Run"):
        with st.spinner("Running archiver..."):
            stdout, stderr = run_archiver(command)
        
        st.subheader("Execution Result")
        if stdout:
            st.subheader("Job report")
            st.text_area("Output", stdout, height=200)
        if stderr:
            st.subheader("Standard out")
            st.text_area("Error", stderr, height=200)

    if st.button("Back"):
        st.session_state.page = 1

