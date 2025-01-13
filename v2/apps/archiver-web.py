import streamlit as st
import subprocess

def next_page():
    st.session_state.page += 1

def prev_page():
    st.session_state.page -= 1

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

if 'batch' not in st.session_state:
    st.session_state.batch = None

# Page 1: Source and Destination Type Selection
if st.session_state.page == 1:
    st.header("Step 1: Select Source and Batch Types")
    
    st.session_state.src_type = st.selectbox("Select Source Type", ['fs', 's3'], key='src_type_select')
    st.session_state.dst_type = st.selectbox("Select Destination Type", ['s3'], key='dst_type_select')
    st.session_state.batch = st.selectbox("Select batch strategy", ['size', 'count'], key='batch_select')

    if st.button("Next", on_click=next_page):
        pass

# Page 2: Parameter Input and Execution
elif st.session_state.page == 2:
    st.header("Step 2: Enter Parameters and Run")

    function_name = f"{st.session_state.src_type}_to_{st.session_state.dst_type}"
    
    st.subheader(f"Parameters for {function_name}")

    batch = st.session_state.batch

    # Common parameters
    num_threads = st.number_input("Numbers of Thread", min_value=1, value=5)
    storage_classes = ['STANDARD','STANDARD_IA','ONEZONE_IA','INTELLIGENT_TIERING','GLACIER','DEEP_ARCHIVE','GLACIER_IR','EXPRESS_ONEZONE']
    tar_storageclass = st.selectbox('Select storage class for tar file:', storage_classes)

    # Function-specific parameters
    if function_name == "fs_to_s3":
        src_path = st.text_input("Source Path")
        dst_bucket = st.text_input("Destination Bucket")
        dst_prefix = st.text_input("Destination Prefix")
        program = "apps/fss3-archiver.py"

        if batch == "size":
            max_size = st.text_input("Max Tarfile Size(MB,GB)", value="100MB")
            command = f"python3 {program} --src-path {src_path} --dst-bucket {dst_bucket} --dst-prefix {dst_prefix} --num-threads {num_threads} --max-size {max_size} --tar-storageclass {tar_storageclass}"
        else:
            max_files = st.number_input("Max files in a tarfile ", value=10000)
            command = f"python3 {program} --src-path {src_path} --dst-bucket {dst_bucket} --dst-prefix {dst_prefix} --num-threads {num_threads} --max-files {max_files} --tar-storageclass {tar_storageclass}"
        
    elif function_name == "s3_to_s3":
        src_bucket = st.text_input("Source Bucket")
        src_prefix = st.text_input("Source Prefix")
        dst_bucket = st.text_input("Destination Bucket")
        dst_prefix = st.text_input("Destination Prefix")
        program = "apps/s3s3-archiver.py"

        if batch == "size":
            max_size = st.text_input("Max Tarfile Size(MB,GB)", value="100MB")
            command = f"python3 {program} --src-bucket {src_bucket} --src-prefix {src_prefix} --dst-bucket {dst_bucket} --dst-prefix {dst_prefix} --num-threads {num_threads} --max-size {max_size} --tar-storageclass {tar_storageclass}"
        else:
            max_files = st.number_input("Max files in a tarfile ", value=10000)
            command = f"python3 {program} --src-bucket {src_bucket} --src-prefix {src_prefix} --dst-bucket {dst_bucket} --dst-prefix {dst_prefix} --num-threads {num_threads} --max-files {max_files} --tar-storageclass {tar_storageclass}"

    if st.button("Run"):
        with st.spinner("Running archiver..."):
            stdout, stderr = run_archiver(command)
        
        st.subheader("Execution Result")
        #if stdout:
        #    st.subheader("Job report")
        #    st.text_area("Output", stdout, height=200)
        if stderr:
            st.subheader("Job report")
            st.text_area("Output", stderr, height=200)

    st.warning("After press 'RUN', do not move to other page. Status will disapper")

    if st.button("Back", on_click=prev_page):
        pass
