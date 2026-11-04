import streamlit as st
import subprocess
import os

def is_process_running(pid):
    try:
        return psutil.pid_exists(pid)
    except:
        return False

def check_lock_file():
    lock_file = "archiver.lock"
    if os.path.exists(lock_file):
        with open(lock_file, 'r') as f:
            pid = int(f.read().strip())
            if is_process_running(pid):
                return True
    return False

def create_lock_file(pid):
    lock_file = "archiver.lock"
    with open(lock_file, 'w') as f:
        f.write(str(pid))

def remove_lock_file():
    lock_file = "archiver.lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)

def next_page():
    st.session_state.page += 1

def prev_page():
    st.session_state.page -= 1

def run_archiver(command):
    try:
        if check_lock_file():
            return "", "Another archiver process is already running. Please wait."
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)

        # Create lock file with current process ID
        create_lock_file(process.pid)
        # Explicitly set timeout=None to wait indefinitely
        stdout, stderr = process.communicate(timeout=None)
        # Remove lock file after completion
        remove_lock_file()
        return stdout, stderr
    except subprocess.TimeoutExpired as e:
        process.kill()
        return "", f"Process timed out: {str(e)}"
    except Exception as e:
        return "", f"Error occurred: {str(e)}"

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
        if check_lock_file():
            st.error("Another archiver process is already running. Please wait.")
        else: 
            # Store the execution state in session
            if 'is_running' not in st.session_state:
                st.session_state.is_running = True
                st.session_state.execution_completed = False

            if st.session_state.is_running:
                with st.spinner("Running archiver..."):
                    stdout, stderr = run_archiver(command)
                    # Store results in session state
                    st.session_state.stdout = stdout
                    st.session_state.stderr = stderr
                    st.session_state.is_running = False
                    st.session_state.execution_completed = True

            # Display results
            if st.session_state.execution_completed:
                st.subheader("Execution Result")
                if st.session_state.stderr:
                    st.subheader("Job report")
                    st.text_area("Output", st.session_state.stderr, height=200)

    # Add this to prevent accidental navigation while process is running
    if 'is_running' in st.session_state and st.session_state.is_running:
        st.warning("Process is running. Please wait until it completes.")
        # Disable the Back button while running
        st.button("Back", disabled=True)
    else:
        if st.button("Back", on_click=prev_page):
            pass
