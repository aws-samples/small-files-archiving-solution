import streamlit as st
import subprocess
import pandas as pd
import io
import os

# Initialize session state variables if they don't exist
if 'parsed_df' not in st.session_state:
    st.session_state.parsed_df = None
if 'selected_bucket_name' not in st.session_state:
    st.session_state.selected_bucket_name = None
if 'selected_tar_name' not in st.session_state:
    st.session_state.selected_tar_name = None
if 'selected_start_byte' not in st.session_state:
    st.session_state.selected_start_byte = None
if 'selected_stop_byte' not in st.session_state:
    st.session_state.selected_stop_byte = None
if 'selected_index' not in st.session_state:
    st.session_state.selected_index = None

def run_search(bucket_name, prefix, search_type, search_value, end_value=None):
    program = "apps/search.py"

    cmd = [
        "python3", program,
        "--bucket", bucket_name,
        "--prefix", prefix,
        "--search_type", search_type,
        "--search_value", search_value
    ]

    if end_value:
        cmd.extend(["--end_value", end_value])

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout

def parse_output(output):
    # Split the output into lines
    lines = output.strip().split('\n')
    
    # Initialize lists to store the data
    index = []
    tarfile_locations = []
    filenames = []
    start_bytes = []
    stop_bytes = []
    date = []

    # Parse each line
    for line in lines:
        parts = line.split('|')
        if len(parts) >= 6:  # Ensure we have at least 6 parts
            index.append(parts[0])
            tarfile_locations.append(parts[1])
            filenames.append(parts[2])
            start_bytes.append(parts[3])
            stop_bytes.append(parts[4])
            date.append(parts[5])

    # Create a DataFrame
    pd.set_option('display.max_colwidth', None)
    df = pd.DataFrame({
        'index': index,
        'tarfile_location': tarfile_locations,
        'filename': filenames,
        'start_bytes': start_bytes,
        'stop_bytes': stop_bytes,
        'date': date 
    })

    return df


def update_selection():
        selected_row = st.session_state.parsed_df.loc[st.session_state.selected_index]
        st.session_state.selected_bucket_name = bucket_name
        st.session_state.selected_tar_name = selected_row['tarfile_location']
        st.session_state.selected_start_byte = selected_row['start_bytes']
        st.session_state.selected_stop_byte = selected_row['stop_bytes']

st.title("Searching files in Amazon S3")

# Input for bucket name and prefix
bucket_name = st.text_input("Enter S3 bucket", value="your-own-dest-bucket")
prefix = st.text_input("Enter Prefix", value="day101/manifests")

# define sidebar
#page = st.sidebar.selectbox("Menu", ["Search", "Restore"])

st.header("Search archived file in S3")
search_type = st.radio("Search Type", ["Name", "Date"])

if search_type == "Name":
    search_value = st.text_input("Enter file name to search")
    end_value = None
else:
    col1, col2 = st.columns(2)
    with col1:
        search_value = st.date_input("Start Date")
    with col2:
        end_value = st.date_input("End Date")

    search_value = search_value.strftime("%Y-%m-%d")
    end_value = end_value.strftime("%Y-%m-%d")

if st.button("Search"):

    if bucket_name and prefix and search_value:
        with st.spinner("Running archiver..."):
            result = run_search(bucket_name, prefix, search_type.lower(), search_value, end_value)
        parsed_df = parse_output(result)
    
        st.session_state.parsed_df = parse_output(result)
    
        if not st.session_state.parsed_df.empty:
            st.subheader("Parsed Results")
            st.dataframe(st.session_state.parsed_df)
            
            # Use the callback to update session state
            st.selectbox(
                'Select file:', 
                st.session_state.parsed_df.index,
                key='selected_index',
                on_change=update_selection
            )
            
            if st.session_state.selected_index is not None:
                st.write(f"Selected index: {st.session_state.selected_index}")
                st.write(f"Selected tar name: {st.session_state.selected_tar_name}")
                st.write(f"Selected start byte: {st.session_state.selected_start_byte}")
                st.write(f"Selected stop byte: {st.session_state.selected_stop_byte}")
    
            # Add download button for CSV
            csv = parsed_df.to_csv(index=False)
            st.download_button(
                label="Download results as CSV",
                data=csv,
                file_name="search_results.csv",
                mime="text/csv",
            )
    
        else:
            st.info("No results found or unable to parse the output.")
    else:
        st.warning("Please enter all required fields: Bucket Name, Prefix, and Search Value.")
