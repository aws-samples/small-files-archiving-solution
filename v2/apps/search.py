import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
import argparse
import sys

# Initialize S3 client
s3 = boto3.client('s3')

def list_csv_files(bucket, prefix):
    """List all CSV files under the given prefix in the S3 bucket"""
    csv_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                csv_files.append(obj['Key'])
    return csv_files

def read_csv_from_s3(bucket, key):
    """Read CSV file from S3 and return a DataFrame"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(body), header=None, sep='|', skiprows=1)

def create_dataframe(bucket, prefix):
    """Create DataFrame from all CSV files under the prefix in S3"""
    csv_files = list_csv_files(bucket, prefix)
    df_list = []
    for file in csv_files:
        df = read_csv_from_s3(bucket, file)
        df_list.append(df)
    
    combined_df = pd.concat(df_list, ignore_index=True)
    
    # Assign column names
    combined_df.columns = ['tarfile_location', 'filename', 'str_date', 
                           'filesize', 'start_byte', 'stop_byte', 'md5']
    
    # Convert str_date to datetime
    pd.set_option('display.max_colwidth', None)
    combined_df['date'] = pd.to_datetime(combined_df['str_date'], format='%Y-%m-%d', errors='coerce')
    
    return combined_df

def search_by_name(df, name):
    """Search files by name"""
    #print(df.columns.values)
    filtered_df = df[df['filename'].str.contains(name, case=False)]
    return filtered_df[["tarfile_location", "filename", "start_byte", "stop_byte", "date"]]

def search_by_date(df, start_date, end_date):
    """Search files by date range"""
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    return df[["tarfile_location", "filename", "start_byte", "stop_byte", "date"]].loc[mask]

def parse_arguments():
    parser = argparse.ArgumentParser(description='Search S3 CSV files')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='manifests/', help='S3 prefix for CSV files')
    parser.add_argument('--search_type', choices=['name', 'date'], required=True, help='Type of search to perform')
    parser.add_argument('--search_value', required=True, help='Search value (name, start_date, or start_byte)')
    parser.add_argument('--end_value', help='End value for date search')
    return parser.parse_args()

# Main execution
if __name__ == "__main__":
    args = parse_arguments()

    # Create DataFrame
    df = create_dataframe(args.bucket, args.prefix)

    # Perform search based on arguments
    if args.search_type == 'name':
        result = search_by_name(df, args.search_value)
    elif args.search_type == 'date':
        start_date = datetime.strptime(args.search_value, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_value, '%Y-%m-%d') if args.end_value else start_date
        result = search_by_date(df, start_date, end_date)

    # Print results
    #print(result)
    result.to_csv(sys.stdout, sep='|')

