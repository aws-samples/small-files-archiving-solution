import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
import argparse
import sys
import awswrangler as wr
from typing import Optional, List, Dict


# Initialize S3 client
s3 = boto3.client('s3')

# Create Athena table
def create_manifest_table(
    database: str,
    table_name: str,
    bucket_name: str,
    prefix: str,
    workgroup: str = 'primary'
) -> None:
    """
    Create Athena table for manifest data with recursive directory scanning
    """
    # Remove trailing slash if present
    prefix = prefix.rstrip('/')
    
    # Construct the full S3 path
    s3_location = f's3://{bucket_name}/{prefix}/manifests/'
    
    # Define table columns and types
    columns_types = {
        "tarname": "string",
        "filename": "string",
        "current_date": "timestamp",
        "size": "int",
        "start_byte": "int",
        "stop_byte": "int",
        "md5": "string"
    }
    
    # Define table properties with recursive directory walk
    table_properties = {
        'delimiter': '|',
        'skip.header.line.count': '1',
        'classification': 'csv',
        'typeOfData': 'file',
        'recursiveDirectoryWalk': 'true'
    }
    
    try:
        # Delete existing table if it exists
        wr.catalog.delete_table_if_exists(
            database=database,
            table=table_name
        )
        
        # Create table using Wrangler's catalog function
        response = wr.catalog.create_csv_table(
            database=database,
            table=table_name,
            path=s3_location,
            columns_types=columns_types,
            sep='|',
            skip_header_line_count=1,
            table_type="EXTERNAL_TABLE",
            parameters=table_properties
        )
        #print(f"Table {table_name} created successfully")
        
        # Count files in the location using correct method
        paths = wr.s3.list_objects(s3_location)
        csv_files = [path for path in paths if path.endswith('.csv')]
        #print(f"Found {len(csv_files)} CSV files in the location")
        
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        raise

def search_keys_in_range(
    database: str,
    table_name: str,
    key_name: str,
    start_date: str,
    end_date: str,
    workgroup: str = 'primary'
) -> Optional[List[Dict]]:
    """
    Search for keys within a date range from the manifest table
    """
    try:
        # Construct query with parameters and column aliases
        query = f"""
        SELECT 
            tarname AS tarfile_location,
            filename AS filename,
            start_byte AS start_byte,
            stop_byte AS stop_byte,
            current_date AS date
        FROM {table_name}
        WHERE filename LIKE '%{key_name}%'
        AND current_date BETWEEN TIMESTAMP '{start_date} 00:00:00' 
        AND TIMESTAMP '{end_date} 23:59:59'
        ORDER BY tarname ASC
        """
        
        # Execute query
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            workgroup=workgroup
        )
        
        if df.empty:
            print(f"No records found for key '{key_name}' between {start_date} and {end_date}")
            return None
        
        # Convert DataFrame to list of dictionaries
        #results = df.to_dict('records')
        #print(f"\nFound {len(results)} matching records:")
        # Print results in CSV format for web interface
        df.to_csv(sys.stdout, sep='|', index=True,  header=False)
        return df
        
    except Exception as e:
        print(f"Error searching keys: {e}")
        return None

def parse_arguments():
    parser = argparse.ArgumentParser(description='Search S3 CSV files')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', help='S3 prefix for CSV files')
    parser.add_argument('--search_value', required=True, help='Search value for keyword search)')
    parser.add_argument('--start_value', required=True, help='start_datefor date search')
    parser.add_argument('--end_value', required=True, help='End value for date search')
    return parser.parse_args()

# Main execution
if __name__ == "__main__":
    args = parse_arguments()

    # Define global variables
    DATABASE="default"
    TABLE_NAME="SFAS_Manifests_Table"
    WORKGROUP = "primary"
    
    # Create manifest table 
    create_manifest_table(
        database=DATABASE,
        table_name=TABLE_NAME,
        bucket_name=args.bucket,
        prefix=args.prefix,
        workgroup=WORKGROUP,
        )
    # Get results
    result = search_keys_in_range(
        database=DATABASE,
        table_name=TABLE_NAME,
        key_name=args.search_value,
        start_date=args.start_value,
        end_date=args.end_value,
        workgroup=WORKGROUP,
    )


