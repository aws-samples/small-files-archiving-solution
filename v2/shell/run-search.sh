#!/bin/bash

program="apps/search.py"
bucket_name="your-dst-bucket"
prefix="day999/manifests"

# search by name
python3 $program --bucket $bucket_name --prefix $prefix --search_type name --search_value file0001

# search by date
#python3 $program --bucket $bucket_name --prefix $prefix --search_type date --search_value 2024-11-01 --end_value 2025-01-09

