#!/bin/bash

program="search_item.py"
bucket_name="your-bucket"
prefix="d1000/manifests/"

# search by name
#python3 $program --bucket $bucket_name --prefix $prefix --search_type name --search_value file0001

# search by date
python3 $program --bucket $bucket_name --prefix $prefix --search_type date --search_value 2024-11-01 --end_value 2024-11-02

