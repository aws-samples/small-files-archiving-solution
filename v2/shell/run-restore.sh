#!/bin/sh

cmd="apps/restore.py"
python3 $cmd --bucket_name 'your-dst-bucket' --key_name 'day1/archive_20240704_011925_ZPYK0J.tar' --start_byte '182784' --stop_byte '303615'
