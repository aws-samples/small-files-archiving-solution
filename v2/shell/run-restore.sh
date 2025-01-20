#!/bin/sh

cmd_prefix="../apps/"
cmd="${cmd_prefix}/restore.py"
python3 $cmd --bucket_name 'your-bucket' --key_name 'day1/archive_20240704_011925_ZPYK0J.tar' --start_byte '182784' --stop_byte '303615'
