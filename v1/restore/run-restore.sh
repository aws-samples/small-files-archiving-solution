#!/bin/sh

cmd='get_tar_part.py'
python3 $cmd --bucket_name 'your-own-dest-bucket' --key_name 'day1/archive_20240704_011925_ZPYK0J.tar' --start_byte '182784' --stop_byte '303615'
