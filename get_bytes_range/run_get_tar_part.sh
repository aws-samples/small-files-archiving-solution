#!/bin/sh

cmd='get_tar_part.py'
python3 $cmd --bucket_name 'your-own-dest-repo' --key_name 'archive_20230501_110237_36WP7R.tar' --start_byte '2056192' --stop_byte '2113534'
