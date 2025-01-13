#!/bin/sh
cmd="s3archiver.py"
#
## from mountpoint-s3
# too low performance ㅜ.ㅜ

# running s3 by size 1GB
#python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1/' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --bucket_name 'your-own-dest-bucket'

# running s3 by count 10000
#python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1/' --combine count --max_file_number 10000 --max_process 10 --bucket_name 'your-own-dest-bucket'

## without prefix
python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1/' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --bucket_name 'your-own-dest-bucket'

## with prefix
#python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --bucket_name 'your-own-dest-bucket' --tar_prefix 'archive/day7' --manifest_prefix 'manifest/day7'

## with input file to s3
#python3 $cmd --protocol s3 --input_file 'input.txt' --combine count --max_file_number 10000 --max_process 10 --bucket_name 'your-own-dest-bucket'

## with input file to fs
#python3 $cmd --protocol fs --input_file 'input.txt' --combine count --max_file_number 10000 --max_process 10 --fs_dir '/mnt/s3/day3'

