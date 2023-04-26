#!/bin/sh
cmd="small-file-archiver-20230424-1.py"
#
# running s3 size
python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --bucket_name 'your-own-dest-seoul'
#--fs_dir '/data2/dest'
#
# running s3 count
#python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1' --combine count --max_file_number 10000 --max_process 10 --bucket_name 'your-own-dest-seoul'
#
# running fs size
#python3 $cmd --protocol fs --src_dir '/data/nfsshare/fs1' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --fs_dir '/data2/dest'

# running fs count
#python3 $cmd --protocol fs --src_dir '/data/nfsshare/fs1' --combine count --max_file_number 10000 --max_process 10 --fs_dir '/data2/dest'
