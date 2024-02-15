#!/bin/sh
cmd="s3archiver.py"
#
# running s3 by size
## without bucket_prefix
python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --bucket_name 'your-own-dest-repo'

## with bucket_prefix
#python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1' --combine size --max_tarfile_size $((500*(1024**2))) --max_process 10 --bucket_name 'your-own-dest-repo' --bucket_prefix '/day1'

# running s3 by count
#python3 $cmd --protocol s3 --src_dir '/data/nfsshare/fs1' --combine count --max_file_number 10000 --max_process 10 --bucket_name 'your-own-dest-repo' --bucket_prefix '/day2'

# running fs by size
#python3 $cmd --protocol fs --src_dir '/data/nfsshare/fs1' --combine size --max_tarfile_size $((1*(1024**3))) --max_process 10 --fs_dir '/data2/dest'

# running fs by count
#python3 $cmd --protocol fs --src_dir '/data/nfsshare/fs1' --combine count --max_file_number 10000 --max_process 10 --fs_dir '/data2/dest'
