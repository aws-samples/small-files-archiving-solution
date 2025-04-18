#!/bin/bash
cmd_prefix="../apps/"
cmd="${cmd_prefix}/fss3-archiver.py"
cmd2="${cmd_prefix}/s3s3-archiver.py"
src_bucket="your-src-bucket"
dst_bucket="your-dest-bucket"
src_bucket_path="fs1/d0001"
src_path="/data/nfsshare/fs1/d0001"
dst_bucket_path="day20250114-3"
dst_path="dest_fs/"
input_file="input.txt"
storageclass="STANDARD_IA"

# fs to s3 by size
function fstos3_size () {
python3 $cmd \
    --src-path $src_path \
    --dst-bucket $dst_bucket \
    --dst-prefix $dst_bucket_path \
    --num-threads 10 \
    --max-size 100MB \
    --tar-storageclass $storageclass 
}

# fs to s3 by count
function fstos3_count () {
python3 $cmd \
    --src-path $src_path \
    --dst-bucket $dst_bucket \
    --dst-prefix $dst_bucket_path \
    --num-threads 4 \
    --max-files 10000
}
# fs to s3 by input file
function fstos3_input () {
python3 $cmd \
    --src-path $src_path \
    --dst-bucket $dst_bucket \
    --dst-prefix $dst_bucket_path \
    --num-threads 5 \
    --max-files 1000 \
    --input-file $input_file 
}

# fs to fs
# not support
#function fstofs () {
#python3 $cmd \
#    --src-type fs \
#    --dst-type fs \
#    --src-path $src_path \
#    --dst-path $dst_path \
#    --log-level INFO \
#    --max-process 4 \
#    --combine count \
#    --max-file-number 10000
#}
#

## s3 to fs
## not support
#function s3tofs () {
#python3 $cmd \
#    --src-type s3 \
#    --dst-type fs \
#    --src-bucket $src_bucket \
#    --src-path $src_bucket_path \
#    --dst-path $dst_path \
#    --log-level INFO \
#    --max-process 4 \
#    --combine count \
#    --max-file-number 100
#}

# s3 to s3 by size
function s3tos3_size () {
python3 $cmd2 \
    --src-bucket $src_bucket \
    --src-prefix $src_bucket_path \
    --dst-bucket $dst_bucket \
    --dst-prefix $dst_bucket_path \
    --num-threads 10 \
    --max-size 10MB \
    --tar-storageclass $storageclass 
}
#    --profile-name my-aws-profile 

# s3 to s3 by count 
function s3tos3_count () {
python3 $cmd2 \
    --src-bucket $src_bucket \
    --src-prefix $src_bucket_path \
    --dst-bucket $dst_bucket \
    --dst-prefix $dst_bucket_path \
    --num-threads 10 \
    --max-files 1000
    --tar-storageclass $storageclass 
}

# s3 to s3 by input  # not implemented
#function s3tos3_input () {
#python3 $cmd2 \
#    --src-bucket $src_bucket \
#    --src-path $src_bucket_path \
#    --dst-bucket $dst_bucket \
#    --dst-path $dst_bucket_path \
#    --log-level INFO \
#    --max-process 10 \
#    --max-files 10000 \
#    --input-file $input_file
#}

#fstos3_size # success
#fstos3_count # success
#fstos3_input #success
s3tos3_size # working but slow perf.
#s3tos3_count # working but slow perf.
###s3tos3_input # not supported
