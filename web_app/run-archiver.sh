#!/bin/bash
src_bucket="your-own-src-bucket"
dst_bucket="your-own-dest-bucket"
src_bucket_path="fs1/d0050"
src_path="/data/fs1"
dst_path="day11/"
fs_dst_path="dest_fs/"
input_file="input.txt"

# fs to s3
function fstos3 () {
python3 archiver.py \
    --src-type fs \
    --dst-type s3 \
    --src-path $src_path \
    --dst-bucket $dst_bucket \
    --dst-path $dst_path \
    --log-level INFO \
    --max-process 4 \
    --combine size \
    --max-tarfile-size 1073741824
}

# fs to s3 by input file
function fstos3_input () {
python3 archiver.py \
    --src-type fs \
    --dst-type s3 \
    --input-file $input_file \
    --dst-bucket $dst_bucket \
    --dst-path $dst_path \
    --log-level INFO \
    --max-process 4 \
    --combine size\
    --max-tarfile-size 1073741
}

# fs to fs
function fstofs () {
python3 archiver.py \
    --src-type fs \
    --dst-type fs \
    --src-path $src_path \
    --dst-path $fs_dst_path \
    --log-level INFO \
    --max-process 4 \
    --combine count \
    --max-file-number 10000
}


# s3 to fs
function s3tofs () {
python3 archiver.py \
    --src-type s3 \
    --dst-type fs \
    --src-bucket $src_bucket \
    --src-path $src_bucket_path \
    --dst-path $fs_dst_path \
    --log-level INFO \
    --max-process 4 \
    --combine count \
    --max-file-number 100
}

# s3 to s3
function s3tos3 () {
python3 archiver.py \
    --src-type s3 \
    --dst-type s3 \
    --src-bucket $src_bucket \
    --src-path $src_bucket_path \
    --dst-bucket $dst_bucket \
    --dst-path $dst_path \
    --log-level INFO \
    --max-process 100 \
    --combine size \
    --max-tarfile-size 136870912
}
#    --profile-name my-aws-profile 

#fstos3 # success
#fstos3_input #success
#fstofs # success
#s3tos3 # working but slow perf.
s3tofs # success
