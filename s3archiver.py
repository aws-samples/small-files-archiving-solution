#!/usr/bin/env python3
'''
ChangeLogs
- 2024.02.22:
    - supporting input file as source
- 2023.09.27:
    - supporting input_file (not completed)
- 2023.09.27:
    - create function: create_manifest_tarfile()
- 2023.04.24:
    - add feature to upload s3 directly
    - adding byte offset
'''

#requirement
## python 3.7+ (os.name)
## boto3
## preferred os: linux (Windows works as well, but performance is slower)

import os
import subprocess
import multiprocessing
from os import path, makedirs
from datetime import datetime, timezone
import logging
import time
import unicodedata
import random
import string
import math
import io
import tarfile
import traceback
import argparse
import shutil
import hashlib
import boto3
from boto3.s3.transfer import TransferConfig

## treating arguments
### common parameters
parser = argparse.ArgumentParser()
parser.add_argument('--src_dir', help='source directory e) /data/dir1/ , you can select src_dir or input_file argument', action='store', required=False)
parser.add_argument('--protocol', help='specify the protocol to use, s3 or fs ', action='store', default='s3', required=True)
parser.add_argument('--prefix_root', help='prefix root e) dir1/', action='store', default='')
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int)
parser.add_argument('--combine', help='size | count, if you combind files based on tarfile size, select \'size\', or if you combine files based on file count, select \'count\'', action='store', default='count', required=True)
parser.add_argument('--max_file_number', help='max files in one tarfile', action='store', default=1000, type=int)
parser.add_argument('--max_tarfile_size', help='NUM bytes e) $((1*(1024**3))) #1GB for < total 50GB, 10GB for >total 50GB', action='store', default=10*(1024**3), type=int)
## for s3 protocol
parser.add_argument('--bucket_name', help='your bucket name e) your-bucket', action='store', required=False)
parser.add_argument('--endpoint', help='snowball endpoint e) http://10.10.10.10:8080 or https://s3.ap-northeast-2.amazonaws.com', action='store', default='https://s3.ap-northeast-2.amazonaws.com', required=False)
parser.add_argument('--profile_name', help='aws_profile_name e) sbe1', action='store', default='default')
parser.add_argument('--storage_class', help='specify S3 classes, be cautious Snowball support only STANDARD class; StorageClass=STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR', action='store', default='STANDARD')
parser.add_argument('--bucket_prefix', help='prefix of tarfile in the bucket', action='store', default='')
### for fs protocol
parser.add_argument('--fs_dir', help='specify fs mounting point when protocol is fs ', action='store', default='/mnt/fs')
### for input_file
parser.add_argument('--input_file', help='specify input_file instead of scanning filesystem, line seperated', action='store', default='')
## end of argments
args = parser.parse_args()

## set to variable
src_dir = args.src_dir  ## Don't forget to add last slash '/'
prefix_root = src_dir ## Don't forget to add last slash '/'
#prefix_root = args.prefix_root ## Don't forget to add last slash '/'
#prefix_root = prefix_list ## Don't forget to add last slash '/'
##Common Variables
# max_process variable is to set concurrent processes count 
max_process = args.max_process
# combine variable is to set which machanism will be used to archive files, tarfile size or files count.
combine = args.combine
# if combind == size, max_tarfile_size should be set, elif combind == count, max_file_number should be set
max_tarfile_size = args.max_tarfile_size # 10GiB, 100GiB is max limit of snowballs
max_file_number = args.max_file_number # 10GiB, 100GiB is max limit of snowball
bucket_prefix = args.bucket_prefix
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR
input_file = args.input_file

# S3 TransferConfig
KB = 1024
MB = KB * KB
mpu_max_concurrency = 10
multipart_chunksize = 16 * MB
transfer_config = TransferConfig(max_concurrency=mpu_max_concurrency, multipart_chunksize = multipart_chunksize)

protocol = args.protocol
bucket_name = args.bucket_name
collected_files_no = 0

# start of functions
def bucket_path_parsing(prefix_path):
# remove '/' at begining and end of bucket_path
    if prefix_path:
        if prefix_path[0] == '/':
            new_path = prefix_path[1:]
        if prefix_path[-1] == '/':
            new_path = prefix_path[:-2]
        return new_path
    else:
        return prefix_path
bucket_prefix = bucket_path_parsing(bucket_prefix)
endpoint = args.endpoint
profile_name = args.profile_name
storage_class = args.storage_class
#date_dir_list = prefix_list.split('/')[-2].split('-') ## 2022-07-04
# or
#data_dir_list = [x for x in prefix_list.split('/') if x][-3:] ## 2022/07/04
#year, month, day = date_dir_list
today = datetime.today()
year = str(today.year)
month= str(today.month)
day = str(today.day)
fs_dir = args.fs_dir
root_log_dir = 'logs'
log_dir = 'lists'
if protocol == 's3':
    contents_dir = bucket_prefix
    contents_log_dir = root_log_dir + '/' + contents_dir + '/' + log_dir 
    if contents_dir:
        contents_log_bucket = contents_dir + '/' + log_dir 
    else:
        contents_log_bucket = log_dir
elif protocol == 'fs':
    contents_dir = fs_dir 
    contents_log_dir = contents_dir + '/' + log_dir 
# end of user variables ## you don't need to modify below codes.

##### Optional variables
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
#cmd='upload_sbe' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
# create log directory
try:
    os.makedirs(contents_log_dir)
except: 
    print('ignoring to create ', contents_log_dir)
try:
    os.makedirs(root_log_dir)
except: 
    pass

errorlog_file = '%s/error-%s.log' % (root_log_dir, current_time)
successlog_file = '%s/success-%s.log' % (root_log_dir, current_time)
quit_flag = 'DONE'
# End of Variables

# S3 session
#s3_client = boto3.client('s3')
session = boto3.Session(profile_name=profile_name)
s3_client = session.client('s3', endpoint_url=endpoint)

if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# defining function
## create md5hash 
def md5hash(filename):
    md5_hash = hashlib.md5()
    with open(filename,"rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            md5_hash.update(byte_block)
        readable_hash = md5_hash.hexdigest()
    return readable_hash

## setup logger
def setup_logger(logger_name, log_file, level=logging.INFO, sHandler=False):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    if sHandler:
        l.addHandler(streamHandler)
    else:
        pass

## define logger
setup_logger('error', errorlog_file, level=log_level, sHandler=True)
setup_logger('success', successlog_file, level=log_level, sHandler=True)
error_log = logging.getLogger('error')
success_log = logging.getLogger('success')

## check fs_dir
if protocol == 'fs':
    if not os.path.isdir(fs_dir):
        print(fs_dir + " does not exist")
        exit()

## create manifest file and tarfile
def create_manifest_and_tarfile(tar_name, org_files_list, manifest_name, recv_buf):
    global bucket_prefix
    global collected_files_no
    delimeter = '|'
    content_log = ''
    success_log.info('%s is combining based on %s',tar_name, combine)
    delimeter = '|'
    tarfile_full_name = contents_dir + "/" + tar_name
    if protocol == 'fs':
        bucket_prefix = fs_dir
    with open(manifest_name, 'a') as manifest_log:
        with tarfile.open(fileobj=recv_buf, mode='w:') as tar:
            for file_name, obj_name, file_size in org_files_list:
                try:
                    # adding manifest info
                    tar_cur_pos = recv_buf.tell()
                    md5 = md5hash(file_name)
                    content_log = bucket_prefix + tar_name + delimeter \
                                + file_name + delimeter \
                                + year + delimeter \
                                + month + delimeter \
                                + day + delimeter \
                                + str(file_size) + delimeter \
                                + str(tar_cur_pos) + delimeter \
                    # perform TAR operation
                    tar.add(file_name, arcname=obj_name)
                    collected_files_no += 1
                    # adding manifest info
                    tar_end_pos = recv_buf.tell() - 1
                    content_log = content_log + str(tar_end_pos) + delimeter + md5 +'\n'
                except Exception as e:
                    error_log.info("%s is ignored" % file_name)
                    error_log.info(e)
                manifest_log.write(content_log)
        if protocol == 'fs':
            with open(tarfile_full_name, 'wb') as outfile:
                outfile.write(recv_buf.getbuffer())
    return collected_files_no

## upload to S3
def copy_to_s3(tar_name, org_files_list):
    recv_buf = io.BytesIO()
    manifest_name = contents_log_dir + '/' + tar_name + '-contents.csv'
    manifest_key = contents_log_bucket + '/' + tar_name + '-contents.csv'
    success_log.info('%s is archiving',tar_name)
    collected_files_no = create_manifest_and_tarfile(tar_name, org_files_list, manifest_name, recv_buf)
    recv_buf.seek(0)
    success_log.info('%s uploading',tar_name)
    # uploading to S3
    tar_full_name = bucket_prefix + tar_name
    s3_client.upload_fileobj(recv_buf, bucket_name, tar_full_name, ExtraArgs={'Metadata': {'archived': 'true'},'StorageClass': storage_class}, Config=transfer_config)
    s3_client.upload_file(manifest_name, bucket_name, manifest_key, ExtraArgs={'Metadata': {'archived': 'true'},'StorageClass': 'STANDARD'}, Config=transfer_config)
    ### print metadata
    meta_out = s3_client.head_object(Bucket=bucket_name, Key=tar_full_name)
    success_log.info('meta info: %s ',str(meta_out))
    success_log.info('%s is uploaded successfully\n' % tar_full_name)
    #print('metadata info: %s\n' % str(meta_out))
    #print('%s is uploaded successfully\n' % tar_name)
    return collected_files_no
## end of uploading to S3

## save to file system
def archive_to_fs(tar_name, org_files_list):
    recv_buf = io.BytesIO()
    success_log.info('%s is combining based on %s',tar_name, combine)
    manifest_name = contents_log_dir + '/' + tar_name + '-contents.csv'
    collected_files_no = create_manifest_and_tarfile(tar_name, org_files_list, manifest_name, recv_buf)
    success_log.info('%s is archived successfully\n' % tar_name)
    return collected_files_no
## end of saving to filesystem

# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set*6, 6)))
# execute multiprocessing
def run_multip(max_process, exec_func, q):
    p_list = []
    for i in range(max_process):
        p = multiprocessing.Process(target = exec_func, args=(q,))
        p_list.append(p)
        p.daemon = True
        p.start()
    return p_list

def finishq(q, p_list):
    for j in range(max_process):
        q.put(quit_flag)
    for pi in p_list:
        pi.join()

def conv_obj_name(file_name, prefix_root, sub_prefix):
    if prefix_root is None:
       prefix_root = '' 
    elif len(prefix_root) == 0 :
        pass
    elif prefix_root[-1] != '/':
        prefix_root = prefix_root + '/'
    else:
        prefix_root = prefix_root

    if len(sub_prefix) == 0:
        pass
    elif sub_prefix[-1] != '/':
        sub_prefix = sub_prefix + '/'
    else:
        pass

    if os.name == 'nt':
        obj_name = prefix_root + file_name.replace(sub_prefix,'',1).replace('\\', '/')
    else:
        obj_name = prefix_root + file_name.replace(sub_prefix,'',1)
    return obj_name

## get files to upload
def process_file(sub_prefix, file_path, combine, size_or_num, maxNo, q, org_files_list, num_obj):
    try:
        obj_name = conv_obj_name(file_path, prefix_root, sub_prefix)
        f_size = os.stat(file_path).st_size

        if combine == 'size':
            size_or_num += f_size
        else:
            size_or_num += 1

        file_info = (file_path, obj_name, f_size)
        org_files_list.append(file_info)

        if maxNo < size_or_num:
            size_or_num = 1
            mp_data = org_files_list
            org_files_list = []

            try:
                q.put(mp_data)
                success_log.debug('0, sending mp_data size: %s' % len(mp_data))
                success_log.debug('0, sending mp_data: %s' % mp_data)
            except Exception as e:
                log_exception(file_path, e)

        num_obj += 1
    except Exception as e:
        log_exception(file_path, e)
    return (size_or_num, num_obj, org_files_list)

def process_files_in_directory(sub_prefix, combine, size_or_num, maxNo, q, num_obj):
    org_files_list = []
    for root, dirs, files in os.walk(sub_prefix):
        for file in files:
            file_path = os.path.join(root, file)
            size_or_num, num_obj, org_files_list = process_file(sub_prefix, file_path, combine, size_or_num, maxNo, q, org_files_list, num_obj)
    return (num_obj, org_files_list)

def ingest_files_from_input_file(input_file, sub_prefix, combine, size_or_num, maxNo, q, num_obj):
    sub_prefix = ''
    org_files_list = []
    with open(input_file, 'r') as file:
        for line in file:
            file_path = line.strip()
            size_or_num, num_obj, org_files_list = process_file(sub_prefix, file_path, combine, size_or_num, maxNo, q, org_files_list, num_obj)
    return (num_obj, org_files_list)

def get_files(sub_prefix, combine, q, input_file=None):
    num_obj = 0
    size_or_num = 0
    maxNo = 0

    if combine == 'size':
        maxNo = max_tarfile_size
    elif combine == 'count':
        maxNo = max_file_number
    else:
        print("combine is not proper, exiting")
        exit()

    if input_file:
        success_log.debug('input file name: %s' % input_file)
        total_num_obj, org_files_list = ingest_files_from_input_file(input_file, sub_prefix, combine, size_or_num, maxNo, q, num_obj)
    else:
        total_num_obj, org_files_list = process_files_in_directory(sub_prefix, combine, size_or_num, maxNo, q, num_obj)

    try:
        mp_data = org_files_list
        q.put(mp_data)
        success_log.debug('1, sending mp_data size: %s' % len(mp_data))
        success_log.debug('1, sending mp_data: %s' % mp_data)
    except Exception as e:
        log_exception(file_path, e)
    return total_num_obj

def log_exception(file_path, exception):
    error_log.info(f'exception error: processing {file_path} failed')
    error_log.info(exception)

## upload file 
def upload_file(q):
    global bucket_prefix
    if bucket_prefix:
        bucket_prefix = bucket_prefix + '/'
    while True:
        mp_data = q.get()
        org_files_list = mp_data
        randchar = str(gen_rand_char())
        #tar_name = ('%sarchive_%s_%s.tar' % (bucket_prefix, current_time, randchar))
        tar_name = ('archive_%s_%s.tar' % (current_time, randchar))
        success_log.debug('receving mp_data size: %s'% len(org_files_list))
        success_log.debug('receving mp_data: %s'% org_files_list)
        if mp_data == quit_flag:
            break
        # debug
        #if org_files_list:
        try:
            if protocol == 's3':
                copy_to_s3(tar_name, org_files_list)
            elif protocol == 'fs':
                archive_to_fs(tar_name, org_files_list)
            else:
                error_log.info('invalid protocol, input s3 or fs')
                exit(100)
        except Exception as e:
            error_log.info('exception error: %s uploading failed' % tar_name)
            error_log.info(e)
            traceback.print_exc()
        # end of debug
        #return 0 ## for the dubug, it will pause with error

def upload_file_multi(src_dir, q):
    success_log.info('%s directory will be transferred' % src_dir)
    p_list = run_multip(max_process, upload_file, q)
    # get object list and ingest to processes
    num_obj = get_files(src_dir, combine, q, input_file)

    # sending quit_flag and join processes
    finishq(q, p_list)
    success_log.info('%s directory is archived' % src_dir)
    return num_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")

# upload log files to S3
def upload_log():
    if protocol == 's3':
        log_files = [errorlog_file, successlog_file]
        for file in log_files:
            if os.path.isdir(contents_log_dir):
                key_name = file
    elif protocol == 'fs':
        log_files = [errorlog_file, successlog_file]
        for file in log_files:
            if os.path.isdir(contents_log_dir):
                shutil.copy(file, contents_dir + "/" + file)
    else:
        error_log.info("Error: uploading csv and log files failed")

def result_log(start_time, total_files, contents_dir):
    end_time = datetime.now()
    if combine == 'size':
        size_or_num = max_tarfile_size
    else:
        size_or_num = max_file_number

    if protocol == 's3':
        dest_location = bucket_name
    elif protocol == 'fs':
        dest_location = contents_dir
    else:
        error_log.info('dest_location is not defined')
        
    success_log.info('====================================')
    success_log.info('Combine: {}'.format(combine))
    #success_log.info('size(bytes) or count: %d' % size_or_num)
    success_log.info('size(bytes) or count: {:,}'.format(size_or_num))
    success_log.info('Duration: {}'.format(end_time - start_time))
    success_log.info('Scanned file numbers: {:,}'.format(total_files))
    success_log.info('TAR files location: {}'.format(dest_location))
    success_log.info('END')
    success_log.info('====================================')

# start main function
if __name__ == '__main__':
    # define simple queue
    q = multiprocessing.Manager().Queue()
    start_time = datetime.now()
    success_log.info("starting script..."+str(start_time))

    total_files = upload_file_multi(src_dir,q)
    result_log(start_time, total_files, contents_dir)
    print("debug4: collect: %d" % collected_files_no )
    #upload_log()
