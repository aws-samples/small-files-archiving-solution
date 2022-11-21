#!/usr/bin/env python3
'''
ChangeLogs
- 2022.06.08:
  - added destination as nfs
  - when using nfs, snow-auto-extract don't be supported
- 2022.03.29:
  - added storage_class option
  - bug fix: error(index range) occured when 'prefix_root' is not specified
- 2022.03.23:
  - uploading log files(filelist, error, success) to S3
- 2022.03.23:
  - added target_file_prefix option
  - featured by "Marcos Diez", Thanks Marcos
- 2022.01.19:
  - added no_extract option
  - featured by "David Byte", Thanks David
- 2021.08.12:
  - using s3client.upload_fileobj instead of mpu_upload
  - improve upload performance, but more memory usage
- 2021.08.11:
  - adding compression argument and adjusting suffix "tgz"
  - compression feature is added by "Kirill Davydychev", Thanks Kirill
- 2021.08.10:
  - check source directory exist
  - handling argument with argparse
- 2021.08.09:
  - error handling, when tar can't archive a file with 'permission denied' error'
- 2021.08.05:
  - this utility will copy files from local filesystem to SnowballEdge in parallel
  - snowball_uploader alternative
- 2021.08.03:
  - support multiprocessing(spawn)
  - fixing windows path delimeter (\)
  - support compatibility of file name between MAC and Windows
- 2021.08.02:
  - adding logger
  - fixing error on python3.8, multiprocessing.set_start_method("fork")
    - https://github.com/pytest-dev/pytest-flask/issues/104
- 2021.08.01: adding uploader feature
- 2021.07.24:
- 2021.07.23: applying multiprocessing.queue + process instead of pool
- 2021.07.21: modified getObject function
  - for parallel processing, multiprocessing.Pool used
  - used bucket.all instead of paginator
- 2021.07.20: first created
'''

#requirement
## python 3.7+ (os.name)
## boto3
## preferred os: linux (Windows works as well, but performance is slower)

import os
#import boto3
#import botocore
import multiprocessing
from os import path, makedirs
from datetime import datetime, timezone
#from botocore.exceptions import ClientError
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
#from boto3.s3.transfer import TransferConfig

## treating arguments
parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', help='your bucket name e) your-bucket', action='store', required=False)
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
#parser.add_argument('--region', help='aws_region e) ap-northeast-2', action='store')
parser.add_argument('--endpoint', help='snowball endpoint e) http://10.10.10.10:8080 or https://s3.ap-northeast-2.amazonaws.com', action='store', default='https://s3.ap-northeast-2.amazonaws.com', required=False)
parser.add_argument('--profile_name', help='aws_profile_name e) sbe1', action='store', default='default')
parser.add_argument('--prefix_root', help='prefix root e) dir1/', action='store', default='')
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int)
parser.add_argument('--max_tarfile_size', help='NUM bytes e) $((1*(1024**3))) #1GB for < total 50GB, 10GB for >total 50GB', action='store', default=10*(1024**3), type=int)
parser.add_argument('--compression', help='specify gz to enable', action='store', default='')
parser.add_argument('--no_extract', help='yes|no; Do not set the autoextract flag', action='store', default='no')
parser.add_argument('--target_file_prefix', help='prefix of the target file we are creating into the snowball', action='store', default='')
parser.add_argument('--storage_class', help='specify S3 classes, be cautious Snowball support only STANDARD class; StorageClass=STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR', action='store', default='STANDARD')
parser.add_argument('--protocol', help='specify transferring protocol s3 or nfs ', action='store', default='s3')
parser.add_argument('--nfs_dir', help='specify nfs mounting point when protocol is nfs ', action='store', default='/nfs')
args = parser.parse_args()

prefix_list = args.src_dir  ## Don't forget to add last slash '/'
#prefix_root = args.prefix_root ## Don't forget to add last slash '/'
prefix_root = prefix_list ## Don't forget to add last slash '/'
##Common Variables
bucket_name = args.bucket_name
profile_name = args.profile_name
endpoint = args.endpoint
max_process = args.max_process
max_tarfile_size = args.max_tarfile_size # 10GiB, 100GiB is max limit of snowball
compression = args.compression # default for no compression, "gz" to enable
target_file_prefix = args.target_file_prefix
no_extract = args.no_extract # default for no compression, "gz" to enable
if args.no_extract == 'yes':
    no_extract = True
else:
    no_extract = False
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR
storage_class = args.storage_class ## value is fixed, snowball only transferred to STANDARD class
#storage_class = 'STANDARD' ## value is fixed, snowball only transferred to STANDARD class
# StorageClass='STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'
mpu_max_concurrency = 10
#transfer_config = TransferConfig(max_concurrency=mpu_max_concurrency)
protocol = args.protocol
date_dir_list = prefix_list.split('/')[-2].split('-')
year, month, day = date_dir_list
nfs_dir = args.nfs_dir
contents_dir = nfs_dir + '/' + year + '/' + month + '/' + day
contents_log_dir = contents_dir + '/' + 'list'
# end of user variables ## you don't need to modify below codes.

##### Optional variables
## begin of snowball_uploader variables
### unUsed options
#parser.add_argument('--max_part_size', help='NUM bytes e) $((100*(1024**2))) #100MB', action='store', default=100*(1024**2), type=int)
#max_part_size = args.max_part_size  # 100MB, 500MiB is max limit of snowball
#min_part_size = 5 * 1024 ** 2 # 16MiB for S3, 5MiB for SnowballEdge
#max_part_count = int(math.ceil(max_tarfile_size / max_part_size))
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
# CMD variables
cmd='upload_sbe' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
# create log directory
try:
    os.makedirs('list')
except: pass
errorlog_file = 'list/error-%s.log' % current_time
successlog_file = 'list/success-%s.log' % current_time
quit_flag = 'DONE'
# End of Variables

if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# S3 session
#s3_client = boto3.client('s3')
#session = boto3.Session(profile_name=profile_name)
#s3_client = session.client('s3', endpoint_url=endpoint)

# defining function
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

## check nfs_dir
if protocol == "nfs":
    if not os.path.isdir(nfs_dir):
        print(nfs_dir + " does not exist")
        exit()

## code from snowball_uploader
def copy_to_snowball(tar_name, org_files_list):
    delimeter = '|'
    tar_file_size = 0
    collected_files_no = 0
    success_log.info('%s is archiving',tar_name)
    try:
        os.makedirs(contents_log_dir)
    except: pass
    content_log_by_tar = contents_log_dir + '/' + tar_name + '-contents.csv'
    #print(content_log_by_tar)
    success_log.info('%s uploading',tar_name)
    tar_loc = contents_dir + "/" + tar_name
    with open(content_log_by_tar, 'w') as contentlist_log:
        with tarfile.open(name=tar_loc, mode='w:'+compression) as tar:
        #with tarfile.open(fileobj=recv_buf, mode='w:'+compression, compresslevel=1) as tar:
            for file_name, obj_name, file_size in org_files_list:
                try:
                    tar.add(file_name, arcname=obj_name)
                    collected_files_no += 1
#                                + obj_name + delimeter
#                                + str(file_size)
                    content_log = tar_name + delimeter \
                                + file_name + delimeter \
                                + year + delimeter \
                                + month + delimeter \
                                + day \
                                + '\n'
                    contentlist_log.write(content_log)
                except:
                    error_log.info("%s is ignored" % file_name)
    success_log.info('%s is uploaded successfully\n' % tar_name)
    #print('metadata info: %s\n' % str(meta_out))
    #print('%s is uploaded successfully\n' % tar_name)
    return collected_files_no
## end of code from snowball_uploader

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
    if len(prefix_root) == 0 :
        pass
    elif prefix_root[-1] != '/':
        prefix_root = prefix_root + '/'
    else:
        prefix_root = prefix_root
    if sub_prefix[-1] != '/':
        sub_prefix = sub_prefix + '/'
    if os.name == 'nt':
        obj_name = prefix_root + file_name.replace(sub_prefix,'',1).replace('\\', '/')
    else:
        obj_name = prefix_root + file_name.replace(sub_prefix,'',1)
    return obj_name

# get files to upload
def upload_get_files(sub_prefix, q):
    num_obj=0
    sum_size = 0
    org_files_list = []
   # get all files from given directory
    for r,d,f in os.walk(sub_prefix):
        for file in f:
            try:
                file_name = os.path.join(r,file)
                # support compatibility of MAC and windows
                #file_name = unicodedata.normalize('NFC', file_name)
                obj_name = conv_obj_name(file_name, prefix_root, sub_prefix)
                f_size = os.stat(file_name).st_size
                file_info = (file_name, obj_name, f_size)
                org_files_list.append(file_info)
                sum_size = sum_size + f_size
                if max_tarfile_size < sum_size:
                    sum_size = 0
                    mp_data = org_files_list
                    org_files_list = []
                    try:
                        # put files into queue in max_tarfile_size
                        q.put(mp_data)
                        success_log.debug('0, sending mp_data size: %s'% len(mp_data))
                        success_log.debug('0, sending mp_data: %s'% mp_data)
                    except Exception as e:
                        error_log.info('exception error: putting %s into queue is failed' % file_name)
                        error_log.info(e)
                num_obj+=1
            except Exception as e:
                error_log.info('exception error: getting %s file info is failed' % file_name)
                error_log.info(e)
            #time.sleep(0.1)
    try:
        # put remained files into queue
        mp_data = org_files_list
        q.put(mp_data)
        success_log.debug('1, sending mp_data size: %s'% len(mp_data))
        success_log.debug('1, sending mp_data: %s'% mp_data)
    except Exception as e:
        error_log.info('exception error: putting %s into queue is failed' % file_name)
        error_log.info(e)
    return num_obj

def upload_file(q):
    global target_file_prefix
    while True:
        mp_data = q.get()
        org_files_list = mp_data
        randchar = str(gen_rand_char())
        if compression == '':
            tar_name = ('%sarchive-%s-%s.tar' % (target_file_prefix, current_time, randchar))
        elif compression == 'gz':
            tar_name = ('%sarchive-%s-%s.tgz' % (target_file_prefix, current_time, randchar))
        success_log.debug('receving mp_data size: %s'% len(org_files_list))
        success_log.debug('receving mp_data: %s'% org_files_list)
        if mp_data == quit_flag:
            break
        try:
            copy_to_snowball(tar_name, org_files_list)
            #print('%s is uploaded' % tar_name)
        except Exception as e:
            error_log.info('exception error: %s uploading failed' % tar_name)
            error_log.info(e)
            traceback.print_exc()
        #return 0 ## for the dubug, it will pause with error

def upload_file_multi(src_dir):
    success_log.info('%s directory is uploading' % src_dir)
    p_list = run_multip(max_process, upload_file, q)
    # get object list and ingest to processes
    num_obj = upload_get_files(src_dir, q)
    # sending quit_flag and join processes
    finishq(q, p_list)
    success_log.info('%s directory is uploaded' % src_dir)
    return num_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")

# upload log files to S3
def upload_log():

    log_files = [errorlog_file, successlog_file]
    for file in log_files:
        if protocol == "s3":
            s3_client.upload_file(file, bucket_name, file)
        elif protocol == "nfs":
            if os.path.isdir(contents_log_dir):
                #os.makedirs('log')
                shutil.copy(file, contents_dir + "/" + file)
        else:
            print("protocol is not specified")

# start main function
if __name__ == '__main__':

    # define simple queue
    #q = multiprocessing.Queue()
    q = multiprocessing.Manager().Queue()
    start_time = datetime.now()
    success_log.info("starting script..."+str(start_time))
    src_dir = prefix_list
    check_srcdir(src_dir)

    if cmd == 'upload_sbe':
        total_files = upload_file_multi(src_dir)
        upload_log()
    else:
        s3_booster_help

    end_time = datetime.now()
    success_log.info('====================================')
    success_log.info('Duration: {}'.format(end_time - start_time))
    success_log.info('Scanned file numbers: %d' % total_files) 
    success_log.info('TAR files location: %s' % contents_dir)
    success_log.info('END')
    success_log.info('====================================')
    print(year, month, day)
    #print('====================================')
    ##for d in down_dir:
    ##    stored_dir = local_dir + d
    ##    print("[Information] Download completed, data stored in %s" % stored_dir)
    #print('Duration: {}'.format(end_time - start_time))
    #print('Scanned File numbers: %d' % total_files) 
    #print('S3 Endpoint: %s' % endpoint)
    #print('End')
    #print('====================================')
