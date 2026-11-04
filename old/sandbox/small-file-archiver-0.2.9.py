#!/usr/bin/env python3
'''
ChangeLogs
- 2022.10.07: 
    - create manifest before tar archiving
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

## treating arguments
parser = argparse.ArgumentParser()
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
parser.add_argument('--prefix_root', help='prefix root e) dir1/', action='store', default='')
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int)
parser.add_argument('--combine', help='size | count, if you combind files based on tarfile size, select \'size\', or if you combine files based on file count, select \'count\'', action='store', default='count', required=True)
parser.add_argument('--max_file_number', help='max files in one tarfile', action='store', default=1000, type=int)
parser.add_argument('--max_tarfile_size', help='NUM bytes e) $((1*(1024**3))) #1GB for < total 50GB, 10GB for >total 50GB', action='store', default=10*(1024**3), type=int)
parser.add_argument('--target_file_prefix', help='prefix of the target file we are creating into the snowball', action='store', default='')
parser.add_argument('--nfs_dir', help='specify nfs mounting point when protocol is nfs ', action='store', default='/nfs')
args = parser.parse_args()

prefix_list = args.src_dir  ## Don't forget to add last slash '/'
#prefix_root = args.prefix_root ## Don't forget to add last slash '/'
prefix_root = prefix_list ## Don't forget to add last slash '/'
##Common Variables
# max_process variable is to set concurrent processes count 
max_process = args.max_process
# combine variable is to set which machanism will be used to archive files, tarfile size or files count.
combine = args.combine
# if combind == size, max_tarfile_size should be set, elif combind == count, max_file_number should be set
max_tarfile_size = args.max_tarfile_size # 10GiB, 100GiB is max limit of snowballs
max_file_number = args.max_file_number # 10GiB, 100GiB is max limit of snowball
target_file_prefix = args.target_file_prefix
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR
mpu_max_concurrency = 10
#date_dir_list = prefix_list.split('/')[-2].split('-') ## 2022-07-04
# or
#data_dir_list = [x for x in prefix_list.split('/') if x][-3:] ## 2022/07/04
#year, month, day = date_dir_list
today = datetime.today()
year = str(today.year)
month= str(today.month)
day = str(today.day)
nfs_dir = args.nfs_dir
contents_dir = nfs_dir + '/' + year + '/' + month + '/' + day
contents_log_dir = contents_dir + '/' + 'list'
# end of user variables ## you don't need to modify below codes.

##### Optional variables
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
cmd='upload_sbe' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
# create log directory
try:
    os.makedirs(contents_log_dir)
except: 
    print('failed to create %s', contents_log_dir)
try:
    os.makedirs('list')
except: 
    print('failed to create list dir')
errorlog_file = 'list/error-%s.log' % current_time
successlog_file = 'list/success-%s.log' % current_time
quit_flag = 'DONE'
# End of Variables

if os.name == 'posix':
    multiprocessing.set_start_method("fork")

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
if not os.path.isdir(nfs_dir):
    print(nfs_dir + " does not exist")
    exit()

## create manifest file
def create_manifest(tar_name, org_files_list, manifest_name):
    delimeter = '|'
    with open(manifest_name, 'a') as manifest_log:
            for file_name, obj_name in org_files_list:
                    content_log = tar_name + delimeter \
                                + file_name + delimeter \
                                + year + delimeter \
                                + month + delimeter \
                                + day \
                                + '\n'
#                                + obj_name + delimeter
#                                + str(file_size)
                    manifest_log.write(content_log)

## code from snowball_uploader
def archive_to_fs(tar_name, org_files_list):
    tar_file_size = 0
    collected_files_no = 0
    success_log.info('%s is combining based on %s',tar_name, combine)
    manifest_name = contents_log_dir + '/' + tar_name + '-contents.csv'
    #print(manifest_name)
    # create manifest
    create_manifest(tar_name, org_files_list, manifest_name)
    # tar archiving
    tarfile_full_name = contents_dir + "/" + tar_name
    ## when using TARFILE module
    with tarfile.open(name=tarfile_full_name, mode='w:') as tar:
        #for file_name, obj_name, file_size in org_files_list:
        for file_name, obj_name in org_files_list:
            try:
                tar.add(file_name, arcname=obj_name)
                collected_files_no += 1
            except:
                error_log.info("%s is ignored" % file_name)
    success_log.info('%s is archived successfully\n' % tar_name)

    ## when using os.system
    #file_args = ''
    #for file, obj in org_files_list:
    #    file_args = file_args + ' ' + file
    ##subprocess.call(f'tar -cf {tarfile_full_name} {file_args}', shell=True)
    #os.system('tar -cf %s %s'%(tarfile_full_name, file_args))
    #org_files_list = []
    #success_log.info('%s is archived successfully\n' % tar_name)
    ##
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
    sum_files = 1
    if combine == 'size':
        size_or_num = sum_size
        maxNo = max_tarfile_size
    elif combine == 'count':
        size_or_num = sum_files
        maxNo = max_file_number
    else:
        print("combine is not proper, exiting")
        exit()
    org_files_list = []
   # get all files from given directory
    for r,d,f in os.walk(sub_prefix):
        for file in f:
            try:
                file_name = os.path.join(r,file)
                # support compatibility of MAC and windows
                #file_name = unicodedata.normalize('NFC', file_name)
                obj_name = conv_obj_name(file_name, prefix_root, sub_prefix)
                if combine == 'size':
                    f_size = os.stat(file_name).st_size
                    size_or_num = size_or_num + f_size
                else:
                    size_or_num += 1
                #file_info = (file_name, obj_name, f_size)
                file_info = (file_name, obj_name)
                org_files_list.append(file_info)
                #if max_tarfile_size < sum_size:
                if maxNo < size_or_num:
                    size_or_num = 1
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
        tar_name = ('%sarchive-%s-%s.tar' % (target_file_prefix, current_time, randchar))
        success_log.debug('receving mp_data size: %s'% len(org_files_list))
        success_log.debug('receving mp_data: %s'% org_files_list)
        if mp_data == quit_flag:
            break
        try:
            archive_to_fs(tar_name, org_files_list)
            #print('%s is uploaded' % tar_name)
        except Exception as e:
            error_log.info('exception error: %s uploading failed' % tar_name)
            error_log.info(e)
            traceback.print_exc()
        #return 0 ## for the dubug, it will pause with error

def upload_file_multi(src_dir):
    success_log.info('%s directory is archived' % src_dir)
    p_list = run_multip(max_process, upload_file, q)
    # get object list and ingest to processes
    num_obj = upload_get_files(src_dir, q)
    # sending quit_flag and join processes
    finishq(q, p_list)
    success_log.info('%s directory is archived' % src_dir)
    return num_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")

# upload log files to S3
def upload_log():

    log_files = [errorlog_file, successlog_file]
    for file in log_files:
        if os.path.isdir(contents_log_dir):
            shutil.copy(file, contents_dir + "/" + file)
def result_log(start_time, total_files, contents_dir):
    end_time = datetime.now()
    if combine == 'size':
        size_or_num = max_tarfile_size
    else:
        size_or_num = max_file_number
    success_log.info('====================================')
    success_log.info('Combine: %s' % combine)
    success_log.info('size or count: %s' % size_or_num)
    success_log.info('Duration: {}'.format(end_time - start_time))
    success_log.info('Scanned file numbers: %d' % total_files) 
    success_log.info('TAR files location: %s' % contents_dir)
    success_log.info('END')
    success_log.info('====================================')

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
        result_log(start_time, total_files, contents_dir)
        upload_log()
    else:
        s3_booster_help

    #print('====================================')
    ##for d in down_dir:
    ##    stored_dir = local_dir + d
    ##    print("[Information] Download completed, data stored in %s" % stored_dir)
    #print('Duration: {}'.format(end_time - start_time))
    #print('Scanned File numbers: %d' % total_files) 
    #print('End')
    #print('====================================')
