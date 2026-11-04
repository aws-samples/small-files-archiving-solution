import boto3
import time
import csv
import get_file_from_athena
import json
from datetime import datetime
import os

# Input parameter #1 : Image file name
file_name = input("Image file to download(ex. 374689.jpg) : ")

# Input parameter #2 : Image created date
# yyyymmdd = input("Image creation date(ex. 2022/07/11) : ")

region = 'ap-northeast-2'
bucket_name = 'your-own-dest-repo'
dest_dir_name = '/recovery/'

params = {
    'region': 'ap-northeast-2',
    'database': 'images_repo',
    'bucket': 'athena-bucket',
    'path': 'from-seoul',
    'query': 'select compress_file,year||\'/\'||month||\'/\'||day yyyymmdd from es_image_archive_list_v where file_name = ?',
    'file_name' : file_name
}
# Sample athena query result
#   compress_file   file_name   year    month   day
#   archive-20220711_074927-AVC2WA.tar  3355457.jpg 2022    07  10
#   archive-20220711_074927-AVC2WA.tar  3355458.jpg 2022    07  10

session = boto3.Session(profile_name='sess1')

query_start_time = datetime.now()
# Execute query and get the compress_file name 
data = get_file_from_athena.query_results(session, params)
query_end_time = datetime.now()

if data == None:
    print("There is no matched compression file with %s" % file_name)
    exit()

#print(data)

s3_client = boto3.client('s3', region)

# download function
def downfiles(bucket_name, yyyymmdd, archived_file, download_file):
    os.makedirs(dest_dir_name+yyyymmdd,exist_ok=True)
    s3_client.download_file(bucket_name, archived_file, download_file)
    print("Download completed.")

download_start_time = datetime.now()
compress_file = data[0]
yyyymmdd = data[1]

archived_file = yyyymmdd+'/'+compress_file #ex.2022/07/11/archive-20220711_094307-G2M26O.tar
download_file = dest_dir_name+archived_file    #ex./recovery/2022/07/11/archive-20220711_094307-G2M26O.tar

print('\n')
print("Compressed file : s3://%s/%s" %(bucket_name, archived_file))
print("Download location : %s" % download_file)

downfiles(bucket_name, yyyymmdd, archived_file, download_file)

download_end_time = datetime.now()

print('\n')
print('Query Duration: {}'.format(query_end_time - query_start_time))
print('Download Duration: {}'.format(download_end_time - download_start_time))    
