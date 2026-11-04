import boto3
import tarfile
import io
import random
import string
import argparse


def safe_extractall(tarf, path="."):
    """extractall without tar path traversal: the 'data' filter where Python has
    it (3.12+, and recent 3.8-3.11 patch releases); otherwise refuse absolute
    paths, '..', links and device files before extracting."""
    if hasattr(tarfile, "data_filter"):
        tarf.extractall(path=path, filter="data")
        return
    import os
    root = os.path.realpath(path)
    for m in tarf.getmembers():
        target = os.path.realpath(os.path.join(root, m.name))
        if not (target == root or target.startswith(root + os.sep)) or not (m.isfile() or m.isdir()):
            raise ValueError(f"unsafe tar member refused: {m.name!r}")
    tarf.extractall(path=path)

parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', help='s3 bucket name', action='store', required=True)
parser.add_argument('--key_name', help='tarfile in S3', action='store', required=True)
parser.add_argument('--start_byte', help='first block of subset file', action='store', required=True)
parser.add_argument('--stop_byte', help='last block of subset file', action='store', required=True)
args = parser.parse_args()

bucket_name = args.bucket_name
key_name = args.key_name
start_byte = int(args.start_byte)
stop_byte = int(args.stop_byte)
extract_path = "restored_data"

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set*6, 6)))

s3 = boto3.client('s3')
resp = s3.get_object(Bucket=bucket_name, Key=key_name, Range='bytes={}-{}'.format(start_byte, stop_byte))
content = resp['Body'].read()
contentObj = io.BytesIO(content)
tarf = tarfile.open(fileobj=contentObj)
rand_char = str(gen_rand_char())

temp_tarfile = 'temp_tarfile-%s.tar' % rand_char

try:
    names = tarf.getnames()
    safe_extractall(tarf, path=extract_path)
    print(names)
except Exception as e:
    print(e)
    print('Warning: \n \
          Incompleted tar block is detected, \n \
          but temp_tarfile is generated, \n \
          you could recover some of files from temp_tarfile')
    print('temp tarfile: %s' % temp_tarfile)
    with open(temp_tarfile, 'wb') as outfile:
        outfile.write(contentObj.getbuffer())
