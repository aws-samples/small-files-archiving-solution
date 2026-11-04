import boto3
import tarfile
import io


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
s3 = boto3.client('s3')

bucket_name = 'your-own-dest-seoul'
key_name = 'archive-20230426_054326-TI7JNA.tar'

start_byte = 9216
stop_byte = 170495
#start_byte = 512
#stop_byte = 1024
resp = s3.get_object(Bucket=bucket_name, Key=key_name, Range='bytes={}-{}'.format(start_byte, stop_byte))
content = resp['Body'].read()
#with open(fn,'wb') as file:
#    content = resp['Body'].read()
#    file.write(content)
contentObj = io.BytesIO(content)
tarf = tarfile.open(fileobj=contentObj)
names = tarf.getnames()
safe_extractall(tarf)
print(names)
