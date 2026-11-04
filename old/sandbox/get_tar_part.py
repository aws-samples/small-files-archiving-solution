import boto3
import tarfile
import io
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
tarf.extractall()
print(names)
