import boto3
from botocore.config import Config
import io
import concurrent.futures
import logging
from botocore.exceptions import ClientError
import threading
from datetime import datetime
import tarfile

DELIMITER = '|'

class S32S3Archiver:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger(__name__)
        self.src_s3_client = self._get_s3_client(args.src_bucket)
        self.dst_s3_client = self._get_s3_client(args.dst_bucket)
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.tar_sequence = 1
        self.tar_sequence_lock = threading.Lock()
        self.total_files = 0
        self.failed_files = 0
        self.total_tar_files = 0
        self.total_manifest_files = 0
        self.src_prefix = args.src_path.rstrip('/') + '/' if args.src_path else ''
        self.dst_prefix = args.dst_path.rstrip('/') + '/' if args.dst_path else ''

    def _get_s3_client(self, bucket):
        session = boto3.Session(profile_name=self.args.profile_name)
        config = Config(
            retries={'max_attempts': 10, 'mode': 'adaptive'},
            max_pool_connections=100
        )
        return session.client('s3', config=config)

    def get_file_list(self):
        if self.args.input_file:
            return self._get_file_list_from_input_file()
        else:
            return self._get_file_list_from_s3()

    def _get_file_list_from_input_file(self):
        file_list = []
        with open(self.args.input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(',')
                    if len(parts) >= 2:
                        s3_key, obj_name = parts[:2]
                    else:
                        s3_key = parts[0]
                        obj_name = s3_key.split('/')[-1]
                    
                    try:
                        response = self.src_s3_client.head_object(Bucket=self.args.src_bucket, Key=s3_key)
                        file_size = response['ContentLength']
                        file_list.append((s3_key, obj_name, file_size))
                    except ClientError:
                        self.logger.warning(f"Object not found in S3: {s3_key}")
        return file_list

    def _get_file_list_from_s3(self):
        file_list = []
        paginator = self.src_s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.args.src_bucket, Prefix=self.src_prefix):
            for obj in page.get('Contents', []):
                s3_key = obj['Key']
                obj_name = s3_key[len(self.src_prefix):]
                file_size = obj['Size']
                file_list.append((s3_key, obj_name, file_size))
        return file_list

    def md5hash(self, s3_key):
        response = self.src_s3_client.head_object(Bucket=self.args.src_bucket, Key=s3_key)
        return response.get('ETag', '').strip('"')

    def create_tar_and_upload(self, file_list):
        with self.tar_sequence_lock:
            tar_name = f"archive_{self.current_time}_{self.tar_sequence:04d}.tar"
            manifest_name = f"manifest_{self.current_time}_{self.tar_sequence:04d}.csv"
            self.tar_sequence += 1

        tar_buffer = io.BytesIO()
        manifest_content = io.StringIO()

        with tarfile.open(fileobj=tar_buffer, mode='w|') as tar:
            for s3_key, obj_name, file_size in file_list:
                tar_cur_pos = tar_buffer.tell()
                
                # Stream the file from source S3 to tar
                obj_data = self.src_s3_client.get_object(Bucket=self.args.src_bucket, Key=s3_key)['Body']
                
                info = tarfile.TarInfo(name=obj_name)
                info.size = file_size
                tar.addfile(info, obj_data)
                
                tar_end_pos = tar_buffer.tell() - 1
                md5 = self.md5hash(s3_key)
                
                content_log = f"{self.dst_prefix}archives/{tar_name}{DELIMITER}{s3_key}{DELIMITER}"
                content_log += f"{datetime.now().strftime('%Y|%m|%d')}{DELIMITER}"
                content_log += f"{file_size}{DELIMITER}{tar_cur_pos}{DELIMITER}"
                content_log += f"{tar_end_pos}{DELIMITER}{md5}\n"
                
                manifest_content.write(content_log)

        self.total_files += len(file_list)
        self.total_tar_files += 1
        self.total_manifest_files += 1

        # Upload tar file using multipart upload
        tar_buffer.seek(0)
        self._multipart_upload(tar_buffer, f"{self.dst_prefix}archives/{tar_name}")

        # Upload manifest file
        manifest_buffer = io.BytesIO(manifest_content.getvalue().encode())
        self._upload_with_retry(manifest_buffer, f"{self.dst_prefix}manifests/{manifest_name}")

        return f"Uploaded {tar_name} and {manifest_name}"

    def _multipart_upload(self, file_obj, s3_key, part_size=16*1024*1024):
        mpu = self.dst_s3_client.create_multipart_upload(Bucket=self.args.dst_bucket, Key=s3_key)
        parts = []

        i = 1
        while True:
            data = file_obj.read(part_size)
            if not data:
                break
            part = self.dst_s3_client.upload_part(Body=data, Bucket=self.args.dst_bucket, Key=s3_key, UploadId=mpu['UploadId'], PartNumber=i)
            parts.append({"PartNumber": i, "ETag": part['ETag']})
            i += 1

        self.dst_s3_client.complete_multipart_upload(Bucket=self.args.dst_bucket, Key=s3_key, UploadId=mpu['UploadId'], MultipartUpload={"Parts": parts})

    def _upload_with_retry(self, file_obj, s3_key, max_retries=3):
        for attempt in range(max_retries):
            try:
                self.dst_s3_client.upload_fileobj(file_obj, self.args.dst_bucket, s3_key)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Upload failed, retrying... (attempt {attempt + 1})")
                file_obj.seek(0)

    def run(self):
        file_list = self.get_file_list()
        current_tar_files = []
        current_tar_size = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.max_process) as executor:
            future_to_tar = {}

            for file_info in file_list:
                _, _, file_size = file_info

                if self.args.combine == 'count' and len(current_tar_files) >= self.args.max_file_number:
                    future_to_tar[executor.submit(self.create_tar_and_upload, current_tar_files)] = len(current_tar_files)
                    current_tar_files = []
                    current_tar_size = 0
                elif self.args.combine == 'size' and current_tar_size + file_size > self.args.max_tarfile_size:
                    future_to_tar[executor.submit(self.create_tar_and_upload, current_tar_files)] = len(current_tar_files)
                    current_tar_files = []
                    current_tar_size = 0

                current_tar_files.append(file_info)
                current_tar_size += file_size

            if current_tar_files:
                future_to_tar[executor.submit(self.create_tar_and_upload, current_tar_files)] = len(current_tar_files)

            for future in concurrent.futures.as_completed(future_to_tar):
                num_files = future_to_tar[future]
                try:
                    result = future.result()
                    self.logger.info(result)
                except Exception as exc:
                    self.logger.error(f"Error processing {num_files} files: {exc}")
                    self.failed_files += num_files

        self.logger.info(f"Total files processed: {self.total_files}")
        self.logger.info(f"Failed files: {self.failed_files}")
        self.logger.info(f"Total tar files created: {self.total_tar_files}")
        self.logger.info(f"Total manifest files created: {self.total_manifest_files}")

