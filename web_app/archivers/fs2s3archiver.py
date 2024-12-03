import os
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import tarfile
from datetime import datetime
import io
import concurrent.futures
import logging
import time
from botocore.exceptions import ClientError
import hashlib
import threading

DELIMITER = '|'

class FS2S3Archiver:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger(__name__)
        self.s3_client = self._get_s3_client()
        self.transfer_config = TransferConfig(
            max_concurrency=50,
            multipart_chunksize=16 * 1024 * 1024
        )
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.tar_sequence = 1
        self.tar_sequence_lock = threading.Lock()
        self.total_files = 0
        self.failed_files = 0
        self.total_tar_files = 0
        self.total_manifest_files = 0
        self.dst_prefix = args.dst_path.rstrip('/') + '/' if args.dst_path else ''

    def _get_s3_client(self):
        session = boto3.Session(profile_name=self.args.profile_name)
        #config = Config(
        #    retries={
        #        'max_attempts': 10,
        #        'mode': 'adaptive'
        #    },
        #    max_pool_connections=50
        #)
        #return session.client('s3', config=config)
        return session.client('s3')

    def get_file_list(self):
        if self.args.input_file:
            return self._get_file_list_from_input_file()
        else:
            return self._get_file_list_from_directory()

    def _get_file_list_from_input_file(self):
        file_list = []
        with open(self.args.input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(',')
                    if len(parts) >= 2:
                        full_path, obj_name = parts[:2]
                    else:
                        full_path = parts[0]
                        obj_name = os.path.basename(full_path)
                    
                    if os.path.exists(full_path):
                        file_size = os.path.getsize(full_path)
                        file_list.append((full_path, obj_name, file_size))
                    else:
                        self.logger.warning(f"File not found: {full_path}")
        return file_list

    def _get_file_list_from_directory(self):
        file_list = []
        for root, _, files in os.walk(self.args.src_path):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, self.args.src_path)
                file_size = os.path.getsize(full_path)
                file_list.append((full_path, rel_path, file_size))
        return file_list

    @staticmethod
    def md5hash(filename):
        md5_hash = hashlib.md5()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()

    def create_tar_and_upload(self, file_list):
        with self.tar_sequence_lock:
            tar_name = f"archive_{self.current_time}_{self.tar_sequence:08d}.tar"
            manifest_name = f"manifest_{self.current_time}_{self.tar_sequence:08d}.csv"
            self.tar_sequence += 1

        tar_buffer = io.BytesIO()
        manifest_content = io.StringIO()

        with tarfile.open(fileobj=tar_buffer, mode='w:') as tar:
            for file_name, obj_name, file_size in file_list:
                tar_cur_pos = tar_buffer.tell()
                tar.add(file_name, arcname=obj_name)
                tar_end_pos = tar_buffer.tell() - 1
                md5 = self.md5hash(file_name)
                content_log = f"{self.dst_prefix}archives/{tar_name}{DELIMITER}{file_name}{DELIMITER}"
                content_log += f"{datetime.now().strftime('%Y|%m|%d')}{DELIMITER}"
                content_log += f"{file_size}{DELIMITER}{tar_cur_pos}{DELIMITER}"
                content_log += f"{tar_end_pos}{DELIMITER}{md5}\n"
                manifest_content.write(content_log)

        self.total_files += len(file_list)
        self.total_tar_files += 1
        self.total_manifest_files += 1

        # Upload tar file with retry
        self._upload_with_retry(tar_buffer, f"{self.dst_prefix}archives/{tar_name}")

        # Upload manifest file with retry
        manifest_buffer = io.BytesIO(manifest_content.getvalue().encode())
        self._upload_with_retry(manifest_buffer, f"{self.dst_prefix}manifests/{manifest_name}")

        return f"Uploaded {tar_name} and {manifest_name}"

    def _upload_with_retry(self, file_obj, s3_key, max_retries=5):
        for attempt in range(max_retries):
            try:
                file_obj.seek(0)
                self.s3_client.upload_fileobj(
                    file_obj,
                    self.args.dst_bucket,
                    s3_key,
                    Config=self.transfer_config
                )
                return
            except ClientError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Upload failed. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to upload {s3_key} after {max_retries} attempts")
                    self.failed_files += 1
                    raise

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

