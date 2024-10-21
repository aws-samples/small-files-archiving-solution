#!/usr/bin/env python3

import os
import argparse
import logging
import hashlib
import boto3
from boto3.s3.transfer import TransferConfig
import tarfile
from datetime import datetime
import io
import concurrent.futures
import sys

# Constants
KB = 1024
MB = KB * KB
MPU_MAX_CONCURRENCY = 10
MULTIPART_CHUNKSIZE = 16 * MB
DELIMITER = '|'

class S3Archiver:
    def __init__(self, args):
        self.args = args
        self.logger = self._setup_logging()
        self.s3_client = self._get_s3_client() if args.protocol == 's3' else None
        self.transfer_config = TransferConfig(
            max_concurrency=MPU_MAX_CONCURRENCY,
            multipart_chunksize=MULTIPART_CHUNKSIZE
        )
        self.collected_files_no = 0
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.error_log_file = f'{args.root_log_dir}/error-{self.current_time}.log'
        self.success_log_file = f'{args.root_log_dir}/success-{self.current_time}.log'
        self.tar_sequence = 1  # Initialize the tar file sequence
        self.start_time = None
        self.end_time = None
        self.upload_success = []
        self.upload_failed = []

    def _setup_logging(self):
        logging.basicConfig(level=getattr(logging, self.args.log_level),
                            format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)

    def _get_s3_client(self):
        session = boto3.Session(profile_name=self.args.profile_name)
        return session.client('s3', endpoint_url=self.args.endpoint)

    @staticmethod
    def md5hash(filename):
        md5_hash = hashlib.md5()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()

    def process_file(self, file_info):
        file_name, obj_name, file_size = file_info
        try:
            if not os.path.exists(file_name):
                raise FileNotFoundError(f"File not found: {file_name}")
            md5 = self.md5hash(file_name)
            return file_name, obj_name, file_size, md5
        except Exception as e:
            self.logger.error(f"Error processing {file_name}: {str(e)}")
            return None

    def create_manifest_and_tarfile(self, tar_name, org_files_list, manifest_name):
        recv_buf = io.BytesIO()
        
        with tarfile.open(fileobj=recv_buf, mode='w:') as tar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.max_process) as executor:
                future_to_file = {executor.submit(self.process_file, file_info): file_info for file_info in org_files_list}
                
                manifest_content = io.StringIO()
                for future in concurrent.futures.as_completed(future_to_file):
                    result = future.result()
                    if result:
                        file_name, obj_name, file_size, md5 = result
                        tar_cur_pos = recv_buf.tell()
                        
                        tar.add(file_name, arcname=obj_name)
                        self.collected_files_no += 1
                        
                        tar_end_pos = recv_buf.tell() - 1
                        content_log = f"{self.args.tar_prefix}{tar_name}{DELIMITER}{file_name}{DELIMITER}"
                        content_log += f"{datetime.now().strftime('%Y|%m|%d')}{DELIMITER}"
                        content_log += f"{file_size}{DELIMITER}{tar_cur_pos}{DELIMITER}"
                        content_log += f"{tar_end_pos}{DELIMITER}{md5}\n"
                        
                        manifest_content.write(content_log)

        if self.args.protocol == 'fs':
            tarfile_full_name = os.path.join(self.args.tar_dir, tar_name)
            with open(tarfile_full_name, 'wb') as outfile:
                outfile.write(recv_buf.getvalue())
            manifest_full_name = os.path.join(self.args.manifest_dir, manifest_name)
            with open(manifest_full_name, 'w') as outfile:
                outfile.write(manifest_content.getvalue())
            self.upload_success.extend([tar_name, manifest_name])
        elif self.args.protocol == 's3':
            if self._upload_to_s3(tar_name, recv_buf, is_tar=True):
                self.upload_success.append(tar_name)
            else:
                self.upload_failed.append(tar_name)
            if self._upload_to_s3(manifest_name, io.BytesIO(manifest_content.getvalue().encode()), is_tar=False):
                self.upload_success.append(manifest_name)
            else:
                self.upload_failed.append(manifest_name)

        return tar_name, manifest_name

    def _upload_to_s3(self, file_name, file_obj, is_tar):
        try:
            file_obj.seek(0)
            prefix = self.args.tar_prefix if is_tar else self.args.manifest_prefix
            s3_key = f"{prefix}/{file_name}"
            self.s3_client.upload_fileobj(
                file_obj,
                self.args.bucket_name,
                s3_key,
                Config=self.transfer_config
            )
            self.logger.info(f"Successfully uploaded {s3_key} to S3")
            return True
        except Exception as e:
            self.logger.error(f"Error uploading {s3_key} to S3: {str(e)}")
            return False

    def get_file_list(self):
        if self.args.input_file:
            file_list = []
            with open(self.args.input_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):  # Skip empty lines and comments
                        continue
                    parts = line.split(',')
                    if len(parts) == 1:
                        # Assume the single value is the full path, and use the basename as obj_name
                        full_path = parts[0]
                        obj_name = os.path.basename(full_path)
                    elif len(parts) >= 2:
                        full_path = parts[0]
                        obj_name = parts[1]
                    else:
                        self.logger.warning(f"Line {line_num}: Invalid format. Expected 'full_path' or 'full_path,obj_name'. Got: {line}")
                        continue

                    if not os.path.exists(full_path):
                        self.logger.warning(f"Line {line_num}: File does not exist: {full_path}")
                        continue

                    file_size = os.path.getsize(full_path)
                    file_list.append((full_path, obj_name, file_size))
                    self.logger.debug(f"Line {line_num}: Added file: {full_path} -> {obj_name} (size: {file_size})")

            self.logger.info(f"Processed {len(file_list)} valid files from the input file.")
            return file_list
        elif self.args.src_dir:
            file_list = []
            for root, _, files in os.walk(self.args.src_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, self.args.src_dir)
                    file_size = os.path.getsize(full_path)
                    file_list.append((full_path, rel_path, file_size))
            return file_list
        else:
            raise ValueError("Either --input_file or --src_dir must be provided")

    def process_files(self):
        file_list = self.get_file_list()
        current_tar_files = []
        current_tar_size = 0

        for file_info in file_list:
            file_name, obj_name, file_size = file_info
            file_size = int(file_size)

            if self.args.combine == 'count' and len(current_tar_files) >= self.args.max_file_number:
                self._create_tar(current_tar_files)
                current_tar_files = []
            elif self.args.combine == 'size' and current_tar_size + file_size > self.args.max_tarfile_size:
                self._create_tar(current_tar_files)
                current_tar_files = []
                current_tar_size = 0

            current_tar_files.append(file_info)
            current_tar_size += file_size

        if current_tar_files:
            self._create_tar(current_tar_files)

    def _create_tar(self, file_list):
        tar_name = f"archive_{self.current_time}_{self.tar_sequence:04d}.tar"
        manifest_name = f"manifest_{self.current_time}_{self.tar_sequence:04d}.csv"
        self.create_manifest_and_tarfile(tar_name, file_list, manifest_name)
        self.tar_sequence += 1  # Increment the sequence for the next tar file

    def run(self):
        try:
            self.start_time = datetime.now()
            self._validate_args()
            self._setup_directories()
            self.process_files()
            self.end_time = datetime.now()
            self._generate_report()
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            sys.exit(1)

    def _validate_args(self):
        if self.args.protocol == 's3' and not self.args.bucket_name:
            raise ValueError("--bucket_name is required when protocol is 's3'")
        if not self.args.src_dir and not self.args.input_file:
            raise ValueError("Either --src_dir or --input_file must be provided")

    def _setup_directories(self):
        os.makedirs(self.args.root_log_dir, exist_ok=True)
        if self.args.protocol == 'fs':
            os.makedirs(self.args.tar_dir, exist_ok=True)
            os.makedirs(self.args.manifest_dir, exist_ok=True)

    def _generate_report(self):
        duration = self.end_time - self.start_time
        
        if self.args.protocol == 's3':
            tar_destination = f"s3://{self.args.bucket_name}/{self.args.tar_prefix}/"
            manifest_destination = f"s3://{self.args.bucket_name}/{self.args.manifest_prefix}/"
        else:
            tar_destination = self.args.tar_dir
            manifest_destination = self.args.manifest_dir

        report = f"""
        Archiving Report
        ----------------
        Start Time: {self.start_time}
        End Time: {self.end_time}
        Duration: {duration}
        
        Total Files Processed: {self.collected_files_no}
        Total Tar Files Created: {self.tar_sequence - 1}
        
        Destination:
        Tar Files: {tar_destination}
        Manifest Files: {manifest_destination}
        
        Successful Uploads: {len(self.upload_success)}
        Failed Uploads: {len(self.upload_failed)}
        
        Successfully Uploaded Files:
        {', '.join(self.upload_success)}
        
        Failed Uploads:
        {', '.join(self.upload_failed)}
        """
        
        self.logger.info(report)
        
        report_file = f'{self.args.root_log_dir}/report-{self.current_time}.txt'
        with open(report_file, 'w') as f:
            f.write(report)
        
        self.logger.info(f"Detailed report saved to {report_file}")

def parse_arguments():
    parser = argparse.ArgumentParser(description='S3 Archiver')
    parser.add_argument('--src_dir', help='Source directory, e.g., /data/dir1/')
    parser.add_argument('--protocol', choices=['s3', 'fs'], required=True, help='Specify the protocol to use')
    parser.add_argument('--max_process', type=int, default=5, help='Number of concurrent processes')
    parser.add_argument('--combine', choices=['size', 'count'], required=True, help='Combine files based on size or count')
    parser.add_argument('--max_file_number', type=int, default=1000, help='Max files in one tarfile')
    parser.add_argument('--max_tarfile_size', type=int, default=10*(1024**3), help='Max tarfile size in bytes')
    parser.add_argument('--bucket_name', help='S3 bucket name')
    parser.add_argument('--endpoint', default='https://s3.ap-northeast-2.amazonaws.com', help='S3 endpoint')
    parser.add_argument('--profile_name', default='default', help='AWS profile name')
    parser.add_argument('--tar_prefix', default='archives', help='Prefix for tarfiles in the bucket or directory for tar files in fs mode')
    parser.add_argument('--manifest_prefix', default='manifests', help='Prefix for manifest files in the bucket or directory for manifest files in fs mode')
    parser.add_argument('--fs_dir', default='/mnt/fs', help='Filesystem mounting point')
    parser.add_argument('--input_file', help='Input file with list of files to process')
    parser.add_argument('--root_log_dir', default='logs', help='Root directory for logs')
    parser.add_argument('--log_level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set the logging level')
    
    args = parser.parse_args()
    
    # Set tar_dir and manifest_dir based on protocol
    if args.protocol == 'fs':
        args.tar_dir = os.path.join(args.fs_dir, args.tar_prefix)
        args.manifest_dir = os.path.join(args.fs_dir, args.manifest_prefix)
    else:
        args.tar_dir = args.tar_prefix
        args.manifest_dir = args.manifest_prefix
    
    return args

def main():
    args = parse_arguments()
    archiver = S3Archiver(args)
    archiver.run()

if __name__ == "__main__":
    main()

