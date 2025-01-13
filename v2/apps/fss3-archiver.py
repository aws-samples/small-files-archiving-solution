"""
- 2025.01.10: adding --input-file feature
"""
import os
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
import tarfile
from datetime import datetime
import io
import concurrent.futures
import logging
import hashlib
import threading
import queue
from dataclasses import dataclass
from typing import List, Tuple
import time
import argparse
from argparse import ArgumentTypeError
from distutils import util

def parse_size(size_str: str) -> int:
    """Convert human readable size string to bytes"""
    size_str = size_str.strip().upper()
    
    units = {
        'B': 1,
        'KB': 1024,
        'K': 1024,
        'MB': 1024 * 1024,
        'M': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
        'G': 1024 * 1024 * 1024,
        'TB': 1024 * 1024 * 1024 * 1024,
        'T': 1024 * 1024 * 1024 * 1024
    }
    
    if size_str.isdigit():
        return int(size_str)
    
    number = ''
    unit = ''
    for char in size_str:
        if char.isdigit() or char == '.':
            number += char
        else:
            unit += char
    
    unit = unit.strip()
    
    if not number or not unit:
        raise ArgumentTypeError(
            f"Invalid size format: {size_str}. Example formats: 100MB, 2GB, 1TB"
        )
    
    if unit not in units:
        raise ArgumentTypeError(
            f"Invalid size unit: {unit}. Must be one of {', '.join(sorted(units.keys()))}"
        )
    
    try:
        size_bytes = float(number) * units[unit]
        return int(size_bytes)
    except ValueError:
        raise ArgumentTypeError(f"Invalid size number: {number}")


@dataclass
class FileInfo:
    full_path: str
    rel_path: str
    size: int
    start_byte: int = 0
    stop_byte: int = 0
    md5: str = ""


@dataclass
class FileBatch:
    files: List[FileInfo]
    batch_number: int
    total_size: int
    file_count: int


class FS2S3Archiver:
    def __init__(self, args):
        self.src_prefix = args.src_path
        self.dst_prefix = args.dst_prefix.rstrip('/')
        self.dst_bucket = args.dst_bucket
        self.max_files_per_tar = args.max_files
        self.max_size_per_tar = args.max_size
        self.num_threads = args.num_threads
        self.compress = args.compress
        self.profile_name = args.profile_name
        self.input_file = args.input_file  # New parameter for input file
        self.current_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Set S3 client
        self.s3_client = self._get_s3_client()
        self.transfer_config = TransferConfig(
            max_concurrency=50,
            multipart_chunksize=16 * 1024 * 1024
        )
        
        # Create necessary directories
        self.directories = self._create_directories()
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create file handler
        log_file = os.path.join(
            self.directories['logs'], 
            f'archiver_{self.current_time}.log'
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Validate options
        if self.max_files_per_tar is not None and self.max_size_per_tar is not None:
            raise ValueError("Cannot specify both --max-files and --max-size")
        if self.max_files_per_tar is None and self.max_size_per_tar is None:
            raise ValueError("Must specify either --max-files or --max-size")
            
        # Log the chosen strategy
        if self.max_files_per_tar is not None:
            self.logger.info(f"Using file count strategy: {self.max_files_per_tar} files per archive")
        else:
            self.logger.info(f"Using size strategy: {self.get_size_display(self.max_size_per_tar)} per archive")
        
        # Initialize locks and events
        self.tar_sequence_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # Initialize counters
        self._total_files = 0
        self._failed_files = 0
        self._total_tar_files = 0
        self._total_manifest_files = 0
        self._total_bytes_transferred = 0
        
        # Queue for producer/consumer pattern
        self.file_batch_queue = queue.Queue(maxsize=self.num_threads * 2)
        
        # Set timestamp for file naming
        self.tar_sequence = 0
        
        # Track start time
        self.start_time = None
        
        # Set delimiter for manifest files
        self.DELIMITER = '|'

    def _get_s3_client(self):
        """Initialize s3 client"""
        session = boto3.Session(profile_name=self.profile_name)
        return session.client('s3')

    def _create_directories(self):
        """Create necessary directories for archives, manifests, and logs"""
        directories = {
            #'archives': os.path.join(self.dst_prefix, 'archives'),
            #'manifests': os.path.join(self.dst_prefix, 'manifests'),
            #'logs': os.path.join(self.dst_prefix, 'logs')
            'logs': os.path.join('logs', self.dst_prefix)
        }
        
        for dir_name, dir_path in directories.items():
            try:
                os.makedirs(dir_path, exist_ok=True)
                #print(f"Created directory: {dir_path}")
            except Exception as e:
                raise RuntimeError(f"Failed to create {dir_name} directory: {str(e)}")
        
        return directories

    def _update_stats(self, files=0, failed=0, tars=0, manifests=0, bytes_transferred=0):
        """Thread-safe statistics update"""
        with self._stats_lock:
            self._total_files += files
            self._failed_files += failed
            self._total_tar_files += tars
            self._total_manifest_files += manifests
            self._total_bytes_transferred += bytes_transferred

    @property
    def total_files(self):
        return self._total_files
    
    @property
    def failed_files(self):
        return self._failed_files
    
    @property
    def total_tar_files(self):
        return self._total_tar_files
    
    @property
    def total_manifest_files(self):
        return self._total_manifest_files
    
    @property
    def total_bytes_transferred(self):
        return self._total_bytes_transferred

    def format_duration(self, seconds):
       """Convert seconds to human readable duration format"""
       hours = int(seconds // 3600)
       minutes = int((seconds % 3600) // 60)
       seconds = int(seconds % 60)
       
       parts = []
       if hours > 0:
          parts.append(f"{hours}h")
       if minutes > 0 or hours > 0:  # Show minutes if there are hours
          parts.append(f"{minutes}m")
       parts.append(f"{seconds}s")
       
       return " ".join(parts)

    def start_processing(self):
        """Start the producer and consumer threads"""
        self.start_time = time.time()
        
        # Create and start consumer threads first
        self.consumer_threads = [
            threading.Thread(
                target=self._tar_creator_consumer,
                name=f"consumer-{i+1}"
            )
            for i in range(self.num_threads)
        ]
        
        for consumer in self.consumer_threads:
            consumer.start()
            
        # Create and start producer thread
        self.producer_thread = threading.Thread(
            target=self._file_list_producer,
            name="producer"
        )
        self.producer_thread.start()
        
        # Wait for completion
        self.producer_thread.join()
        for consumer in self.consumer_threads:
            consumer.join()

        # Log final statistics
        elapsed_time = time.time() - self.start_time
        formatted_duration = self.format_duration(elapsed_time)

        self.logger.info(f"####################################")
        self.logger.info(f"Archival process completed in {formatted_duration} seconds")
        self.logger.info(f"Start_time: {self.current_time}")
        self.logger.info(f"Source path:  {self.src_prefix}")
        self.logger.info(f"Destination bucket:  {self.dst_bucket}")
        self.logger.info(f"Destination path:  {self.dst_prefix}")
        self.logger.info(f"Total files processed: {self.total_files:,}")
        self.logger.info(f"Failed files: {self.failed_files:,}")
        self.logger.info(f"Total tar files created: {self.total_tar_files:,}")
        self.logger.info(f"Total manifest files created: {self.total_manifest_files:,}")
        self.logger.info(f"Total bytes transferred: {self.get_size_display(self.total_bytes_transferred)}")
        if elapsed_time > 0:
            transfer_rate = self.total_bytes_transferred / elapsed_time
            self.logger.info(f"Average transfer rate: {self.get_size_display(transfer_rate)}/s")
        self.logger.info(f"####################################")

    def _read_input_file(self, input_file):
        """Read file paths from input file"""
        files_info = []
        try:
            with open(input_file, 'r') as f:
                for line in f:
                    file_path = line.strip()
                    if not file_path or file_path.startswith('#'):  # Skip empty lines and comments
                        continue
                        
                    if os.path.isfile(file_path):
                        try:
                            file_size = os.path.getsize(file_path)
                            # Make the path relative to src_prefix if it starts with it
                            rel_path = (file_path[len(self.src_prefix):].lstrip(os.sep) 
                                      if file_path.startswith(self.src_prefix) 
                                      else file_path)
                            
                            files_info.append(FileInfo(
                                full_path=file_path,
                                rel_path=rel_path,
                                size=file_size
                            ))
                        except OSError as e:
                            self.logger.error(f"Error accessing file {file_path}: {str(e)}")
                            self._update_stats(failed=1)
                    else:
                        self.logger.warning(f"File not found: {file_path}")
                        self._update_stats(failed=1)
        except Exception as e:
            self.logger.error(f"Error reading input file {input_file}: {str(e)}")
            raise
        
        return files_info

    def _scan_directory(self):
        """Scan directory for files"""
        files_info = []
        try:
            for root, _, files in os.walk(self.src_prefix):
                for filename in files:
                    full_path = os.path.join(root, filename)
                    try:
                        file_size = os.path.getsize(full_path)
                        rel_path = os.path.relpath(full_path, self.src_prefix)
                        
                        files_info.append(FileInfo(
                            full_path=full_path,
                            rel_path=rel_path,
                            size=file_size
                        ))
                    except OSError as e:
                        self.logger.error(f"Error accessing file {full_path}: {str(e)}")
                        self._update_stats(failed=1)
        except Exception as e:
            self.logger.error(f"Error scanning directory {self.src_prefix}: {str(e)}")
            raise
            
        return files_info

    @staticmethod
    def get_size_display(size_in_bytes):
        """Convert bytes to human readable format"""
        if size_in_bytes == 0:
            return "0B"
            
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        size = float(size_in_bytes)
        unit_index = 0
        
        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1
            
        return f"{size:.2f}{units[unit_index]}"

    def _file_list_producer(self):
        """Producer that generates file batches as soon as criteria are met"""
        try:
            current_batch = []
            current_batch_size = 0
            batch_number = 0
    
            def send_batch():
                nonlocal current_batch, current_batch_size, batch_number
                if current_batch:
                    file_batch = FileBatch(
                        files=current_batch,
                        batch_number=batch_number,
                        total_size=current_batch_size,
                        file_count=len(current_batch)
                    )
                    self.file_batch_queue.put(file_batch)
                    batch_number += 1
                    current_batch = []
                    current_batch_size = 0
    
            # If using input file
            if self.input_file:
                for file_info in self._read_input_file(self.input_file):
                    current_batch.append(file_info)
                    current_batch_size += file_info.size
    
                    # Check if batch criteria are met
                    if self._should_send_batch(current_batch, current_batch_size):
                        send_batch()
            else:
                # Walking directory structure
                for root, _, files in os.walk(self.src_prefix):
                    for filename in files:
                        full_path = os.path.join(root, filename)
                        rel_path = os.path.relpath(full_path, self.src_prefix)
                        
                        try:
                            file_size = os.path.getsize(full_path)
                            file_info = FileInfo(
                                full_path=full_path,
                                rel_path=rel_path,
                                size=file_size
                            )
                            
                            current_batch.append(file_info)
                            current_batch_size += file_size
    
                            # Check if batch criteria are met
                            if self._should_send_batch(current_batch, current_batch_size):
                                send_batch()
                                
                        except OSError as e:
                            self.logger.error(f"Error processing {full_path}: {str(e)}")
                            self._update_stats(failed=1)
    
            # Send any remaining files in the last batch
            send_batch()
            
        except Exception as e:
            self.logger.error(f"Producer error: {str(e)}")
            self.stop_event.set()
        finally:
            # Signal consumers that no more batches are coming
            for _ in range(self.num_threads):
                self.file_batch_queue.put(None)

    def _should_send_batch(self, current_batch, current_batch_size):
        """Check if the current batch should be sent based on criteria"""
        if self.max_files_per_tar is not None:
            return len(current_batch) >= self.max_files_per_tar
        else:  # using max_size_per_tar
            return current_batch_size >= self.max_size_per_tar


    def _submit_batch(self, batch, batch_number):
        """Submit a batch of files to the queue"""
        total_size = sum(f.size for f in batch)
        file_batch = FileBatch(
            files=batch,
            batch_number=batch_number,
            total_size=total_size,
            file_count=len(batch)
        )
        self.file_batch_queue.put(file_batch)

    def _create_hash(self, batch):
        """Create file hash"""
        file_hashes = {}
        for file_info in batch.files:
            try:
                with open(file_info.full_path, 'rb') as f:
                    file_data = f.read()
                    file_hashes[file_info.full_path] = hashlib.md5(file_data).hexdigest()
            except Exception as e:
                self.logger.error(f"Failed to calculate MD5 for {file_info.full_path}: {str(e)}")
                failed_files.append(file_info.full_path)
        return file_hashes

    def _tar_creator_consumer(self):
        """Consumes file batches from the queue and creates tar archives"""
        thread_name = threading.current_thread().name
        current_date = datetime.now().strftime('%Y-%m-%d')

        # enable compress
        if self.compress:
            compress = "gz"
            tar_ext = ".tar.gz"
        else:
            compress = ""
            tar_ext = ".tar"
        
        while not self.stop_event.is_set():
            try:
                # Get batch from queue
                try:
                    batch = self.file_batch_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                if batch is None:
                    self.logger.debug(f"{thread_name}: Received completion signal")
                    self.file_batch_queue.task_done()
                    break
                
                # Generate tar file name
                with self.tar_sequence_lock:
                    self.tar_sequence += 1
                    batch_id = f"{self.current_time}_{self.tar_sequence:04d}"
                    tar_filename = f"archive_{batch_id}{tar_ext}"
                    manifest_filename = f"manifest_{batch_id}.csv"
                
                tar_path = self.dst_prefix + "/archives/" + tar_filename
                manifest_path = self.dst_prefix + "/manifests/" + manifest_filename
                
                # Create tar archive
                try:
                    manifest_content = []
                    failed_files = []
                    
                    # Add header to manifest
                    manifest_content.append(
                        f"tarfile_name{self.DELIMITER} file_name{self.DELIMITER} current_date{self.DELIMITER} filesize{self.DELIMITER} start_bytes{self.DELIMITER} stop_bytes{self.DELIMITER} md5"
                    )
                    
                    # First pass: calculate MD5 hashes
                    hash_enabled = True
                    if hash_enabled:
                        file_hashes = self._create_hash(batch)
                    else:
                        file_hashes = {}

                    # Create tar file with positioning information
                    tar_buffer = io.BytesIO()
                    manifest_buffer = io.StringIO()

                    with tarfile.open(fileobj=tar_buffer, mode='w:'+ compress) as tar:
                        offset = 0  # Track current position in tar file
                        
                        for file_info in batch.files:
                            if file_info.full_path in failed_files:
                                continue
                                
                            try:
                                # Get file size and calculate positions
                                file_size = os.path.getsize(file_info.full_path)
                                start_pos = tar_buffer.tell()
                                
                                # Add file to tar
                                tar.add(file_info.full_path, arcname=file_info.rel_path)

                                # Show file name while executing
                                #print(f"Adding {file_info.full_path} into {tar_path}")
                                
                                # Calculate end position
                                end_pos = tar_buffer.tell() - 1
                                
                                # Update offset for next file
                                offset = end_pos + 1
                                
                                # Add to manifest with correct positions
                                if file_hashes == {}:
                                    manifest_content.append(
                                        f"{tar_path}{self.DELIMITER}{file_info.full_path}{self.DELIMITER}{current_date}{self.DELIMITER}"
                                        f"{file_size}{self.DELIMITER}{start_pos}{self.DELIMITER}{end_pos}{self.DELIMITER}"
                                        )
                                else:
                                    manifest_content.append(
                                        f"{tar_path}{self.DELIMITER}{file_info.full_path}{self.DELIMITER}{current_date}{self.DELIMITER}"
                                        f"{file_size}{self.DELIMITER}{start_pos}{self.DELIMITER}{end_pos}{self.DELIMITER}"
                                        f"{file_hashes[file_info.full_path]}"
                                        )
                                
                            except Exception as e:
                                self.logger.error(f"Failed to add file {file_info.full_path}: {str(e)}")
                                failed_files.append(file_info.full_path)
                    
                    # Write manifest file
                    content_log = '\n'.join(manifest_content)
                    manifest_buffer.write(content_log)

                    # Upload files
                    tar_buffer.seek(0)
                    self._upload_to_s3(bucket=self.dst_bucket, key=tar_path, data=tar_buffer)
                    manifest_buffer.seek(0)
                    self._upload_to_s3(bucket=self.dst_bucket, key=manifest_path, data=manifest_buffer.getvalue().encode('utf-8'))

                    # Update statistics
                    self._update_stats(
                        files=len(batch.files) - len(failed_files),
                        failed=len(failed_files),
                        tars=1,
                        manifests=1,
                        bytes_transferred=batch.total_size
                    )
                    
                    self.logger.info(
                        f"{thread_name}: Created archive {tar_filename} with {len(batch.files)} files "
                        f"({self.get_size_display(batch.total_size)})"
                    )

                    
                except Exception as e:
                    self.logger.error(f"{thread_name}: Failed to create archive {tar_filename}: {str(e)}")
                    self.logger.exception(f"{thread_name}: Failed to create archive {tar_filename}: {str(e)}")
                    # Attempt to clean up partial files
                    for file in [tar_path, manifest_path]:
                        if os.path.exists(file):
                            try:
                                os.remove(file)
                            except Exception:
                                pass
                
                finally:
                    self.file_batch_queue.task_done()

            except Exception as e:
                self.logger.error(f"{thread_name}: Consumer error: {str(e)}", exc_info=True)
                self.stop_event.set()
                break

    def _upload_to_s3(self, bucket, key, data):
        """Upload data to S3"""
        try:
            if isinstance(data, io.BytesIO):
                self.s3_client.upload_fileobj(
                    data,
                    bucket,
                    key.lstrip('/'),
                    Config=self.transfer_config
                )
            else:
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key.lstrip('/'),
                    Body=data
                )
        except Exception as e:
            raise Exception(f"Failed to upload to S3: {str(e)}") 

    def _upload_with_retry(self, file_obj, s3_key, max_retries=5):
        """Upload to S3 with retry logic"""
        for attempt in range(max_retries):
            try:
                file_obj.seek(0)
                self.s3_client.upload_fileobj(
                    file_obj,
                    self.dst_bucket,
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


def main():
    parser = argparse.ArgumentParser(description='File System to S3 Archiver')
    parser.add_argument('--src-path', required=True, help='Source directory path')
    parser.add_argument('--dst-bucket', required=True, help='Destination S3 bucket')
    parser.add_argument('--dst-prefix', required=True, help='Destination prefix for output files')
    parser.add_argument('--max-files', type=int, help='Maximum number of files per tar archive')
    parser.add_argument('--max-size', type=parse_size, help='Maximum size per tar archive (e.g., 5GB)')
    parser.add_argument('--num-threads', type=int, default=4, help='Number of worker threads')
    parser.add_argument('--compress', type=lambda x: bool(util.strtobool(x)), default=False,
                      help='Whether to compress the tar files')
    parser.add_argument('--profile-name', default='default', help='AWS profile name')
    parser.add_argument('--input-file', help='Path to a file containing list of files to process')
    
    args = parser.parse_args()
    
    archiver = FS2S3Archiver(args)
    archiver.start_processing()

if __name__ == '__main__':
    main()

