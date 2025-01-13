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
    bucket: str
    key: str
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

class S3toS3Archiver:
    def __init__(self, args):
        self.src_bucket = args.src_bucket
        self.src_prefix = args.src_prefix.rstrip('/')
        self.dst_prefix = args.dst_prefix.rstrip('/')
        self.dst_bucket = args.dst_bucket
        self.max_files_per_tar = args.max_files
        self.max_size_per_tar = args.max_size
        self.num_threads = args.num_threads
        self.compress = args.compress
        self.profile_name = args.profile_name

        # Configure S3 client with higher max pool connections
        config = Config(
            max_pool_connections=min(self.num_threads * 2, 1000),
            retries={'max_attempts': 3},
            connect_timeout=5,
            read_timeout=60
        )
        
        # Set S3 client
        session = boto3.Session(profile_name=self.profile_name)
        self.s3_client = session.client('s3', config=config)
        
        self.transfer_config = TransferConfig(
            max_concurrency=256,
            multipart_chunksize=64 * 1024 * 1024,  # 64MB chunks
            multipart_threshold=64 * 1024 * 1024  # Start multipart at 64MB
        )
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
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
        self.current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.tar_sequence = 0
        
        # Track start time
        self.start_time = None
        
        # Set delimiter for manifest files
        self.DELIMITER = '|'

    def _is_batch_full(self, batch_files, current_size):
        """Check if the current batch is full based on the chosen strategy"""
        if self.max_files_per_tar:
            return len(batch_files) >= self.max_files_per_tar
        return current_size >= self.max_size_per_tar

    def _queue_batch(self, batch_files, batch_number, current_size):
        """Create and queue a new batch"""
        batch = FileBatch(
            files=batch_files,
            batch_number=batch_number,
            total_size=current_size,
            file_count=len(batch_files)
        )
        self.file_batch_queue.put(batch)

    def _file_list_producer(self):
        """List objects from source S3 bucket and create batches"""
        try:
            batch_files = []
            current_size = 0
            batch_number = 1
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Paginate through S3 objects
            for page in paginator.paginate(
                Bucket=self.src_bucket,
                Prefix=self.src_prefix
            ):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    if self.stop_event.is_set():
                        return

                    file_info = FileInfo(
                        bucket=self.src_bucket,
                        key=obj['Key'],
                        size=obj['Size']
                    )

                    batch_files.append(file_info)
                    current_size += obj['Size']

                    # Check if batch is full based on strategy
                    if self._is_batch_full(batch_files, current_size):
                        self._queue_batch(batch_files, batch_number, current_size)
                        batch_files = []
                        current_size = 0
                        batch_number += 1

            # Queue remaining files
            if batch_files:
                self._queue_batch(batch_files, batch_number, current_size)

        except Exception as e:
            self.logger.error(f"Error in producer: {str(e)}")
            self.stop_event.set()
        finally:
            # Signal consumers to stop
            for _ in range(self.num_threads):
                self.file_batch_queue.put(None)

    def _download_s3_object(self, file_info):
        """Download single object from S3 to memory"""
        try:
            response = self.s3_client.get_object(
                Bucket=file_info.bucket,
                Key=file_info.key
            )
            return response['Body'].read()
        except Exception as e:
            self.logger.error(f"Failed to download {file_info.key}: {str(e)}")
            return None

    def _create_manifest_entry(self, file_info, content, tar_key, start_pos, end_pos):
        """Create a manifest entry for a file with position information"""
        current_date = datetime.now().strftime('%Y-%m-%d')
        hash_enabled = True

        if hash_enabled:
            md5_hash = hashlib.md5(content).hexdigest()
            return (
                f"{tar_key}{self.DELIMITER}"
                f"{file_info.key}{self.DELIMITER}"
                f"{current_date}{self.DELIMITER}"
                f"{file_info.size}{self.DELIMITER}"
                f"{start_pos}{self.DELIMITER}"
                f"{end_pos}{self.DELIMITER}"
                f"{md5_hash}"
            )
        else:
            return (
                f"{tar_key}{self.DELIMITER}"
                f"{file_info.key}{self.DELIMITER}"
                f"{current_date}{self.DELIMITER}"
                f"{file_info.size}{self.DELIMITER}"
                f"{start_pos}{self.DELIMITER}"
                f"{end_pos}{self.DELIMITER}"
            )


    def _tar_creator_consumer(self):
        """Consumer thread that creates tar archives from S3 objects"""
        while not self.stop_event.is_set():
            batch = self.file_batch_queue.get()
            if batch is None:
                break

            # Generate tar filename
            tar_key = f"{self.dst_prefix}/archives/archive_{self.current_time}_{batch.batch_number}.tar"
            if self.compress:
                tar_key += ".gz"

            tar_buffer = io.BytesIO()
            manifest_entries = []
            current_pos = 0  # Track position in tar file
            
            with tarfile.open(
                fileobj=tar_buffer,
                mode='w:gz' if self.compress else 'w',
                bufsize=256 * 1024 * 1024  # 256MB buffer
            ) as tar:
                for file_info in batch.files:
                    try:
                        # Download object from S3
                        content = self._download_s3_object(file_info)
                        if content is None:
                            self._update_stats(failed=1)
                            continue

                        # Create tar info
                        tar_info = tarfile.TarInfo(name=file_info.key)
                        tar_info.size = len(content)
                        
                        # Calculate positions
                        start_pos = current_pos
                        
                        # Add to tar archive
                        tar.addfile(tar_info, io.BytesIO(content))

                        # Show file name while executing
                        #self.logger.info(f"Adding {file_info.key} into {tar_key}")
                        #print(f"Adding {file_info.key} into {tar_key}")
                        
                        # Calculate end position (start + header + content + padding)
                        header_size = 512  # tar header size
                        content_size = len(content)
                        padding = (512 - (content_size % 512)) % 512  # padding to 512 byte boundary
                        end_pos = start_pos + header_size + content_size + padding
                        
                        # Update current position
                        current_pos = end_pos
                        
                        # Create manifest entry with position information
                        manifest_entry = self._create_manifest_entry(
                            file_info,
                            content,
                            tar_key,
                            start_pos,
                            end_pos
                        )
                        manifest_entries.append(manifest_entry)
                        
                        self._update_stats(files=1, bytes_transferred=file_info.size)
                    
                    except Exception as e:
                        self.logger.error(f"Error processing {file_info.key}: {str(e)}")
                        self._update_stats(failed=1)

            # Upload tar file and manifest
            if manifest_entries:
                self._upload_archive_and_manifest(tar_buffer, manifest_entries, batch.batch_number, tar_key)
                self.logger.info(f"{tar_key}: uploaded")

    def _upload_archive_and_manifest(self, tar_buffer, manifest_entries, batch_number, tar_key):
        """Upload tar archive and manifest to destination S3"""
        try:
            manifest_key = f"{self.dst_prefix}/manifests/manifest_{self.current_time}_{batch_number}.csv"

            # Upload tar file
            tar_buffer.seek(0)
            self.s3_client.upload_fileobj(
                tar_buffer,
                self.dst_bucket,
                tar_key,
                Config=self.transfer_config
            )
            self._update_stats(tars=1)

            # Add header to manifest
            manifest_header = "tar_path|file_path|timestamp|file_size|start_position|end_position|md5_hash"
            manifest_content = manifest_header + '\n' + '\n'.join(manifest_entries)
            
            # Upload manifest
            self.s3_client.put_object(
                Bucket=self.dst_bucket,
                Key=manifest_key,
                Body=manifest_content.encode('utf-8')
            )
            self._update_stats(manifests=1)

        except Exception as e:
            self.logger.error(f"Failed to upload archive/manifest {batch_number}: {str(e)}")

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
        self.logger.info(f"Archival process completed in {formatted_duration}")
        self.logger.info(f"Start_time: {self.current_time}")
        self.logger.info(f"Source bucket: {self.src_bucket}")
        self.logger.info(f"Source prefix: {self.src_prefix}")
        self.logger.info(f"Destination bucket: {self.dst_bucket}")
        self.logger.info(f"Destination prefix: {self.dst_prefix}")
        self.logger.info(f"Total files processed: {self.total_files:,}")
        self.logger.info(f"Failed files: {self.failed_files:,}")
        self.logger.info(f"Total tar files created: {self.total_tar_files:,}")
        self.logger.info(f"Total manifest files created: {self.total_manifest_files:,}")
        self.logger.info(f"Total bytes transferred: {self.get_size_display(self.total_bytes_transferred)}")
        if elapsed_time > 0:
            transfer_rate = self.total_bytes_transferred / elapsed_time
            self.logger.info(f"Average transfer rate: {self.get_size_display(transfer_rate)}/s")
        self.logger.info(f"####################################")

    @staticmethod
    def get_size_display(size_in_bytes):
        """Convert bytes to human readable format"""
        if size_in_bytes == 0:
            return "0B"
            
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        size = float(size_in_bytes)
        unit_index = 0
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
            
        return f"{size:.2f}{units[unit_index]}"

def main():
    parser = argparse.ArgumentParser(description='S3 to S3 Archiver')
    parser.add_argument('--src-bucket', required=True, help='Source S3 bucket name')
    parser.add_argument('--src-prefix', required=True, help='Source S3 prefix path')
    parser.add_argument('--dst-bucket', required=True, help='Destination S3 bucket name')
    parser.add_argument('--dst-prefix', required=True, help='Destination prefix path')
    parser.add_argument('--max-files', type=int, help='Maximum number of files per tar archive')
    parser.add_argument('--max-size', type=parse_size, help='Maximum size per tar archive (e.g., 5GB)')
    parser.add_argument('--num-threads', type=int, default=10, help='Number of worker threads')
    parser.add_argument('--compress', type=util.strtobool, default=False, help='GZip Compress for tarfile, True or False')
    parser.add_argument('--profile-name', help='AWS profile name to use')

    args = parser.parse_args()
    
    archiver = S3toS3Archiver(args)
    archiver.start_processing()

if __name__ == '__main__':
    main()

