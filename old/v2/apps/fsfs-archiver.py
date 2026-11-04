import os
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


class FS2FSArchiver:
    def __init__(self, args):
        self.src_prefix = args.src_path
        self.dst_prefix = args.dst_path
        self.max_files_per_tar = args.max_files
        self.max_size_per_tar = args.max_size
        self.num_threads = args.num_threads
        self.compress = args.compress
        
        # Create necessary directories
        self.directories = self._create_directories()
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create file handler
        log_file = os.path.join(
            self.directories['logs'], 
            f'archiver_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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
        self.current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.tar_sequence = 0
        
        # Track start time
        self.start_time = None
        
        # Set delimiter for manifest files
        self.DELIMITER = '|'
    def _create_directories(self):
        """Create necessary directories for archives, manifests, and logs"""
        directories = {
            'archives': os.path.join(self.dst_prefix, 'archives'),
            'manifests': os.path.join(self.dst_prefix, 'manifests'),
            'logs': os.path.join(self.dst_prefix, 'logs')
        }
        
        for dir_name, dir_path in directories.items():
            try:
                os.makedirs(dir_path, exist_ok=True)
                print(f"Created directory: {dir_path}")
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
        self.logger.info(f"####################################")
        self.logger.info(f"Archival process completed in {elapsed_time:.1f} seconds")
        self.logger.info(f"Start_time: {self.current_time}")
        self.logger.info(f"Source path:  {self.src_prefix}")
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
        """Produces batches of files and puts them in the queue immediately"""
        try:
            current_batch = []
            current_batch_size = 0
            batch_number = 0
            total_files_found = 0
            start_time = time.time()
            
            self.logger.info("Starting file discovery and immediate processing...")
            
            for root, _, files in os.walk(self.src_prefix):
                for file in files:
                    if self.stop_event.is_set():
                        return
                    
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, self.src_prefix)
                    
                    try:
                        file_size = os.path.getsize(full_path)
                    except OSError as e:
                        self.logger.error(f"Failed to get size for file {full_path}: {str(e)}")
                        continue
                    
                    total_files_found += 1
                    
                    if total_files_found % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_files_found / elapsed if elapsed > 0 else 0
                        self.logger.info(
                            f"Discovered {total_files_found:,} files... "
                            f"({rate:.0f} files/sec)"
                        )
                    
                    # Create FileInfo object
                    file_info = FileInfo(
                        full_path=full_path,
                        rel_path=rel_path,
                        size=file_size
                    )
                    
                    # Add to current batch
                    current_batch.append(file_info)
                    current_batch_size += file_size
                    
                    # Check if batch is ready to be processed
                    should_process_batch = False
                    if self.max_files_per_tar is not None:
                        should_process_batch = len(current_batch) >= self.max_files_per_tar
                    elif self.max_size_per_tar is not None:
                        should_process_batch = current_batch_size >= self.max_size_per_tar
                    
                    # If batch is ready, queue it immediately
                    if should_process_batch:
                        batch = FileBatch(
                            files=current_batch,
                            batch_number=batch_number,
                            total_size=current_batch_size,
                            file_count=len(current_batch)
                        )
                        
                        # Queue the batch for immediate processing
                        while not self.stop_event.is_set():
                            try:
                                self.file_batch_queue.put(batch, timeout=5.0)
                                self.logger.info(
                                    f"Queued batch #{batch_number} with {len(current_batch):,} files "
                                    f"({self.get_size_display(current_batch_size)})"
                                )
                                batch_number += 1
                                current_batch = []
                                current_batch_size = 0
                                break
                            except queue.Full:
                                self.logger.warning("Queue full, waiting for consumers to catch up...")
                                time.sleep(1)
            
            # Process any remaining files in the last batch
            if current_batch:
                batch = FileBatch(
                    files=current_batch,
                    batch_number=batch_number,
                    total_size=current_batch_size,
                    file_count=len(current_batch)
                )
                
                while not self.stop_event.is_set():
                    try:
                        self.file_batch_queue.put(batch, timeout=5.0)
                        self.logger.info(
                            f"Queued final batch #{batch_number} with {len(current_batch):,} files "
                            f"({self.get_size_display(current_batch_size)})"
                        )
                        break
                    except queue.Full:
                        self.logger.warning("Queue full, waiting to queue final batch...")
                        time.sleep(1)
            
            self.logger.info(
                f"File discovery complete. Total files found: {total_files_found:,} "
                f"in {time.time() - start_time:.1f} seconds"
            )
            
            # Send completion signals to consumers
            for _ in range(self.num_threads):
                while not self.stop_event.is_set():
                    try:
                        self.file_batch_queue.put(None, timeout=5.0)
                        break
                    except queue.Full:
                        time.sleep(1)
                        
        except Exception as e:
            self.logger.error(f"Producer error: {str(e)}", exc_info=True)
            self.stop_event.set()
            # Ensure consumers exit
            for _ in range(self.num_threads):
                try:
                    self.file_batch_queue.put(None, timeout=1.0)
                except queue.Full:
                    pass

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
                    tar_filename = f"archive_{self.current_time}_{self.tar_sequence:04d}{tar_ext}"
                    manifest_filename = f"manifest_{self.current_time}_{self.tar_sequence:04d}.csv"
                
                tar_path = os.path.join(self.directories['archives'], tar_filename)
                manifest_path = os.path.join(self.directories['manifests'], manifest_filename)
                
                # Create tar archive
                try:
                    manifest_content = []
                    failed_files = []
                    
                    # Add header to manifest
                    manifest_content.append(
                        "tarfile_name|original_file_name|current_date|filesize|start_bytes|stop_bytes|md5"
                    )
                    
                    # First pass: calculate MD5 hashes
                    file_hashes = {}
                    for file_info in batch.files:
                        try:
                            with open(file_info.full_path, 'rb') as f:
                                file_data = f.read()
                                file_hashes[file_info.full_path] = hashlib.md5(file_data).hexdigest()
                        except Exception as e:
                            self.logger.error(f"Failed to calculate MD5 for {file_info.full_path}: {str(e)}")
                            failed_files.append(file_info.full_path)

                    # Create tar file with positioning information

                    with tarfile.open(tar_path, 'w:'+ compress) as tar:
                        offset = 0  # Track current position in tar file
                        
                        for file_info in batch.files:
                            if file_info.full_path in failed_files:
                                continue
                                
                            try:
                                # Get file size and calculate positions
                                file_size = os.path.getsize(file_info.full_path)
                                start_pos = offset
                                
                                # Add file to tar
                                tar.add(file_info.full_path, arcname=file_info.rel_path)
                                
                                # Calculate end position
                                end_pos = start_pos + file_size - 1
                                
                                # Update offset for next file
                                offset = end_pos + 1
                                
                                # Add to manifest with correct positions
                                manifest_content.append(
                                    f"{tar_filename}|{file_info.rel_path}|{current_date}|"
                                    f"{file_size}|{start_pos}|{end_pos}|"
                                    f"{file_hashes[file_info.full_path]}"
                                )
                                
                            except Exception as e:
                                self.logger.error(f"Failed to add file {file_info.full_path}: {str(e)}")
                                failed_files.append(file_info.full_path)
                    
                    # Write manifest file
                    with open(manifest_path, 'w') as f:
                        f.write('\n'.join(manifest_content))
                    
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


def main():
    parser = argparse.ArgumentParser(description='Archive files from source to destination')
    parser.add_argument('--src-path', required=True, help='Source directory path')
    parser.add_argument('--dst-path', required=True, help='Destination directory path')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--max-files', type=int, help='Maximum files per archive')
    group.add_argument(
        '--max-size', 
        type=parse_size, 
        help='Maximum size per archive (e.g., 100MB, 2GB)'
    )
    
    parser.add_argument('--num-threads', type=int, default=4, help='Number of consumer threads')
    parser.add_argument('--compress', type=util.strtobool, default=False, help='GZip Compress for tarfile, True or False')
    
    args = parser.parse_args()
    
    archiver = FS2FSArchiver(args)
    archiver.start_processing()


if __name__ == '__main__':
    main()

